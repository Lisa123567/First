
__author__ = 'rkissos'

from random import choice
from random import randint
from os.path import join
from copy import copy
from time import sleep
from struct import pack,unpack
from pytest import mark

from tonat.context import ctx
from tonat.test_base import TestBase, attr
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.errors import FileIntegrityError
from pd_tools.fio_caps import FioCaps
from tonat.rand_ex import RandStr

class TestMobilitySimpleModel(TestBase):

    # https://atlassian.corp.primarydata.com/stash/projects/MGMT/repos
    # /sys-mgmt/browse/extras/etc/fio-test.conf
    _FIO_GLOBALS = {'group_reporting': None, 'thread': None,
                    'ioengine': 'libaio', 'time_based': None, 'runtime': 45,
                    'direct': 1, 'stonewall': None}

    _FIO_LAT_OPT = {'blocksize': '4K',
                    'rw': 'randread',
                    'iodepth': '2',
                    'numjobs': '1'}

    _FIO_IOPS_OPT = {'blocksize': '512',
                     'rw': 'randrw',
                     'iodepth': '10',
                     'numjobs': '4'}
    PB3_BW_CONVERSION = 1024
    PB4_FILL_PERCENT = 78
    PB4_DEFAULT_UTIL_THRES = 75
    PB4_UTIL_THRES_1 = 50
    PB4_UTIL_THRES_2 = 25
    PB4_MIN_BS = 0.5
    PB4_TIMEOUT = 300

    _PARAMS_MAPPER = {'min_write_iops': 'write_iops',
                     'min_write_throughput': 'write_bw',
                     'max_write_response_time': 'write_latency',
                     'min_read_iops': 'read_iops',
                     'min_read_throughput': 'read_bw',
                     'max_read_response_time': 'read_latency'}

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason='Insufficient resources - Supply at least 2 DSes')
    @mark.skip_all
    @attr(jira='PD-15245', name='test_performance_mobility_playbook1')
    def test_performance_mobility_playbook1(self, get_slow_ds,
                                            get_mount_points):
        assert len(ctx.cluster.data_stores) == 1, \
            'Test cannot run with more than single storage-volumes after'
        ctx.clients.execute(['rpcdebug', '-m', 'nfs', '-s', 'all'])
        mount = get_mount_points[0]
        slow_ds = get_slow_ds
        slow_ds_ip = slow_ds.node.mgmt_ip_address.address
        fast_ds_physical = choice([ds for ds in ctx.data_stores
                                   if ds.address != slow_ds_ip])

        with self.step('Create set_a files'):
            files = [join(mount.path + "/a_file" + str(file_index)) for file_index in xrange(3)]
            a_fds = []
            for f in files:
                ctx.clients[0].truncate(f, '500M')
                fd = ctx.clients[0].file_declare(f, mount.share)
                a_fds.append(fd)
                fd.verify_location(slow_ds)

        # Create objective with an ask of multiple times the read_iops of the slow_ds so later
        # when fast_ds is added the file with automatically move to fast_ds as the
        # requested read_iops can be met by the fast_ds
        with self.step('Create smart-objective on set_b files'):
            slow_ds_read_iops = int(slow_ds.storage_capabilities.performance.read_performance.iops)
            min_read_iops = slow_ds_read_iops*2
            iopso = ctx.cluster.cli.basic_objective_create(name='any_iops',
                                            min_read_iops=min_read_iops)
            cond = ctx.cluster.cli.condition_create(name='bfile_activity',
                                                    pattern='b_file*',)
            rule = '{}:{}'.format(cond.name, iopso.name)
            so = ctx.cluster.cli.smart_objective_create(name='Smart_Perf_B',
                                                        rule=rule)
            ctx.cluster.cli.share_objective_add(name=mount.share.name,
                                                objective=so.name,
                                                path='/')
            # SLO updates are not reflected in real time so sleep for few secs before file creation
            sleep(3)

        with self.step('Create set_b files'):
            files = [join(mount.path + "/b_file" + str(file_index)) for file_index in xrange(3)]
            b_fds = []
            for f in files:
                ctx.clients[0].truncate(f, '500M')
                fd = ctx.clients[0].file_declare(f, mount.share)
                b_fds.append(fd)
                fd.verify_location(slow_ds)

        with self.step('Add second faster ds'):
            ds_name = fast_ds_physical.get_hostname()
            un_node = ctx.cluster.unauthenticated_nodes[ds_name]
            ctx.cluster.cli._node_add(fast_ds_physical, name=un_node.name)
            lv = fast_ds_physical.get_free_export()
            fast_ds = ctx.cluster.cli.ds_add(node_name=ds_name,
                                             logical_volume_name=lv.name)

        with self.step('Wait for set_b files to move to faster ds'):
            fvs = []
            for fd in b_fds:
                fvs.append(fd.wait_for_mobility(fast_ds, attempt=60,
                                                interval=30, block=False))
            fvs = dispatcher_tuple(fvs)
            fvs.get()

        with self.step('Create smart-objective on set_a files'):
            cond_a = ctx.cluster.cli.condition_create(name='afile_activity',
                                                      pattern='a_file*',
                                                      activity='100')
            rule_a = '{}:{}'.format(cond_a.name, iopso.name)
            so_a = ctx.cluster.cli.smart_objective_create(name='Smart_Perf_A',
                                                          rule=rule_a)
            ctx.cluster.cli.share_objective_add(name=mount.share.name,
                                                objective=so_a.name,
                                                path='/')

        with self.step('Run FIO on set_a_IO files'):
            fio_cap = FioCaps(conf_file='fio-set-a')
            conf = copy(self._FIO_GLOBALS)
            conf.update({'filename': a_fds[0].filename})
            fio_cap.edit_conf_file(global_sec=conf,
                                   job_a=self._FIO_IOPS_OPT)
            fio = fio_cap.generic(options={'runtime': 300}, teardown=False)
            fio_fv = ctx.clients[0].execute_tool(fio, mount, block=False)

        with self.step('Wait for set_a_io_files files to move to faster ds'):
            a_fds[0].wait_for_mobility(fast_ds, attempt=60)
            fio_fv.get()

        with self.step('Verify set_a non IO files didn\'t move'):
            for fd in a_fds[1:]:
                fd.verify_location(slow_ds)

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason='Insufficient resources - Supply at least 2 DSes')
    @mark.skip_all
    @attr(jira='PD-17043', name='test_performance_mobility_playbook2')
    def test_performance_mobility_playbook2(self, get_smaller_ds_and_max_lv,
                                            get_mount_points):
        assert len(ctx.cluster.data_stores) == 1, \
            'Test cannot run with more than single storage-volumes after'
        min_ds, max_lv = get_smaller_ds_and_max_lv
        mount = get_mount_points[0]

        with self.step('Fill smaller DS to 80% capacity'):
            largest_fname = join(mount.path, 'Largest')
            largest_md5 = ctx.clients[0].quick_write_to_file(largest_fname,
                                                             count=3*1024)
            ctx.clients[0].quick_write_to_file(join(mount.path, '2gb'),
                                               count=2*1024)
            for i in xrange(2):
                ctx.clients[0].quick_write_to_file(join(mount.path,
                                                        '1gb-{}'.format(i)),
                                                   count=1024)
            for i in xrange(2):
                ballon_fname = join(mount.path, '0.5gb-{}'.format(i))
                ctx.clients[0].quick_write_to_file(ballon_fname,
                                                   count=1024/2)

        with self.step('Add Bigger DS'):
            max_ds = ctx.cluster.cli.ds_add(node_name=max_lv.node.name,
                                            logical_volume_name=max_lv.name)

        with self.step('Fill smaller DS to 90% capacity'):
            ctx.clients[0].write_to_file(ballon_fname, bs='1M', count=1024,
                                         oflag='append', conv='notrunc')

        with self.step('Wait for Largest file to get evicted'):
            fd = ctx.clients[0].file_declare(largest_fname, mount.share)
            fd.wait_for_mobility(max_ds, attempt=60)

        with self.step('Verify mobility integrity'):
            md5_post_eviction = ctx.clients[0].get_md5(largest_fname)
            if md5_post_eviction != largest_md5:
                raise FileIntegrityError('File {} got corrupted'
                                         ''.format(largest_fname))

    @staticmethod
    def _gather_vol_capabilities(vol):
        """
        This helper function will gather the performance capabilities of a given volume/DS
        :param vol: volume object
        :return: dictionary of performance capabilities of the vol
        """
        vol_cap = vol.storage_capabilities.performance
        read_bw = vol_cap.read_performance.bandwidth
        read_iops = vol_cap.read_performance.iops
        read_latency = vol_cap.read_performance.latency
        write_bw = vol_cap.write_performance.bandwidth
        write_iops = vol_cap.write_performance.iops
        write_latency = vol_cap.write_performance.latency
        perf_capabilities = pack('llllll', read_bw, read_iops, write_bw, write_iops, 0, 0)
        return perf_capabilities

    @staticmethod
    def _pick_capability(cap1, cap2):
        """
        This helper function will pick the capability that matches our defined criteria.
        criteria: maximum capability value (faster DS) that is greater than 15% of the minimum value (slower DS)
        for eg: if read_iops on slower DS is 150 and on faster DS is 200, read_iops is chosen
                if write iops on slower DS is 200 and on faster DS is 210, this capability is ignored.
        :param cap1: capability dict of slowDS
        :param cap2: capability dict of fasterDS
        :return: capability dict that matches our criteria
        """
        chosen_capability = {}
        for i in cap1.keys():
            min_cap_val = min(cap1[i], cap2[i])
            max_cap_val = max(cap1[i], cap2[i])
            cap_diff = max_cap_val - min_cap_val
            if cap_diff > 0.15 * min_cap_val:
                chosen_capability[i] = max_cap_val
                return chosen_capability
        assert "Could not find a capability which matches our criteria!"

    @staticmethod
    def _map_param(self, param):
        for k, v in self._PARAMS_MAPPER.items():
            if v == param:
                return k

    def derive_performance_capability(self, volume1, volume2):
        """
        This function will extract the performance capability and it's that needs to be configured in the objective
        :param volume1: slowDS object
        :param volume2: fastDS object
        :return: capability characteristics dict which lists the name, value of the performance capability along with
        the objective cli param that feeds into objective creation subroutine.
        """
        vol1_perf_cap = self._gather_vol_capabilities(volume1)
        vol2_perf_cap = self._gather_vol_capabilities(volume2)
        read_bw, read_iops, write_bw, write_iops, read_latency, write_latency = unpack('llllll', vol1_perf_cap)
        read_bw *= self.PB3_BW_CONVERSION
        write_bw *= self.PB3_BW_CONVERSION
        vol1_perf_cap = {'read_bw': read_bw, 'read_iops': read_iops, 'write_bw': write_bw, 'write_iops': write_iops,
                         'read_latency': read_latency, 'write_latency': write_latency}
        read_bw, read_iops, write_bw, write_iops, read_latency, write_latency = unpack('llllll', vol2_perf_cap)
        read_bw *= self.PB3_BW_CONVERSION
        write_bw *= self.PB3_BW_CONVERSION
        vol2_perf_cap = {'read_bw': read_bw, 'read_iops': read_iops, 'write_bw': write_bw, 'write_iops': write_iops,
                         'read_latency': read_latency, 'write_latency': write_latency}
        perf_capability = self._pick_capability(vol1_perf_cap, vol2_perf_cap)
        perf_capability_name_key = perf_capability.keys()
        perf_capability_value_value = perf_capability.values()
        perf_capability_name = perf_capability_name_key[0]
        perf_capability_value = perf_capability_value_value[0]
        objective_name = 'any_'+perf_capability_name
        objective_cli_param = self._map_param(self, perf_capability_name)
        capability_characteristics = {'capability_name': perf_capability_value,
                                      'capability_value': perf_capability_value,
                                      'objective_name': objective_name,
                                      'objective_cli_param': objective_cli_param
                                      }
        return capability_characteristics

    # Playbook3 - Durability based mobility
    @mark.skipif(len(ctx.data_stores) < 2,
                 reason='Insufficient resources - Supply at least 2 DSes')
    @mark.skip_all
    @attr(jira='PD-18562', name='test_performance_mobility_playbook3')
    def test_performance_mobility_playbook3(self, get_slow_ds,
                                            get_mount_points):
        """
        Verify mobility based on durability where the file initially gets
        placed on a faster storage with low durability and later moves to a
        slower storage with high durability both based on set objective.
        """
        assert len(ctx.cluster.data_stores) == 1, \
            'Test cannot run with more than single storage-volumes after'
        mount = get_mount_points[0]

        # Have 2 volumes - slow and fast
        slow_ds = get_slow_ds
        slow_ds_ip = slow_ds.node.mgmt_ip_address.address
        fast_ds_physical = choice([ds for ds in ctx.data_stores
                                   if ds.address != slow_ds_ip])

        # Update the durability numbers for slow_volume to be 99.9999%
        ctx.cluster.cli.volume_update(name=slow_ds.name, durability=9)

        with self.step('Add second faster ds'):
            fast_ds_name = fast_ds_physical.get_hostname()
            un_node = ctx.cluster.unauthenticated_nodes[fast_ds_name]
            ctx.cluster.cli._node_add(fast_ds_physical, name=un_node.name)
            lv = fast_ds_physical.get_free_export()
            fast_ds = ctx.cluster.cli.ds_add(node_name=fast_ds_name,
                                             logical_volume_name=lv.name)

            # Update the durability numbers for fast_volume to be 90%
            fast_ds = ctx.cluster.cli.volume_update(name=fast_ds.name,
                                                    durability=1)

        # Derive perf capabilities of the slow and the fast ds
        capabilities = self.derive_performance_capability(slow_ds, fast_ds)

        # Set the objective for ALL files with an ask of high performance but
        # low durability (90%) which can only be matched by the fast_volume
        with self.step('Create basic-objective for ALL files'):
            bo = ctx.cluster.cli.basic_objective_create(name=capabilities['objective_name'],
                                                        durability=1,
                                                        **{capabilities['objective_cli_param']:
                                                        long(capabilities['capability_value'])})

            ctx.cluster.cli.share_objective_add(name=mount.share.name,
                                                objective=bo.name,
                                                path='/')
        # SLO updates are not reflected in real time so sleep for few secs before file creation
        sleep(3)

        # Create 3 files and make sure the initial placement happens on the
        # fast_volume
        with self.step('Create files'):
            files = [join(mount.path + "/file" + str(file_index)) for file_index in xrange(3)]
            fds = []
            for f in files:
                ctx.clients[0].truncate(f, '500M')
                fd = ctx.clients[0].file_declare(f, mount.share)
                fds.append(fd)
                fd.verify_location(fast_ds)

        # Change the objective for ALL files with an ask of low performance
        # but high durability (99.9999%)
        # which can only be matched by the slow_volume
        with self.step('Update basic-objective for ALL files'):
            ctx.cluster.cli.basic_objective_update(name=capabilities['objective_name'],
                                                   durability=9,
                                                   **{capabilities['objective_cli_param']: 0})

        # Verify that ALL the files move from the fast_volume to the slow_volume
        with self.step('Wait for ALL files to move to slower ds but with '
                       'high durability'):
            fvs = []
            for fd in fds:
                fvs.append(fd.wait_for_mobility(slow_ds, attempt=60,
                                                interval=30, block=False))
            fvs = dispatcher_tuple(fvs)
            fvs.get()

    def get_ds_space_info(self, local_ds):
        space = []

        ls = [my_ds for my_ds in ctx.data_stores if my_ds.address == local_ds.node.mgmt_ip_address.address]
        local_export_path = local_ds.logical_volume.export_path
        df_output = ls[0].execute(['df', local_export_path,'|','grep', local_export_path])

        total_space = df_output.stdout.split()[1]
        space.append(total_space)

        free_space = df_output.stdout.split()[3]
        space.append(free_space)

        used_percent = df_output.stdout.split()[4]
        space.append(float(used_percent.strip(' \t\n\r%')))

        self._logger.info(space[0], space[1], space[2])
        return space

    def wait_for_file_eviction(self, used_percent_value, local_ds):
        count = 1
        with self.step('Wait for files to get evicted and used space go below %d' % used_percent_value+'%'):
            local_ds_space = self.get_ds_space_info(local_ds)
            while count < 20 and local_ds_space[2] >= used_percent_value:
                sleep(60)
                count += 1
                local_ds_space = self.get_ds_space_info(local_ds)
        return local_ds_space[2]

    # Playbook4 - Capacity Utilization threshold based mobility
    @mark.skipif(len(ctx.data_stores) < 2,
                 reason='Insufficient resources - Supply at least 2 DSes')
    @mark.skip_all
    @attr(jira='PD-20134', name='test_utilization_based_mobility_playbook4')
    def test_utilization_based_mobility_playbook4(self, get_smaller_ds_and_max_lv,
                                            get_mount_points):
        """
        Verify mobility based on capacity utilization threshold at
        default 75% and user defined 50% and 25%.
        """
        assert len(ctx.cluster.data_stores) == 1, \
            'Test cannot run with more than single storage-volumes after'

        mount = get_mount_points[0]
        src_min_ds, max_lv = get_smaller_ds_and_max_lv

        with self.step('Fill the volume to %d' % self.PB4_DEFAULT_UTIL_THRES+"%"):
            space = self.get_ds_space_info(src_min_ds)
            while space[2] < self.PB4_FILL_PERCENT:
                # create files of size varying from 1% to 5% of the volume size
                block_size = int(int(space[0]) * self.PB4_MIN_BS)
                filename = join(mount.path, 'file_{}'.format(str(RandStr())))
                rand_count = randint(50, 100)
                ctx.clients[0].write_to_file(filename, bs=block_size, count=rand_count, timeout=self.PB4_TIMEOUT)
                fd = ctx.clients[0].file_declare(filename, mount.share)
                space = self.get_ds_space_info(src_min_ds)

        with self.step('Add Bigger DS'):
            max_ds = ctx.cluster.cli.ds_add(node_name=max_lv.node.name,
                                            logical_volume_name=max_lv.name)
        space = self.get_ds_space_info(src_min_ds)
        ds_used_percent_value = self.wait_for_file_eviction(self.PB4_DEFAULT_UTIL_THRES, src_min_ds)

        # assert if src_min_ds used percentage is NOT less than 75%
        assert ds_used_percent_value < self.PB4_DEFAULT_UTIL_THRES, \
            'Volume %s used space is NOT less than %d percent. Current used space %s percent' % \
            (src_min_ds.name, self.PB4_DEFAULT_UTIL_THRES, ds_used_percent_value)

        # verify file eviction at utilization thresholds - 50% and 25%
        for util_threshold in [self.PB4_UTIL_THRES_1, self.PB4_UTIL_THRES_2]:
            with self.step('Update the utilization threshold of the volume to %d' % util_threshold):
                ctx.cluster.cli.volume_update(name=src_min_ds.name,
                                              utilization_threshold=util_threshold)
            ds_used_percent_value = self.wait_for_file_eviction(util_threshold, src_min_ds)

            assert ds_used_percent_value <= util_threshold, \
                'Volume %s used space is NOT less than or equal to %d percent. Current used space %s percent' % \
                (src_min_ds.name, util_threshold, ds_used_percent_value)

    # @mark.skipif(len(ctx.data_stores) < 2,
    #              reason='Insufficient resources - Supply at least 2 DSes')
    # @attr(jira='next', name='test_object_not_comply_simple_model')
    # def test_object_not_comply_simple_model(self, get_mount_points,
    #                                         get_singles_dses):
    #     mount = get_mount_points[0]
    #     fast_ds = choice(get_singles_dses)
    #     slow_ds = choice([ds for ds in get_singles_dses if ds != fast_ds])
    #     physical = slow_ds.node.mgmt_ip_address.address
    #     with ctx.get_ds('address', physical).tc.limit('pddata', latency=1):
    #     # Run some IO
    #     fio_cap = FioCaps(conf_file='fio-lat-test')
    #     fio_cap.edit_conf_file(global_sec=self._FIO_GLOBALS,
    #                            baloon_fast=self._FIO_LAT_OPT)
    #     fio = fio_cap.generic(options={'directory': mount.path,
    #                                    'runtime': 1}, teardown=False)
    #     res_fast = ctx.clients[0].execute_tool(fio, mount)
    #     fast_ds_filename = join(res_fast._data_target, 'baloon_fast.1.0')
    #
    #     fio_cap.edit_conf_file(global_sec=self._FIO_GLOBALS,
    #                            baloon_slow=self._FIO_LAT_OPT)
    #     fio = fio_cap.generic(options={'directory': mount.path,
    #                                    'runtime': 1}, teardown=False)
    #     res_slow = ctx.clients[0].execute_tool(fio, mount)
    #     slow_ds_filename = join(res_slow._data_target, 'baloon_slow.1.0')
    #
    #     # Move file to correct DS
    #     fd_baloon_fast = ctx.clients[0].file_declare(fast_ds_filename,
    #                                                  mount.share)
    #     fd_baloon_slow = ctx.clients[0].file_declare(slow_ds_filename,
    #                                                  mount.share)
    #     if fast_ds not in [i.data_store for i in fd_baloon_fast.instances]:
    #         fd_baloon_fast.trigger_move(fast_ds)
    #     if slow_ds not in [i.data_store for i in fd_baloon_slow.instances]:
    #         fd_baloon_slow.trigger_move(slow_ds)
    #
    #     fio_cap.edit_conf_file(global_sec=self._FIO_GLOBALS,
    #                            baloon_fast=self._FIO_LAT_OPT)
    #     fio = fio_cap.generic(options={'directory': mount.path},
    #                           teardown=False)
    #     ctx.clients[0].execute_tool(fio, mount)
    #
    #     fio_cap.edit_conf_file(global_sec=self._FIO_GLOBALS,
    #                            baloon_slow=self._FIO_LAT_OPT)
    #     fio = fio_cap.generic(options={'directory': mount.path},
    #                           teardown=False)
    #     ctx.clients[0].execute_tool(fio, mount)
    #     sleep(5)
    #
    #     aggr = ctx.cluster.influx_db.aggregate_performance
    #     aggr_perf_fast_ds = aggr(type='ds', id=fast_ds.internal_id)[0]
    #     fast_read_lat = aggr_perf_fast_ds.avgReadRequestDuration[-1]
    #     aggr_perf_slow_ds = aggr(type='ds', id=slow_ds.internal_id)[0]
    #     slow_read_lat = aggr_perf_slow_ds.avgReadRequestDuration[-1]
    #
    #     f1 = join(mount.path, 'not_meet_obj')
    #     f2 = join(mount.path, 'exactly_obj')
    #     f3 = join(mount.path, 'achieve_obj')
    #     fds = []
    #     for f in [f1, f2, f3]:
    #         ctx.clients[0].touch(f)
    #         fd = ctx.clients[0].file_declare(f, mount.share)
    #         if slow_ds not in [i.data_store for i in fd.instances]:
    #             fd.trigger_move(slow_ds)
    #         fds.append(fd)
    #
    #     ctx.cluster.cli.basic_objective_create(name='gold',
    #                                            max_read_response_time=fast_read_lat/2)
    #     ctx.cluster.cli.share_objective_add(name=mount.share.name,
    #                                         objective='not_meet_obj',
    #                                         path=f1)
    #     ctx.cluster.cli.basic_objective_create(name='silver',
    #                                            max_read_response_time=fast_read_lat)
    #     ctx.cluster.cli.share_objective_add(name=mount.share.name,
    #                                         objective='exactly_obj',
    #                                         path=f2)
    #     ctx.cluster.cli.basic_objective_create(name='bronze',
    #                                            max_read_response_time=fast_read_lat*2)
    #     ctx.cluster.cli.share_objective_add(name=mount.share.name,
    #                                         objective='achieve_obj',
    #                                         path=f3)
    #
    #     sleep(10)
    #     for f in [f1, f2, f3]:
    #         print ctx.clients[0].file_declare(f, mount.share)
    #
    #     print fast_read_lat, slow_read_lat
