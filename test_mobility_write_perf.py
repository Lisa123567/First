__author__ = 'sudhakar'

from os.path import join
from time import sleep

from _pytest.python import yield_fixture

from tonat.context import ctx
from tonat.test_base import TestBase, attr
from tonat.rand_ex import RandStr


class TestMobilityWritePerf(TestBase):
    
    @yield_fixture
    def create_dses(self):
        dses = ctx.cluster.data_stores.values()
        ds_in_use = [ds.logical_volume.ip_addresses[0].address for ds in ctx.cluster.data_stores.values()]
        self._logger.info(ds_in_use)
        for ds in ctx.data_stores:
            if ds.address in ds_in_use:
                continue
            node = ctx.cluster.cli._node_add(ds, async=True)
            export = ds.get_free_export()
            dses.append(ctx.cluster.cli.ds_add(node_name=node.name,
                                               logical_volume_name=export.name,
                                               async=True))
        yield dses
        #teardown

    @attr(jira='PD-11248', name='test_write_perf_mob')
    def test_write_perf_mob(self, create_dses, get_nfs4_2_mount_points):
        dses = create_dses
        my_client_id = 2

        mounts = get_nfs4_2_mount_points
        rand_str = "test_write_perf_mob"
        
        # Create basic-objective
        slo = ctx.cluster.cli.basic_objective_create(name='SLO_'+rand_str, min_write_iops='500')

        # Create smart-objective
        rule = '{}:{}'.format("default-condition", slo.name)
        smart_objective = ctx.cluster.cli.smart_objective_create(name='PROFILE_'+rand_str, rule=rule)

        # Add objective to share
        ctx.cluster.cli.share_objective_add(name=mounts[0].share.name, path='/', objective=smart_objective.name)

        rand_name = str(RandStr())+'.txt'
        filename = join(mounts[0].path, rand_name)
        fd = ctx.clients[0].open_file(filename, 1052738, 0644)
        ctx.clients[0].write_file(fd, 0, 10, 1)
        ctx.clients[0].close_file(fd)

        fd = ctx.clients[0].file_declare(filename, mounts[0].share)
        inode = fd.inode
        obj_id1 = fd.obj_id
        basic_objective_id = fd.slo_id
        smart_objective_id = fd.profile_id
        inst_id1 = fd.instances[0].instance_id
        ds_name1 = fd.instances[0].data_store.name
        ds_id1 = fd.instances[0].data_store.internal_id

        self._logger.info("=====================================================================")
        self._logger.info("File Location: Before issuing the layout_stats message to degrade DS1")
        self._logger.info("=====================================================================")
        self._logger.info('Inode Number : %s' % inode)
        self._logger.info('OBJ ID       : %d' % obj_id1)
        self._logger.info('Instance ID  : %d' % inst_id1)
        self._logger.info('SLO ID       : %s' % basic_objective_id)
        self._logger.info('Profile ID   : %s' % smart_objective_id)
        self._logger.info('DS Name      : %s' % ds_name1)
        self._logger.info('DS ID        : %d' % ds_id1)

        if dses[0].internal_id == ds_id1:
            my_ds = dses[0]
        else:
            my_ds = dses[1]

        for x in range(1, 6):

            self._logger.info("=================================================================================")
            self._logger.info('%d: Degrade DS ID - %d performance by sending LOW IOPS client_layout_stats using '
                              'Instance ID - %d' % (x, ds_id1, inst_id1))
            self._logger.info("=================================================================================")

            variant = 100

            # Send the client layout_stats for DS1 in loop to degrade the DS1 performance
            ctx.cluster.kafka.send_layout_stats_msg(my_ds, mounts[0].share, object_id=obj_id1, instance_id=inst_id1,
                                                    client_id=my_client_id, inode=inode, profile_id=smart_objective_id or 1,
                                                    rule_id=1, slo_id=slo_id,
                                                    read_bytes_completed=0, read_bytes_not_delivered=0,
                                                    read_bytes_requested=0, read_ops_requested=0,
                                                    read_ops_completed=0, read_total_busy_time=0,
                                                    read_aggregate_completion_time=0, read_iops_requested_delta=0,
                                                    read_bytes_completed_per_sec=0, read_current_op_latency_nsec=0,
                                                    write_bytes_completed=variant*x, write_bytes_not_delivered=0,
                                                    write_bytes_requested=variant*x, write_ops_requested=variant*x,
                                                    write_ops_completed=variant*x, write_total_busy_time=1000000000*x,
                                                    write_aggregate_completion_time=1000000000*x,
                                                    write_iops_requested_delta=0, write_bytes_completed_per_sec=0,
                                                    write_current_op_latency_nsec=0)
            sleep(1)

        # Sleep sometime for the grading and mobility to happen
        sleep_time = 180
        self._logger.info('Sleeping %s secs for the grading and mobility to happen' % sleep_time)
        sleep(sleep_time)

        fd = ctx.clients[0].file_declare(filename, mounts[0].share)
        inode = fd.inode
        obj_id2 = fd.obj_id
        basic_objective_id2 = fd.slo_id
        smart_objective_id2 = fd.profile_id
        inst_id2 = fd.instances[0].instance_id
        ds_name2 = fd.instances[0].data_store.name
        ds_id2 = fd.instances[0].data_store.internal_id

        self._logger.info("====================================================================")
        self._logger.info("File Location: After issuing the layout_stats message to degrade DS1")
        self._logger.info("====================================================================")
        self._logger.info('Inode Number : %s' % inode)
        self._logger.info('OBJ ID       : %d' % obj_id2)
        self._logger.info('Instance ID  : %d' % inst_id2)
        self._logger.info('SLO ID       : %s' % basic_objective_id2)
        self._logger.info('Profile ID   : %s' % smart_objective_id2)
        self._logger.info('DS Name      : %s' % ds_name2)
        self._logger.info('DS ID        : %d' % ds_id2)

        # Verify Performance based mobility
        if ds_id1 == ds_id2:
            self._logger.info("========================================================================")
            self._logger.info('Performance (IOPS) based data mobility is UNSUCCESSFUL')
            self._logger.info('DS ID on which file was residing before performance trigger  : %d' % ds_id1)
            self._logger.info('DS ID on which file was residing after performance trigger   : %d' % ds_id2)
            self._logger.info("========================================================================")
        else:
            self._logger.info("========================================================================")
            self._logger.info('Performance (IOPS) based data mobility is SUCCESSFUL')
            self._logger.info('DS ID on which file was residing before performance trigger  : %d' % ds_id1)
            self._logger.info('DS ID on which file was residing after performance trigger   : %d' % ds_id2)
            self._logger.info("========================================================================")

        # Remove objective from share
        ctx.cluster.cli.share_objective_remove(name=mounts[0].share.name, path='/')

        # Delete smart-objective
        ctx.cluster.cli.smart_objective_delete(name='PROFILE_'+rand_str)

        # Delete basic-objective
        ctx.cluster.cli.basic_objective_delete(name='SLO_'+rand_str)

        ctx.clients[0].close_agent()

        # TODO - RESET postgres database on the DD


    @attr(jira='PD-11253', name='test_write_perf_mob_2')
    def test_write_perf_mob_2(self, create_dses, get_nfs4_1_mount_points):
        dses = create_dses

        # Create dummy files on DS1 and DS2
        mounts = get_nfs4_1_mount_points
        ds_id1 = ds_id2 = 99
        y = 1
        my_client_id = 2

        while ds_id1 == ds_id2:
            file_name1 = join(mounts[0].path, 'dummyfile_%d' % y)
            y += y
            fd = ctx.clients[0].open_file(file_name1, 1052738, 0644)
            ctx.clients[0].write_file(fd, 0, 10, 1)
            ctx.clients[0].close_file(fd)
            fd = ctx.clients[0].file_declare(file_name1, mounts[0].share)

            if ds_id1 == 99:
                # store the information needed to send mocked layout stats for ds_id1
                inode1 = fd.inode
                obj_id1 = fd.obj_id
                slo_id1 = fd.slo_id
                profile_id1 = fd.profile_id
                inst_id1 = fd.instances[0].instance_id
                ds_id1 = fd.instances[0].data_store.internal_id

            # store the information needed to send mocked layout stats for ds_id2
            inode2 = fd.inode
            obj_id2 = fd.obj_id
            slo_id2 = fd.slo_id
            profile_id2 = fd.profile_id
            inst_id2 = fd.instances[0].instance_id
            ds_id2 = fd.instances[0].data_store.internal_id

            self._logger.info("======================")
            self._logger.info("Create Dummy File: %d" % y)
            self._logger.info("======================")
            self._logger.info('Inode Number : %s' % fd.inode)
            self._logger.info('OBJ ID       : %d' % fd.obj_id)
            self._logger.info('Instance ID  : %d' % fd.instances[0].instance_id)
            self._logger.info('SLO ID       : %s' % fd.slo_id)
            self._logger.info('Profile ID   : %s' % fd.profile_id)
            self._logger.info('DS Name      : %s' % fd.instances[0].data_store.name)
            self._logger.info('DS ID        : %d' % fd.instances[0].data_store.internal_id)
            self._logger.info('DS Name      : %s' % fd.instances[0].data_store.name)

        self._logger.info("Done creating at least one dummy file on each DS")

        if dses[0].internal_id == ds_id1:
            my_ds1 = dses[0]
        else:
            my_ds1 = dses[1]

        for x in range(1, 5):

            self._logger.info("=================================================================================")
            self._logger.info('%d: Sending HIGH IOPS client_layout_stats to DS ID - %d using Instance ID - %d'
                              % (x, ds_id1, inst_id1))
            self._logger.info("=================================================================================")

            variant = 500

            # Send the client layout_stats for DS2 to train it as a high performer
            ctx.cluster.kafka.send_layout_stats_msg(my_ds1, mounts[0].share, object_id=obj_id1, instance_id=inst_id1,
                                                    client_id=my_client_id, inode=inode1, profile_id=profile_id1 or 1,
                                                    rule_id=1, slo_id=slo_id1,
                                                    read_bytes_completed=0, read_bytes_not_delivered=0,
                                                    read_bytes_requested=0, read_ops_requested=0,
                                                    read_ops_completed=0, read_total_busy_time=0,
                                                    read_aggregate_completion_time=0, read_iops_requested_delta=0,
                                                    read_bytes_completed_per_sec=0, read_current_op_latency_nsec=0,
                                                    write_bytes_completed=variant*x, write_bytes_not_delivered=0,
                                                    write_bytes_requested=variant*x, write_ops_requested=variant*x,
                                                    write_ops_completed=variant*x, write_total_busy_time=1000000000*x,
                                                    write_aggregate_completion_time=1000000000*x,
                                                    write_iops_requested_delta=0, write_bytes_completed_per_sec=0,
                                                    write_current_op_latency_nsec=0)
            sleep(2)

        if dses[0].internal_id == ds_id2:
            my_ds2 = dses[0]
        else:
            my_ds2 = dses[1]

        for x in range(1, 5):

            self._logger.info("=================================================================================")
            self._logger.info('%d: Sending MEDIUM IOPS client_layout_stats to DS ID - %d using Instance ID - %d'
                              % (x, ds_id2, inst_id2))
            self._logger.info("=================================================================================")

            variant = 200

            # Send the client layout_stats for DS2 to train it as a medium performer
            ctx.cluster.kafka.send_layout_stats_msg(my_ds2, mounts[0].share, object_id=obj_id2, instance_id=inst_id2,
                                                    client_id=my_client_id, inode=inode2, profile_id=profile_id2 or 1,
                                                    rule_id=1, slo_id=slo_id2,
                                                    read_bytes_completed=0, read_bytes_not_delivered=0,
                                                    read_bytes_requested=0, read_ops_requested=0,
                                                    read_ops_completed=0, read_total_busy_time=0,
                                                    read_aggregate_completion_time=0, read_iops_requested_delta=0,
                                                    read_bytes_completed_per_sec=0, read_current_op_latency_nsec=0,
                                                    write_bytes_completed=variant*x, write_bytes_not_delivered=0,
                                                    write_bytes_requested=variant*x, write_ops_requested=variant*x,
                                                    write_ops_completed=variant*x, write_total_busy_time=1000000000*x,
                                                    write_aggregate_completion_time=1000000000*x,
                                                    write_iops_requested_delta=0, write_bytes_completed_per_sec=0,
                                                    write_current_op_latency_nsec=0)
            sleep(2)

        rand_str = "test_write_perf_mob"
        # Create basic-objective
        slo = ctx.cluster.cli.basic_objective_create(name='SLO_'+rand_str, min_write_iops='500')

        # Create smart-objective
        rule = '{}:{}'.format("default-condition", slo.name)
        profile = ctx.cluster.cli.smart_objective_create(name='PROFILE_'+rand_str, rule=rule)

        # Add profile to share
        ctx.cluster.cli.share_profile_add(name=mounts[0].share.name, path='/', profile=profile.name)

        rand_name = str(RandStr())+'.txt'
        file_name2 = join(mounts[0].path, rand_name)
        fd = ctx.clients[0].open_file(file_name2, 1052738, 0644)
        ctx.clients[0].write_file(fd, 0, 10, 1)
        ctx.clients[0].close_file(fd)
        fd3 = ctx.clients[0].file_declare(file_name2, mounts[0].share)

        inode3 = fd3.inode
        obj_id3 = fd3.obj_id
        slo_id3 = fd3.slo_id
        profile_id3 = fd3.profile_id
        inst_id3 = fd3.instances[0].instance_id
        ds_name3 = fd3.instances[0].data_store.name
        ds_id3 = fd3.instances[0].data_store.internal_id

        self._logger.info("=====================================================================")
        self._logger.info("File Location: Before issuing the layout_stats message to degrade DS1")
        self._logger.info("=====================================================================")
        self._logger.info('Inode Number : %s' % fd3.inode)
        self._logger.info('OBJ ID       : %d' % fd3.obj_id)
        self._logger.info('Instance ID  : %d' % fd3.instances[0].instance_id)
        self._logger.info('SLO ID       : %s' % fd3.slo_id)
        self._logger.info('Profile ID   : %s' % fd3.profile_id)
        self._logger.info('DS Name      : %s' % fd3.instances[0].data_store.name)
        self._logger.info('DS ID        : %d' % fd3.instances[0].data_store.internal_id)
        self._logger.info('DS Name      : %s' % ds_name3)

        # Verify Initial placement
        if ds_id3 == ds_id1:
            self._logger.info("==================================================")
            self._logger.info('Initial placement is SUCCESSFUL')
            self._logger.info('DS ID with high IOPS                 : %d' % ds_id1)
            self._logger.info('DS ID on which the new file is placed: %d' % ds_id3)
            self._logger.info("==================================================")
        else:
            self._logger.info("==================================================")
            self._logger.info('Initial placement is UNSUCCESSFUL')
            self._logger.info('DS ID with high IOPS                 : %d' % ds_id1)
            self._logger.info('DS ID on which the new file is placed: %d' % ds_id3)
            self._logger.info("==================================================")

        for x in range(1, 60):

            self._logger.info("=================================================================================")
            self._logger.info('%d: Degrade DS ID - %d performance by sending LOW IOPS client_layout_stats using '
                              'Instance ID - %d' % (x, ds_id3, inst_id3))
            self._logger.info("=================================================================================")

            variant = 5

            # Send the client layout_stats for DS1 in loop - 5K IOPS - Degrade the DS1 performance
            ctx.cluster.kafka.send_layout_stats_msg(my_ds1, mounts[0].share, object_id=obj_id3, instance_id=inst_id3,
                                                    client_id=my_client_id, inode=inode3, profile_id=profile_id3 or 1,
                                                    rule_id=1, slo_id=slo_id3,
                                                    read_bytes_completed=0, read_bytes_not_delivered=0,
                                                    read_bytes_requested=0, read_ops_requested=0,
                                                    read_ops_completed=0, read_total_busy_time=0,
                                                    read_aggregate_completion_time=0, read_iops_requested_delta=0,
                                                    read_bytes_completed_per_sec=0, read_current_op_latency_nsec=0,
                                                    write_bytes_completed=variant*x, write_bytes_not_delivered=0,
                                                    write_bytes_requested=variant*x, write_ops_requested=variant*x,
                                                    write_ops_completed=variant*x, write_total_busy_time=1000000000*x,
                                                    write_aggregate_completion_time=1000000000*x,
                                                    write_iops_requested_delta=0, write_bytes_completed_per_sec=0,
                                                    write_current_op_latency_nsec=0)
            sleep(15)

        # Sleep sometime for the grading and mobility to happen
        sleep_time = 180
        self._logger.info('Sleeping %s secs for the grading and mobility to happen' % sleep_time)
        sleep(sleep_time)

        fd4 = ctx.clients[0].file_declare(file_name2, mounts[0].share)
        inode4 = fd4.inode
        obj_id4 = fd4.obj_id
        slo_id4 = fd4.slo_id
        profile_id4 = fd4.profile_id
        inst_id4 = fd4.instances[0].instance_id
        ds_name4 = fd4.instances[0].data_store.name
        ds_id4 = fd4.instances[0].data_store.internal_id

        self._logger.info("====================================================================")
        self._logger.info("File Location: After issuing the layout_stats message to degrade DS1")
        self._logger.info("====================================================================")
        self._logger.info('Inode Number : %s' % inode4)
        self._logger.info('OBJ ID       : %d' % obj_id4)
        self._logger.info('Instance ID  : %d' % inst_id4)
        self._logger.info('SLO ID       : %s' % slo_id4)
        self._logger.info('Profile ID   : %s' % profile_id4)
        self._logger.info('DS Name      : %s' % ds_name4)
        self._logger.info('DS ID        : %d' % ds_id4)

        # Verify Performance based mobility
        if ds_id3 == ds_id4:
            self._logger.info("========================================================================")
            self._logger.info('Performance (IOPS) based data mobility is UNSUCCESSFUL')
            self._logger.info('DS ID on which file was residing before performance trigger  : %d' % ds_id3)
            self._logger.info('DS ID on which file was residing after performance trigger   : %d' % ds_id4)
            self._logger.info("========================================================================")
        else:
            self._logger.info("========================================================================")
            self._logger.info('Performance (IOPS) based data mobility is SUCCESSFUL')
            self._logger.info('DS ID on which file was residing before performance trigger  : %d' % ds_id3)
            self._logger.info('DS ID on which file was residing after performance trigger   : %d' % ds_id4)
            self._logger.info("========================================================================")

        # Remove profile from share
        ctx.cluster.cli.share_profile_remove(name=mounts[0].share.name, path='/')

        # Delete smart-objective
        ctx.cluster.cli.smart_objective_delete(name='PROFILE_'+rand_str)

        # Delete basic-objective
        ctx.cluster.cli.basic_objective_delete(name='SLO_'+rand_str)

        ctx.clients[0].close_agent()

        # TODO - RESET postgres database on the DD
