from __future__ import division

__author__ = 'rkissos'

from time import sleep, time
from random import choice, shuffle
from os.path import join

from client.models.NodeType import NodeType

from tonat.context import ctx
from tonat.test_base import TestBase, attr
from tonat.product.nodes.client import Client
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.nfsctl import NfsVersion
from tonat.product.objects.mount import Mount
from tonat.rand_ex import RandPath
from tonat.conn_lib.ssh_conn import SshConn, ExtendedConn

from pd_common import utils
from pd_tools.fio_caps import FioCaps


class TestPocScript(TestBase):

    _TEMPLATE = 'centos71-v1'
    _CLIENTS_DIR = 'Clients'
    _VVOL_NAME = 'poc-vvol'
    _POC_SMART_SHARE = None
    _COPY_TIMEOUT = 300
    _EPSILON = 0.0001
    _VMPREFIX = 'poc_vm_{}'
    _SNAPSHOT_NAME = 'poc-test'
    _SNAPSHOT_DESC = 'poc-test'
    _POC_VASA_NAME = 'PD-POC-VASA'

    def assert_approximately_equal(self, expected, found, name=None):
        name = ' ' + name if name else ''
        ratio = expected / found
        diff = expected - found
        msg = ('Expected{} capacity {}, found {} '
               '(difference of {}, {}%)').format(
                 name, expected, found, diff, (ratio - 1) * 100)
        self._logger.info(msg)
        assert abs(1 - ratio) < self._EPSILON, msg

    def _clone_vm_on_vvol(self, hypervisor, vm_name=None):
        vm_name = vm_name or self._VMPREFIX.format(utils.lower_generator(5))
        vvol_ds = hypervisor._get_datastore_by_name(self._VVOL_NAME)
        folder = hypervisor.get_folder_by_name(self._CLIENTS_DIR)
        self._logger.info('Cloning template {!r} to folder {!r} '
                          'with name {!r} in datastore {!r}'.format(
                              self._TEMPLATE, self._CLIENTS_DIR,
                              vm_name, self._VVOL_NAME))
        clonetask = hypervisor.clone_vm(self._TEMPLATE, vm_name,
                                        datastore=vvol_ds, folder=folder)
        client = hypervisor.get_first_vm(vm_name)
        self._logger.debug('Clone succeeded, powering on VM')
        client.poweron()
        self._logger.debug('Waiting for IP')
        address = client.wait_for_ip()
        return client, address

    def _get_largest_vmdk(self, share, vm_name):
        folder = '/mnt/pdfs/{}/{}/*.vmdk'.format(share.name, vm_name)
        out = ctx.cluster.execute(['stat', '-c', '%s %n', folder]).stdout
        vmdk = None
        max_size = 0
        for line in out.split('\n')[:-1]:
            size, name = line.split()
            if int(size) > max_size:
                vmdk = name
                max_size = int(size)
        assert vmdk, 'Couldn\'t find any VMDK file on {}'.format(folder)
        return vmdk

    @attr('poc_script', jira='PD-13028', name='test_poc_script_phase_1')
    def test_poc_script_phase_1(self):

        pd_dss = ctx.data_stores.filter(NodeType.PD)
        other_dss = ctx.data_stores - pd_dss
        for ds in pd_dss:
            with self.step('pd-ds node-add'):
                ds_name = ds.get_hostname()
                un_node = ctx.cluster.unauthenticated_nodes[ds_name]
                ctx.cluster.cli._node_add(ds, name=un_node.name,
                                          clish_username='admin',
                                          clish_password='admin')

            with self.step('pd-ds volume-add'):
                lv = ds.get_free_export()
                ctx.cluster.cli.ds_add(node_name=ds_name,
                                       logical_volume_name=lv.name,
                                       clish_username='admin',
                                       clish_password='admin')
        for ds in other_dss:
            ds_name = 'poc_{0}'.format(utils.lower_generator(5))
            lv = ds.create_share(join(choice(ds.get_fses()).name, ds_name))
            with self.step('3party node-add'):
                ctx.cluster.cli._node_add(ds, name=ds_name,
                                          clish_username='admin',
                                          clish_password='admin')

            with self.step('3party ds-add'):
                ctx.cluster.cli.ds_add(node_name=ds_name,
                                       logical_volume_name=lv.name,
                                       clish_username='admin',
                                       clish_password='admin')

        with self.step('share-create'):
            share = ctx.cluster.cli.share_create(name='poc', path='/poc',
                                                 export_option='*,RW,no-root-squash',
                                                 clish_username='admin',
                                                 clish_password='admin')

        for dp in ctx.data_portals:
            with self.step('data-portal node-add'):
                dp_name = dp.get_hostname()
                un_node = ctx.cluster.unauthenticated_nodes[dp_name]
                ctx.cluster.cli._node_add(dp, name=un_node.name,
                                          clish_username='admin',
                                          clish_password='admin')

        secs = 60
        self._logger.debug('Sleeping for {} s'.format(secs))
        sleep(secs)
        with self.step('Register vasa provider'):
            hypervisor = ctx.hypervisors[0]
            vasa_address = ctx.data_director.address
            vasa_username, vasa_password = ctx.data_director.auth
            hypervisor.register_vasa_provider(address=vasa_address,
                                              prov_username=vasa_username,
                                              prov_password=vasa_password,
                                              name=_POC_VASA_NAME)

        sleep(secs)
        with self.step('Add vvol datastore'):
            hosts = [host.name for host in hypervisor._get_hosts()]
            hypervisor.add_vvol_datastore(self._VVOL_NAME, share.name,
                                          hosts_list=hosts)
        sleep(secs*3)

        with self.step('Create vm on VVOL'):
            client, address = self._clone_vm_on_vvol(hypervisor)
            ctx.clients += Client(address=address,
                                  username='root', password='Tonian',
                                  hw_mgmt=(hypervisor._address,
                                           hypervisor._username,
                                           hypervisor._password,
                                           client.name))
        sleep(secs*3)


    @attr('poc_script', jira='PD-13029', name='test_poc_script_phase_2')
    def test_poc_script_phase_2(self, get_pd_share):

        poc_share = get_pd_share
        with self.step('Create 3 basic objectives'):
            basic_obj_create = ctx.cluster.cli.basic_objective_create
            high_obj = basic_obj_create(name='high_perf1', min_read_iops=6000)
            low_obj = basic_obj_create(name='low_cost1', min_read_iops=3000)
            archive_obj = basic_obj_create(name='archive1', min_read_iops=300)

        with self.step('Smart objective with 3 rules'):
            rules = []
            cond1 = ctx.cluster.cli.condition_create(name='high_activity1',
                                                     pattern='*.vmdk',
                                                     activity='3000')
            rules.append('{}:{}'.format(cond1.name, high_obj.name))
            cond2 = ctx.cluster.cli.condition_create(name='low_activity',
                                                     pattern='*.vmdk',
                                                     activity='1')
            rules.append('{}:{}'.format(cond2.name, low_obj.name))
            cond3 = ctx.cluster.cli.condition_create(name='inactivity',
                                                     pattern='*.vmdk',
                                                     inactivity='5400s')
            rules.append('{}:{}'.format(cond3.name, archive_obj.name))
            poc_smart = ctx.cluster.cli.smart_objective_create(
                name='poc_smart_obj', rule=rules)

        with self.step('Apply smart objective to a share'):
            obj = ctx.cluster.cli.share_objective_add(name=poc_share.name,
                                                      objective=poc_smart.name,
                                                      path='/')
            self._POC_SMART_SHARE = obj

    @attr('poc_script', jira='PD-13181', name='test_poc_script_phase_3')
    def test_poc_script_phase_3(self, get_pd_share):

        hypervisor = ctx.hypervisor
        clients = [hypervisor.get_first_vm(c.name)
                   for c in hypervisor._get_vms_in_folder(self._CLIENTS_DIR)]
        if not ctx.clients and not clients:
            with self.step('Create vm on VVOL'):
                client, address = self._clone_vm_on_vvol(hypervisor)
                ctx.clients += Client(address=address,
                                      username='root', password='Tonian',
                                      hw_mgmt=(hypervisor._address,
                                               hypervisor._username,
                                               hypervisor._password,
                                               client.vm.name))
        else:
            for c in clients:
                ctx.clients += Client(address=c.wait_for_ip(),
                                      username='root', password='Tonian',
                                      hw_mgmt=(hypervisor._address,
                                               hypervisor._username,
                                               hypervisor._password,
                                               c.name))

        with self.step('share-objective-remove'):
            obj = self._POC_SMART_SHARE
            if obj:
                ctx.cluster.cli.share_objective_remove(name=obj.name,
                                                       path='/')
            self._POC_SMART_SHARE = None

        with self.step('Deploy tools on clients'):
            ctx.clients.deploy()

        with self.step('limit datastore bandwidth'):
            # get non vm ds caps bw (pdfs cli on vmdk file)
            vmdk_file = self._get_largest_vmdk(get_pd_share,
                                               ctx.clients[0].hw._vm_name)
            vmdk_inode = ctx.cluster.file_inode(vmdk_file)
            fi = ctx.cluster.get_file_info(get_pd_share, vmdk_inode)
            vm_ds = fi.instances[0].data_store
            vm_ds_ip = vm_ds.node.mgmt_ip_address.address
            ds = choice([ds for ds in ctx.cluster.data_stores.values()
                         if ds != vm_ds])
            ds_write_bw = ds.storage_capabilities.performance.write_bandwidth
            # set TC on vm ds to half
            with ctx.get_ds('address', vm_ds_ip).tc.limit('pddata',
                                                          bw=ds_write_bw/2):
                with self.step('share-objective-add'):
                    poc_share = get_pd_share
                    poc_smart = obj or ctx.cluster.smart_objectives.\
                        get('poc_smart_obj')
                    obj = ctx.cluster.cli.\
                        share_objective_add(name=poc_share.name,
                                            objective=poc_smart.name,
                                            path='/')
                    self._POC_SMART_SHARE = obj

                with self.step('run IO on VM'):
                    dir_name = str(RandPath())
                    ctx.clients.mkdirs(dir_name)
                    dirs = dispatcher_tuple([Mount(poc_share,
                                                   NfsVersion.no_nfs,
                                                   path=dir_name)
                                             for _ in ctx.clients])
                    fio = FioCaps().basic(teardown=False)
                    ctx.clients.execute_tool(fio, dirs)

            with self.step('Wait for move'):
                # wait for move, timeout - 300 seconds (pdfs cli on vmdk file)
                t2 = t1 = time()
                while t2 - t1 < self._COPY_TIMEOUT:
                    new_fi = ctx.cluster.get_file_info(get_pd_share, vmdk_inode)
                    if {vm_ds} != set([inst.data_store
                                       for inst in new_fi.instances]):
                        break
                    sleep(1)
                    t2 = time()
                assert t2 - t1 < self._COPY_TIMEOUT, \
                    'the move process took more than {} seconds' \
                    ''.format(self._COPY_TIMEOUT)

    @attr('poc_script', jira='PD-14210', name='test_poc_script_phase_4')
    def test_poc_script_phase_4(self):
        with self.step('pd-ds volume-add'):
            hypervisor = ctx.hypervisor
            pd_dss = list(ctx.data_stores.filter(NodeType.PD, attr='type'))
            shuffle(pd_dss)
            for ds in pd_dss:
                lv = ds.get_free_export()
                if lv is not None:
                    break
            else:
                raise RuntimeError('no free exports')
            ds_name = ds.get_hostname()
            self._logger.debug('datastore host name: {}'.format(ds_name))
            vm = hypervisor.get_first_vm(self._VMPREFIX.format(''))
            #vvol = hypervisor._get_datastore_by_name(self._VVOL_NAME)
            vvol = vm.get_datastore()
            vvol_free_space_before = vvol.info.freeSpace
            self._logger.debug(
                'Initial free space on VVOL: {}'.format(vvol_free_space_before))
            lv_total_space = lv.capacity.total
            self._logger.debug(
                'Logical volume total space: {}'.format(lv_total_space))
            total_space_before = ctx.data_director.capacity['total']
            self._logger.debug(
                'DataSphere total space: {}'.format(total_space_before))
            ctx.cluster.cli.ds_add(node_name=ds_name,
                                   logical_volume_name=lv.name,
                                   clish_username='admin',
                                   clish_password='admin')
            secs = 60*2
            sleep(secs)

        with self.step('validate data space capacity increase'):
            total_space_after = ctx.data_director.capacity['total']
            expected = total_space_before + lv_total_space
            found = total_space_after
            self.assert_approximately_equal(expected, found, 'DataSphere')

        with self.step('validate vvol capacity increase'):
            # refresh the storage info
            vm.RefreshStorageInfo()
            vvol2 = vm.get_datastore()
            vvol_free_space_after = vvol2.info.freeSpace
            expected = vvol_free_space_before + lv_total_space
            found = vvol_free_space_after
            self.assert_approximately_equal(expected, found, 'VVOL')

    @attr('poc_script', jira='PD-14753', name='test_poc_script_phase_5')
    def test_poc_script_phase_5(self):

        def getconn():
            ssh_conn = SshConn(address=addr, username='root', password='Tonian')
            econn = ExtendedConn(ssh_conn)
            return econn

        hypervisor = ctx.hypervisor

        with self.step('clone VM and boot'):
            vm, addr = self._clone_vm_on_vvol(hypervisor)
        with self.step('take snapshot'):
            vm.take_snapshot(self._SNAPSHOT_NAME, self._SNAPSHOT_DESC)
            snapshot = vm.get_snapshot(self._SNAPSHOT_NAME)
            assert snapshot, 'no snapshot created'
        with self.step('make a change to the VM'):
            randfile = '/tmp/' + utils.lower_generator(8)
            conn = getconn()
            assert not conn.exists(randfile), 'file already exists'
            cmd = ['echo', 'hello', '>', randfile]
            conn.execute(cmd)
            assert conn.exists(randfile), 'file does not exist'
        with self.step('revert to snapshot'):
            vm.revert_to_snapshot(self._SNAPSHOT_NAME)
        with self.step('verify that the vm has reverted'):
            conn = getconn()
            assert not conn.exists(randfile), 'file still exists'


    @attr('poc_script', jira='PD-15079', name='test_unregister_vasa')
    def test_poc_script_unregister_vasa(self):
        hypervisor = ctx.hypervisor
        with self.step('Destroy VMs on VVOL'):
            vvol = hypervisor._get_datastore_by_name(self._VVOL_NAME)
            self._logger.info('Got VVOL "{}"'.format(vvol.name))
            for vm in vvol.vm:
                self._logger.info('Deleting VM "{}"'.format(vm.name))
                hypervisor.destroy_vm(vm)

        with self.step('Remove VVOL'):
            self._logger.info('Removing VVOL {}'.format(vvol.name))
            hypervisor.remove_datastore(vvol)

        with self.step('Unregister VASA provider'):
            self._logger.info(
                'Unregistering VASA provider {}'.format(self._POC_VASA_NAME))
            hypervisor.unregister_vasa_provider(self._POC_VASA_NAME)
