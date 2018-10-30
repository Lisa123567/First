#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pytest import yield_fixture
import os
import random
from time import sleep
#from tonat.product.objects.obs import DsState
from pd_tests import test_ha
from tonat.product.nodes.cluster import ClusterState
from tonat.errors import Fatal
import pd_common.utils as utils
from tonat.product.objects.mount import Mount, NfsVersion
#from tonat.product.objects.label import Label

PD_DS_FAIL_TIMEOUT = 1800


class TestDSFailure(TestBase):
        
    #return value of nocreate flag in tonpol per given DS
    def tonpol_nocreate(self,ds_id):
        return ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).execute(['cat','/sys/kernel/config/tonpol/obs/%s/nocreate' % (ds_id)]).stdout

    #return PD-DS object in FALED state
    def pick_and_fail_pd_ds(self):
        curr_obs_obj = TestDSFailure.pick_pd_ds(self)
        if curr_obs_obj.ds_share.server.is_virtual():
            curr_obs_obj.ds_share.server.execute(['echo','1','>','/sys/block/sdb/device/delete'])
        else:
            curr_obs_obj.ds_share.server.execute(['echo','1','>','/sys/block/rssdb/device/delete'])
        
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.FAILED, attempt=12, interval=5)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        return curr_obs_obj
    
    #return PD-DS object in ONLINE state, if name is specified the function will return DS with another name then 'name'
    #if label is specified the function will return DS with another label then 'label'
    #if ds_list_used specified function will return DS that not present in it 
    def pick_pd_ds(self,name=None,label=None,ds_list_used=['']):
        curr_obs_obj = None
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            #using _get_ds_state below because obs_obj.state.value seems to return cached value
            #if obs_obj.type.value == 'PD' and obs_obj.state.value == 'ONLINE':
            if obs_obj.type.value == 'PD' and ctx.cluster._get_ds_state(obs_obj).value == 'ONLINE' and obs_name != name and obs_obj.label != label and obs_obj not in ds_list_used:
                curr_obs_obj = obs_obj
                break
        assert curr_obs_obj != None, 'No suitable PD-DS found'
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        return curr_obs_obj
    
    def pick_two_pd_ds_with_same_label(self,name=None,label=None,ds_list_used=['']):
        first_obs_obj = None
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if obs_obj.type.value == 'PD' and ctx.cluster._get_ds_state(obs_obj).value == 'ONLINE' and obs_name != name and obs_obj.label != label and obs_obj not in ds_list_used:
                first_obs_obj = obs_obj
                break
        assert first_obs_obj != None, 'No suitable PD-DS found'
        assert TestDSFailure.tonpol_nocreate(self,first_obs_obj.id) == '0\n'
        #searching for second PD-DS with the same label as the first one
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if obs_obj.type.value == 'PD' and ctx.cluster._get_ds_state(obs_obj).value == 'ONLINE' and obs_name != first_obs_obj.name and obs_obj.label == first_obs_obj.label and obs_obj not in ds_list_used:
                second_obs_obj = obs_obj
                break
        assert second_obs_obj != None, 'No suitable PD-DS found'
        assert TestDSFailure.tonpol_nocreate(self,second_obs_obj.id) == '0\n'
        return first_obs_obj,second_obs_obj
    
    #return 3P object in ONLINE state
    def pick_3p_ds(self):
        curr_obs_obj = None
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if obs_obj.type.value == '3P' and ctx.cluster._get_ds_state(obs_obj).value == 'ONLINE':
                curr_obs_obj = obs_obj
                break
        assert curr_obs_obj != None, 'No suitable 3P-DS found'
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        return curr_obs_obj
    
    #return number of PD-DSs in the system
    def pd_ds_count(self):
        i=0
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if obs_obj.type.value == 'PD':
                i = i + 1               
        return i
    
    #return number of 3P-DSs in the system
    def ds_3p_count(self):
        i=0
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if obs_obj.type.value == '3P':
                i = i + 1               
        return i
    
    #check that all DSs in the system are ONLINE
    def check_ds_state_all(self):
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if ctx.cluster._get_ds_state(obs_obj).value != 'ONLINE':
                raise Fatal('Not all DSs in the ONLINE state')
       
    
    def setup_pd_ds(self,curr_obs_obj):
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_server.execute(['pcs','cluster','destroy'])
        curr_obs_server.execute(['rm','-rf','/etc/pd/init_completed'])
        curr_obs_server.execute(['pd-node-setup','--standalone','--configure-ds','--local-node-ip','%s' % (curr_obs_server.address),'--dd-cluster-ip','%s' % (ctx.cluster.pcs.get_cluster_ip()),'--force'])
        
    def call_mount(self, share_name, mount_point):
        ctx.clients[0].mkdirs(mount_point)
        share = ctx.cluster.shares[share_name]
        mnt_objs = Mount(share, NfsVersion.pnfs,
                         path=str(mount_point))
        mnt_cl = ctx.clients[0].nfs.mount(mnt_objs)
        return mnt_cl
    
    def call_umount(self, mnt_cl):
        ctx.clients[0].clean_dir(mnt_cl.path)
        ctx.clients[0].nfs.umount(mnt_cl)        
    
    def write_data(self,share_name,num_of_files=100):
        #mounting client
        mnt_point = '/mnt/' + utils.lower_generator(8)
        mnt = self.call_mount(share_name, mnt_point)
        i=0
        while i < num_of_files:
            file_name = mnt_point + '/' + '%s' % (utils.lower_generator(8)) + '%s' % (i)
            ctx.clients[0].execute(['dd','if=/dev/urandom','of=%s' % (file_name),'bs=1M','count=10','conv=fsync'])
            i = i + 1
        return mnt
    
    #fail PD-DS, delete it, reboot DD and re-add DS to the system
    @attr(jira='PD-4400', complexity=1, name='delete_pd_ds_and_reboot')
    def test_001_delete_pd_ds_and_reboot(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_and_fail_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_label = curr_obs_obj.label
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.hw.power_off()
        #rebooting DS after DS was deleted from the system
        ha = test_ha.TestHA()
        ha.test_002_dd_force_reboot_active()
        curr_obs_server.hw.power_on()
        curr_obs_server.wait_for(self,attempt=50,interval=10)
        TestDSFailure.setup_pd_ds(self, curr_obs_obj)
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        TestDSFailure.check_ds_state_all(self)
        
    #reboot DD with FAILED PD-DS
    @attr(jira='PD-4401', complexity=1, name='reboot_dd_failed_pd_ds1')
    def test_002_reboot_dd_failed_pd_ds1(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_and_fail_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_label = curr_obs_obj.label
        curr_obs_server = curr_obs_obj.ds_share.server
        #reboot DD with FAILED DS but DS itself is still reachable 
        ha = test_ha.TestHA()
        ha.test_002_dd_force_reboot_active()
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.hw.reset()
        curr_obs_server.wait_for(self,attempt=50,interval=10)
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        TestDSFailure.check_ds_state_all(self)
        
    #reboot DD with FAILED PD-DS (unreachable)
    @attr(jira='PD-4402', complexity=1, name='reboot_dd_failed_pd_ds2')
    def test_003_reboot_dd_failed_pd_ds2(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_and_fail_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_label = curr_obs_obj.label
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_server.hw.power_off()
        #reboot DD with FAILED DS but DS itself is unreachable 
        ha = test_ha.TestHA()
        ha.test_002_dd_force_reboot_active()
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        #deleting DS when it is unreachable
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.hw.power_on()
        curr_obs_server.wait_for(self,attempt=50,interval=10)
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        TestDSFailure.check_ds_state_all(self)
        
    #reboot DD with SUSPECTED PD-DS
    @attr(jira='PD-4403', complexity=1, name='reboot_dd_suspected_pd_ds')
    def test_004_reboot_dd_suspected_pd_ds(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #reboot DD when DS is unreachable TODO: check what happening with local mounts
        ha = test_ha.TestHA()
        ha.test_002_dd_force_reboot_active()
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        curr_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        TestDSFailure.check_ds_state_all(self)
        
    #fail PD-DS, delete it and re-add DS to the system
    @attr(jira='PD-4404', complexity=1, name='delete_pd_ds')
    def test_005_delete_pd_ds(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_and_fail_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_label = curr_obs_obj.label
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.hw.reset()
        curr_obs_server.wait_for(attempt=50,interval=10)
        TestDSFailure.setup_pd_ds(self, curr_obs_obj)
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        assert ctx.cluster._get_ds_state(curr_obs_obj).value == 'ONLINE'
        TestDSFailure.check_ds_state_all(self)
        

    #shutdown PD-DS and check that the state changing accordingly
    @attr(jira='PD-4405', complexity=1, name='pd_ds_shutdown')
    def test_006_pd_ds_shutdown(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        curr_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        TestDSFailure.check_ds_state_all(self)
        
    #shutdown PD-DS and check that it moving to FAILED state after timeout, delete DS when it is unreachable and re-add it 
    @attr(jira='PD-4406', complexity=1, name='pd_ds_shutdown_and_fail')
    def test_007_pd_ds_shutdown_and_fail(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_label = curr_obs_obj.label
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        sleep(PD_DS_FAIL_TIMEOUT)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.FAILED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.hw.power_on()
        curr_obs_server.wait_for(self,attempt=50,interval=10)
        TestDSFailure.setup_pd_ds(self, curr_obs_obj) ## TODO check if this really needed
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        assert ctx.cluster._get_ds_state(curr_obs_obj).value == 'ONLINE'
        TestDSFailure.check_ds_state_all(self)
        
    # fail DS via pdcli, delete it and re-add
    @attr(jira='PD-4407', complexity=1, name='pd_ds_fail_pdcli')
    def test_008_pd_ds_fail_pdcli(self):
        #at least one PD-DS should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_pd_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_label = curr_obs_obj.label
        curr_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.cli.ds_fail_by_obj(curr_obs_obj)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.FAILED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        curr_obs_server.hw.power_on()
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        curr_obs_server.wait_for(self,attempt=50,interval=10)
        TestDSFailure.setup_pd_ds(self, curr_obs_obj) ## TODO check if this really needed
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        assert ctx.cluster._get_ds_state(curr_obs_obj).value == 'ONLINE'
        TestDSFailure.check_ds_state_all(self)
        
        
    #checking that two suspected DSs are never moving to FAILED if they are entered to suspected state within 2 minutes - PD-4260
    @attr(jira='PD-4408', complexity=1, name='pd_ds_two_suspected')
    def test_009_pd_ds_two_suspected(self):
        TestDSFailure.check_ds_state_all(self)
        #at least two PD-DSs should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 2 
        first_obs_obj = TestDSFailure.pick_pd_ds(self)
        first_obs_server = first_obs_obj.ds_share.server
        first_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(first_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,first_obs_obj.id) == '1\n'
        second_obs_obj = TestDSFailure.pick_pd_ds(self)
        second_obs_server = second_obs_obj.ds_share.server
        second_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(second_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,second_obs_obj.id) == '1\n'
        sleep(PD_DS_FAIL_TIMEOUT)
        assert ctx.cluster._get_ds_state(first_obs_obj).value == 'SUSPECTED'
        assert ctx.cluster._get_ds_state(second_obs_obj).value == 'SUSPECTED'
        first_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(first_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        ##sleeping here to make sure that system manager timer restarted and second DS didn't move to FAILED immediately after first returned
        sleep(120)
        assert ctx.cluster._get_ds_state(second_obs_obj).value == 'SUSPECTED'
        second_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(second_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,first_obs_obj.id) == '0\n'
        assert TestDSFailure.tonpol_nocreate(self,second_obs_obj.id) == '0\n'
        TestDSFailure.check_ds_state_all(self)
        
    #checking that suspected PD-DS is never moving to FAILED after timeout if there is already failed DS in the system
    @attr(jira='PD-4409', complexity=1, name='pd_ds_failed_and_suspected')
    def test_010_pd_ds_failed_and_suspected(self):
        #at least two PD-DSs should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 2 
        TestDSFailure.check_ds_state_all(self)
        first_obs_obj = TestDSFailure.pick_and_fail_pd_ds(self)
        first_obs_server = first_obs_obj.ds_share.server
        first_obs_label = first_obs_obj.label
        first_obs_name = first_obs_obj.name
        second_obs_obj = TestDSFailure.pick_pd_ds(self)
        second_obs_server = second_obs_obj.ds_share.server
        second_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(second_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,second_obs_obj.id) == '1\n'
        sleep(PD_DS_FAIL_TIMEOUT)
        assert ctx.cluster._get_ds_state(second_obs_obj).value == 'SUSPECTED'
        second_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(second_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        first_obs_server.hw.power_off()
        ctx.cluster.cli.ds_delete(name=first_obs_name)
        first_obs_server.hw.power_on()
        first_obs_server.wait_for(self,attempt=50,interval=10)
        ##TestDSFailure.setup_pd_ds(self, first_obs_obj) //TODO check if that really needed
        ctx.cluster.cli.ds_add_by_obj(first_obs_obj,first_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,first_obs_obj.id) == '0\n'
        assert ctx.cluster._get_ds_state(first_obs_obj).value == 'ONLINE'
        TestDSFailure.check_ds_state_all(self)
        
    #checking that all suspected DSs in the systems are never moving to FAILED
    @attr(jira='PD-4424', complexity=1, name='pd_ds_all_suspected')
    def test_011_pd_ds_all_suspected(self):
        #at least two PD-DSs should be in the system
        assert TestDSFailure.pd_ds_count(self) >= 2 
        TestDSFailure.check_ds_state_all(self)
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            if ctx.cluster._get_ds_state(obs_obj).value != 'ONLINE':
                self._logger.info('One of the DSs not ONLINE. Terminating...') 
                raise Exception
            obs_server = obs_obj.ds_share.server
            ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).block_single_ip(obs_server.address)
            ctx.cluster.wait_for_ds_state(obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        sleep(PD_DS_FAIL_TIMEOUT)
        for obs_name, obs_obj in ctx.cluster.obses.iteritems():
            obs_server = obs_obj.ds_share.server
            ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).unblock_single_ip(obs_server.address)
            ctx.cluster.wait_for_ds_state(obs_obj, expected_state=DsState.ONLINE, attempt=24, interval=10)
        TestDSFailure.check_ds_state_all(self)
        
    #block in firewall 3P-DS on DD and check states
    @attr(jira='PD-4539', complexity=1, name='3p_ds_suspected_short')
    def test_012_3p_ds_suspected_short(self):
        #at least one 3P-DS should be in the system
        assert TestDSFailure.ds_3p_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_3p_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).block_single_ip(curr_obs_server.address)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        sleep(PD_DS_FAIL_TIMEOUT/3)
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).unblock_single_ip(curr_obs_server.address)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.ONLINE, attempt=24, interval=10)
        TestDSFailure.check_ds_state_all(self)
        
    #block in firewall 3P-DS on DD and check states - long test
    @attr(jira='PD-4540', complexity=1, name='3p_ds_suspected_long')
    def test_013_3p_ds_suspected_long(self):
        #at least one 3P-DS should be in the system
        assert TestDSFailure.ds_3p_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_3p_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).block_single_ip(curr_obs_server.address)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        sleep(PD_DS_FAIL_TIMEOUT)
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).unblock_single_ip(curr_obs_server.address)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.ONLINE, attempt=24, interval=10)
        TestDSFailure.check_ds_state_all(self)
        
    # fail 3P-DS via pdcli, delete it and re-add
    @attr(jira='PD-4541', complexity=1, name='3p_ds_fail_pdcli')
    def test_014_3p_ds_fail_pdcli(self):
        #at least one 3P-DS should be in the system
        assert TestDSFailure.ds_3p_count(self) >= 1 
        TestDSFailure.check_ds_state_all(self)
        curr_obs_obj = TestDSFailure.pick_3p_ds(self)
        curr_obs_name = curr_obs_obj.name
        curr_obs_server = curr_obs_obj.ds_share.server
        curr_obs_label = curr_obs_obj.label
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).block_single_ip(curr_obs_server.address)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10) 
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.cli.ds_fail_by_obj(curr_obs_obj)
        ctx.cluster.wait_for_ds_state(curr_obs_obj, expected_state=DsState.FAILED, attempt=12, interval=10)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '1\n'
        ctx.cluster.get_active_dd(expected_state=ClusterState.HEALTHY).unblock_single_ip(curr_obs_server.address)
        ctx.cluster.cli.ds_delete(name=curr_obs_name)
        ctx.cluster.cli.ds_add_by_obj(curr_obs_obj,curr_obs_label)
        assert TestDSFailure.tonpol_nocreate(self,curr_obs_obj.id) == '0\n'
        assert ctx.cluster._get_ds_state(curr_obs_obj).value == 'ONLINE'
        TestDSFailure.check_ds_state_all(self)
        
    #PD-DS (source) enters to SUSPECTED during share-rebalance (DS1->DS2, no CSM here), at least two PD-DSs are mandatory
    @attr(jira='PD-4572', complexity=1, name='susp_src_pd_ds_during_rebalance')
    def test_015_susp_source_pd_ds_during_rebalance(self):
        #at least two PD-DSs should be in the system + share
        assert TestDSFailure.pd_ds_count(self) >= 2 
        TestDSFailure.check_ds_state_all(self)
        ds_list_used = []
        src_obs_obj = TestDSFailure.pick_pd_ds(self)
        ds_list_used.append(src_obs_obj)
        src_obs_server = src_obs_obj.ds_share.server
        #we are looking for a DS with different label
        dst_obs_obj = TestDSFailure.pick_pd_ds(self,label=src_obs_obj.label,ds_list_used=ds_list_used)
        ds_list_used.append(dst_obs_obj)
        share = (ctx.cluster.cli.share_list())[0]
        src_dp = utils.lower_generator(8)
        dst_dp = utils.lower_generator(8)
        ctx.cluster.cli.data_profile_create(src_dp, [src_obs_obj.label.name])
        ctx.cluster.cli.data_profile_create(dst_dp, [dst_obs_obj.label.name])
        src_cls = utils.lower_generator(8)
        dst_cls = utils.lower_generator(8)
        ctx.cluster.cli.classifier_create(src_cls, ['Active,NOOP,%s,CREATE' % (src_dp)])
        ctx.cluster.cli.classifier_create(dst_cls, ['Active,NOOP,%s,CREATE' % (dst_dp)])
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=src_cls)
        mnt = TestDSFailure.write_data(self,share.name,50)
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=dst_cls)
        ctx.cluster.cli.share_rebalance(share.name)
        #let the rebalance start
        sleep(5)
        src_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(src_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #TODO change to random sleep less then 30 min
        sleep(60)
        src_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(src_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        #TODO replace sleep below when PD-4273 fixed (rebalance finished event)
        sleep(120)
        ctx.cluster.shares[share.name].verify_location()
        self.call_umount(mnt)
        
    #PD-DS (destination) enters to SUSPECTED during share-rebalance (DS1->DS2, no CSM here), at least two PD-DSs are mandatory
    @attr(jira='PD-4617', complexity=1, name='susp_dst_pd_ds_during_rebalance')
    def test_016_susp_dst_pd_ds_during_rebalance(self):
        #at least two PD-DSs should be in the system + share
        assert TestDSFailure.pd_ds_count(self) >= 2 
        TestDSFailure.check_ds_state_all(self)
        ds_list_used = []
        src_obs_obj = TestDSFailure.pick_pd_ds(self)
        ds_list_used.append(src_obs_obj)
        #we are looking for a DS with different label
        dst_obs_obj = TestDSFailure.pick_pd_ds(self,label=src_obs_obj.label,ds_list_used=ds_list_used)
        ds_list_used.append(dst_obs_obj)
        dst_obs_server = dst_obs_obj.ds_share.server
        share = (ctx.cluster.cli.share_list())[0]
        src_dp = utils.lower_generator(8)
        dst_dp = utils.lower_generator(8)
        ctx.cluster.cli.data_profile_create(src_dp, [src_obs_obj.label.name])
        ctx.cluster.cli.data_profile_create(dst_dp, [dst_obs_obj.label.name])
        src_cls = utils.lower_generator(8)
        dst_cls = utils.lower_generator(8)
        ctx.cluster.cli.classifier_create(src_cls, ['Active,NOOP,%s,CREATE' % (src_dp)])
        ctx.cluster.cli.classifier_create(dst_cls, ['Active,NOOP,%s,CREATE' % (dst_dp)])
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=src_cls)
        #writing 50 files
        mnt = TestDSFailure.write_data(self,share.name,50)
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=dst_cls)
        ctx.cluster.cli.share_rebalance(share.name)
        #let the rebalance start
        sleep(5)
        dst_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(dst_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #TODO change to random sleep less then 30 min
        sleep(60)
        dst_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(dst_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        #TODO replace sleep below when PD-4273 fixed (rebalance finished event)
        sleep(120)
        ctx.cluster.shares[share.name].verify_location()
        self.call_umount(mnt)
        
    #PD-DS (DS2) enters to SUSPECTED during share-rebalance (DS1->DS2,DS3), at least three PD-DSs are mandatory (two with same label)
    @attr(jira='PD-4618', complexity=1, name='susp_dst_pd_ds_csm_during_rebalance')
    def test_017_susp_dst_pd_ds_csm_during_rebalance(self):
        #at least three PD-DSs should be in the system + share
        assert TestDSFailure.pd_ds_count(self) >= 3 
        TestDSFailure.check_ds_state_all(self)
        ds_list_used = []
        #picking two PD-DSs with the same label
        dst1_obs_obj, dst2_obs_obj = TestDSFailure.pick_two_pd_ds_with_same_label(self)
        ds_list_used.append(dst1_obs_obj)
        ds_list_used.append(dst2_obs_obj)
        #we are looking for a DS with different label than two above
        src_obs_obj = TestDSFailure.pick_pd_ds(self,label=dst1_obs_obj.label,ds_list_used=ds_list_used)
        ds_list_used.append(src_obs_obj)
        dst1_obs_server = dst1_obs_obj.ds_share.server
        share = (ctx.cluster.cli.share_list())[0]
        src_dp = utils.lower_generator(8)
        dst_dp = utils.lower_generator(8)
        ctx.cluster.cli.data_profile_create(src_dp, [src_obs_obj.label.name])
        ctx.cluster.cli.data_profile_create(dst_dp, [dst1_obs_obj.label.name,dst2_obs_obj.label.name])
        src_cls = utils.lower_generator(8)
        dst_cls = utils.lower_generator(8)
        ctx.cluster.cli.classifier_create(src_cls, ['Active,NOOP,%s,CREATE' % (src_dp)])
        ctx.cluster.cli.classifier_create(dst_cls, ['Active,NOOP,%s,CREATE' % (dst_dp)])
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=src_cls)
        #writing 50 files
        mnt = TestDSFailure.write_data(self,share.name,50)
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=dst_cls)
        ctx.cluster.cli.share_rebalance(share.name)
        #let the rebalance start
        sleep(5)
        dst1_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(dst1_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #TODO change to random sleep less then 30 min
        sleep(60)
        dst1_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(dst1_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        #TODO replace sleep below when PD-4273 fixed (rebalance finished event)
        sleep(120)
        ctx.cluster.shares[share.name].verify_location()
        self.call_umount(mnt)
     
     
    #PD-DS (DS3) enters to SUSPECTED during share-rebalance (DS1->DS2,DS3), at least three PD-DSs are mandatory (two with same label)
    @attr(jira='PD-4683', complexity=1, name='susp_dst_pd_ds_csm_during_rebalance1')
    def test_018_susp_dst_pd_ds_csm_during_rebalance1(self):
        #at least three PD-DSs should be in the system + share
        assert TestDSFailure.pd_ds_count(self) >= 3 
        TestDSFailure.check_ds_state_all(self)
        ds_list_used = []
        #picking two PD-DSs with the same label
        dst1_obs_obj, dst2_obs_obj = TestDSFailure.pick_two_pd_ds_with_same_label(self)
        ds_list_used.append(dst1_obs_obj)
        ds_list_used.append(dst2_obs_obj)
        #we are looking for a DS with different label than two above
        src_obs_obj = TestDSFailure.pick_pd_ds(self,label=dst1_obs_obj.label,ds_list_used=ds_list_used)
        ds_list_used.append(src_obs_obj)
        dst2_obs_server = dst2_obs_obj.ds_share.server
        share = (ctx.cluster.cli.share_list())[0]
        src_dp = utils.lower_generator(8)
        dst_dp = utils.lower_generator(8)
        ctx.cluster.cli.data_profile_create(src_dp, [src_obs_obj.label.name])
        ctx.cluster.cli.data_profile_create(dst_dp, [dst1_obs_obj.label.name,dst2_obs_obj.label.name])
        src_cls = utils.lower_generator(8)
        dst_cls = utils.lower_generator(8)
        ctx.cluster.cli.classifier_create(src_cls, ['Active,NOOP,%s,CREATE' % (src_dp)])
        ctx.cluster.cli.classifier_create(dst_cls, ['Active,NOOP,%s,CREATE' % (dst_dp)])
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=src_cls)
        #writing 50 files
        mnt = TestDSFailure.write_data(self,share.name,50)
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=dst_cls)
        ctx.cluster.cli.share_rebalance(share.name)
        #let the rebalance start
        sleep(5)
        dst2_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(dst2_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #TODO change to random sleep less then 30 min
        sleep(60)
        dst2_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(dst2_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        #TODO replace sleep below when PD-4273 fixed (rebalance finished event)
        sleep(120)
        ctx.cluster.shares[share.name].verify_location()
        self.call_umount(mnt)
        
    #PD-DS (DS1) enters to SUSPECTED during share-rebalance (DS1->DS2,DS3), at least three PD-DSs are mandatory (two with same label)
    @attr(jira='PD-4684', complexity=1, name='susp_src_pd_ds_csm_during_rebalance1')
    def test_019_susp_src_pd_ds_csm_during_rebalance1(self):
        #at least three PD-DSs should be in the system + share
        assert TestDSFailure.pd_ds_count(self) >= 3 
        TestDSFailure.check_ds_state_all(self)
        ds_list_used = []
        #picking two PD-DSs with the same label
        dst1_obs_obj, dst2_obs_obj = TestDSFailure.pick_two_pd_ds_with_same_label(self)
        ds_list_used.append(dst1_obs_obj)
        ds_list_used.append(dst2_obs_obj)
        #we are looking for a DS with different label than two above
        src_obs_obj = TestDSFailure.pick_pd_ds(self,label=dst1_obs_obj.label,ds_list_used=ds_list_used)
        ds_list_used.append(src_obs_obj)
        src_obs_server = src_obs_obj.ds_share.server
        share = (ctx.cluster.cli.share_list())[0]
        src_dp = utils.lower_generator(8)
        dst_dp = utils.lower_generator(8)
        ctx.cluster.cli.data_profile_create(src_dp, [src_obs_obj.label.name])
        ctx.cluster.cli.data_profile_create(dst_dp, [dst1_obs_obj.label.name,dst2_obs_obj.label.name])
        src_cls = utils.lower_generator(8)
        dst_cls = utils.lower_generator(8)
        ctx.cluster.cli.classifier_create(src_cls, ['Active,NOOP,%s,CREATE' % (src_dp)])
        ctx.cluster.cli.classifier_create(dst_cls, ['Active,NOOP,%s,CREATE' % (dst_dp)])
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=src_cls)
        #writing 50 files
        mnt = TestDSFailure.write_data(self,share.name,50)
        ctx.cluster.cli.share_classifier_set('/', name=share.name, classifier_name=dst_cls)
        ctx.cluster.cli.share_rebalance(share.name)
        #let the rebalance start
        sleep(5)
        src_obs_server.hw.power_off()
        ctx.cluster.wait_for_ds_state(src_obs_obj, expected_state=DsState.SUSPECTED, attempt=12, interval=10)
        #TODO change to random sleep less then 30 min
        sleep(60)
        src_obs_server.hw.power_on()
        ctx.cluster.wait_for_ds_state(src_obs_obj, expected_state=DsState.ONLINE, attempt=50, interval=10)
        #TODO replace sleep below when PD-4273 fixed (rebalance finished event)
        sleep(120)
        ctx.cluster.shares[share.name].verify_location()
        self.call_umount(mnt)
        

###TODO: automate tests for share-rebalance when rebalance is started when DS is already in SUSP/FAILED + scenario described in PD-4713 (with ds-fail)
