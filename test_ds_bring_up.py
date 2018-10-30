#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pytest import yield_fixture
import os


FS_PATH = '/pd_ds0'
FS_TYPE = 'ext4'
TCPPORTS = ['22', '875', '32803', '892', '662', '111', '2049', '20048', '8080', '8443', '7789', '2224']
UDPPORTS = ['875', '32769', '892', '662', '111', '2049', '20048', '5405', '5404']

@yield_fixture
def do_nothing():
    yield


class TestDSBringUp(TestBase):

    @attr(jira='PD-3809', complexity=1, name='ds_bring_up_verification')
    def test_001_ds_bring_up_verification(self, do_nothing):
        # Step 1 -Check if the pd-sys-agent daemon is running
        #res = ctx.data_stores[0].get_pid_by_proc_name('pd-sys-agent')
        #self._logger.info('pd-sys-agent is up and running')

        # Step 2 - Get all relevant log messages from the journal and check for errors / start / stop messages
        #messages = ctx.get_active_dd().execute(['journalctl','-b', '-p', '0..5','_COMM=pd-ctl', '_COMM=pd-sys-agent'])
        #self._logger.info(messages)
        #success = True
        #for line in messages[0].stdout.split('\n'):
        #    message = line.split(' ')[1]
        #    if 'pd' in message:
        #        success = False
        #        break
        #assert success, 'pd-sys-agent/pd-pctl error messages were found in the journal: %s' % message
        #self._logger.info('no pd-sys-agent/pd-pctl error messages were found')

        # Check that the FS is mounted and with the correct FS type and correct mount options
        mount = ''
        mounts_res = ctx.data_stores[0].execute(['cat','/proc/mounts'])
        for line in mounts_res.stdout.split('\n'):
            if line.strip() != '' and FS_PATH == line.split(' ')[1]:
                mount = line
                break
        assert mount != '', '%s is not mounted' % FS_PATH
        self._logger.info('FS is mounted')

        # Check that the MDFS has the correct filsystem type
        assert 'ext4' == mount.split()[2], '%s has an incorrect FS type: %s' % (FS_PATH, mount.split()[2])
        self._logger.info('FS is ext4')

        # Check that the FS is mounted on a LV device
        assert '/dev/mapper/vg_ds0-lv_ds0' in mount.split()[0], '%s is not mounted on a correct LV device' % FS_PATH
        self._logger.info('FS is on a LV device')  
        
        # Check that the LV is on the correct VG
        res = ctx.data_stores[0].execute(['lvdisplay', '-c', '/dev/vg_ds0/lv_ds0'])
        vg = ''
        for line in res.stdout.split('\n'):
            if 'lv_ds0' in line:
                vg = line.split(':')[1]
        assert vg == 'vg_ds0', 'the logical volume is placed on the wrong vg: %s' % vg

        #  Check if the VG is built from the correct devices - relevant only for a physical host
        disks = []
        if not ctx.data_stores[0].is_virtual():
            res = ctx.data_stores[0].execute(['pvs', '--noheading', '--separator', ','])
            for line in res.stdout.split('\n'):
                vg = line.split(',')[1]
                if vg == 'vg_ds0':
                    disks.append(os.path.basename(line.split(',')[0].strip()))
            res = ctx.data_stores[0].execute(['cat', '/proc/devices', '|', 'grep', 'mtip32xx'])
            major = ''
            for line in res.stdout.split('\n'):
                if 'mtip32xx' in line:
                    major = line.split()[0]
            partitions = []
            res = ctx.data_stores[0].execute(['cat', '/proc/partitions', '|', 'grep', major])
            for line in res.stdout.split('\n'):
                if major == line.split()[0]:
                    partitions.append(line.split()[1].strip())
            for disk in disks:
                assert disk in partitions, 'the VG has a disk which is not a micron disk: %s' % disk
            self._logger.info('The VG consists of Micron disks')

        # Check if Micron module is installed
        res = ctx.data_stores[0].execute(['modinfo', 'mtip32xx'])
        self._logger.info("Micron (mtip32xx) kernel module is installed")

        # Check if Micron module has the correct version
        micron_version = ''
        for line in res.stdout.split('\n'):
            if 'version:' in line:
                micron_version = line.split(':')[1].strip()
                break
        assert micron_version == '3.9.0', 'Micron driver\'s version is incorrect: %s' % micron_version
        self._logger.info("Micron kernel module version is correct")

        # Check if mellanox modules are installed
        res = ctx.data_stores[0].execute(['modinfo', 'mlx4_en'])
        self._logger.info("Mellanox kernel modules are installed")
        mlx_version = ''
        for line in res.stdout.split('\n'):
            if 'version:' in line:
                mlx_version = line.split(':')[1].strip().split(' ')[0]
                break
        assert mlx_version == '2.2-1.0.1', 'Mellanox driver\'s version is incorrect: %s' % mlx_version
        self._logger.info("Mellanox kernel module version is correct")

        # Check if the FS export exists
        res = ctx.data_stores[0].execute(['exportfs', '|', 'grep', '/pd_ds0'])
        self._logger.info("the main export is exported")

        # Check if firewalld is enabled
        res = ctx.data_stores[0].execute(['systemctl', 'status', 'firewalld'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'firewalld is not enabled'
                break
        self._logger.info("firewalld is enabled")

        # Check that the firewalld service is running
        res = ctx.data_stores[0].service.srv_exe('firewalld', 'status')
        self._logger.info("firewalld is running")

        # Check selinux
        res = ctx.data_stores[0].execute(['sestatus'])
        for line in res.stdout.split('\n'):
            if 'SELinux:' in line:
                assert 'disabled' in line, 'selinux is enabled'
                break
        
        # Check that the data mover service is running
        res = ctx.data_stores[0].service.srv_exe('nfs', 'status')
        self._logger.info("NFS server is running")
     
        # Check that the NFS service is running
        res = ctx.data_stores[0].service.srv_exe('pd-data-mover', 'status')
        self._logger.info("Data mover is running")

        # Check if pacemaker is enabled
        res = ctx.data_stores[0].execute(['systemctl', 'status', 'pacemaker'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'pacemaker is not enabled'
                break
        self._logger.info("pacemaker is enabled")

        # Check that the pacemaker service is running
        res = ctx.data_stores[0].service.srv_exe('pacemaker', 'status')
        self._logger.info("pacemaker is running")
        
        # Check if corosync is enabled
        res = ctx.data_stores[0].execute(['systemctl', 'status', 'corosync'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'corosync is not enabled'
                break
        self._logger.info("corosync is enabled")

        # Check that the corosync service is running
        res = ctx.data_stores[0].service.srv_exe('corosync', 'status')
        self._logger.info("corosync is running")
        
        # Check that the system agent service is running
        res = ctx.data_stores[0].service.srv_exe('pd-sys-agent', 'status')
        self._logger.info("pd-sys-agent is running")
