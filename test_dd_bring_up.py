#!/usr/bin/env python
# -*- coding: UTF-8 -*-


from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pytest import yield_fixture
import os


MDFS_PATH = '/pd/dcns/mdfs'
PIPEFS_PATH= '/pd_mgmt/nfs/rpc_pipefs'
MDFS_TYPE = 'tfs2'
TCPPORTS = ['22', '875', '32803', '892', '662', '111', '2049', '20048', '8080', '8443', '7789', '2224']
UDPPORTS = ['875', '32769', '892', '662', '111', '2049', '20048', '5405', '5404']

@yield_fixture
def do_nothing():
    yield


class TestDDBringUp(TestBase):

    @attr(jira='PD-2872', name='dd_bring_up_verification', complexity=1)
    def test_001_dd_bring_up_verification(self, do_nothing):
        # Step 1 -Check if the pd-sys-agent daemon is running
        ctx.cluster.get_pid_by_proc_name('pd-sys-agent')
        self._logger.info('pd-sys-agent is up and running')

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

        # Step 3 - Check that the MDFS is mounted and with the correct FS type and correct mount options
        mount = ''
        mounts_res = ctx.cluster.execute(['cat','/proc/mounts'])
        for line in mounts_res.stdout.split('\n'):
            if line.strip() != '' and MDFS_PATH == line.split(' ')[1]:
                mount = line
                break
        assert mount != '', '%s is not mounted' % MDFS_PATH
        self._logger.info('MDFS is mounted')

        # Step 4 - Check that the MDFS has the correct filsystem type
        assert 'tfs2' == mount.split()[2], '%s has an incorrect filesystem type: %s' % (MDFS_PATH, mount.split()[2])
        self._logger.info('MDFS is tfs2')

        mount = ''
        mounts_res = ctx.cluster.execute(['cat','/proc/mounts'])
        for line in mounts_res.stdout.split('\n'):
            if line.strip() != '' and PIPEFS_PATH == line.split(' ')[1]:
                mount = line
                break
        assert mount != '', '%s is not mounted' % PIPEFS_PATH
        self._logger.info('PIPEFS is mounted')

        assert 'rpc_pipefs' == mount.split()[2], '%s has an incorrect filesystem type: %s' % (PIPEFS_PATH, mount.split()[2])
        self._logger.info('PIPEFS is rpc_pipefs')

#         # Step 5 - Check that the MDFS is mounted on a DRBD device
#         assert 'drbd' in mount.split()[0], '%s is not mounted on a drbd device' % MDFS_PATH
#         self._logger.info('MDFS is on a drbd device')

#         # Step 6 - Check that the DRBD device is mounted on a LV
#         disk_res = ctx.get_active_dd().execute(['grep', '\'disk.*\"\;$\'', '/etc/drbd.conf'])
#         lvol = disk_res.stdout.rstrip().split("\"")[1]
#         if lvol == '':
#             raise Exception("The drbd configuration is corrupted")
#         res = ctx.get_active_dd().execute(['lvdisplay', '-c', lvol])
#         self._logger.info('the drbd device is on an LV')

#         # Step 7 - Check that the LV is on the correct VG
#         vg = ''
#         for line in res.stdout.split('\n'):
#             if 'mdu_lv' in line:
#                 vg = line.split(':')[1]
#         assert vg == 'mdu_vg', 'the logical volume is placed on the wrong vg: %s' % vg

        # Step 8 - Check if the VG is built from the correct devices - relevant only for a physical host
        disks = []
        if not ctx.cluster.is_virtual():
            res = ctx.cluster.execute(['pvs', '--noheading', '--separator', ','])
            for line in res.stdout.split('\n'):
                vg = line.split(',')[1]
                if vg == 'mdu_vg':
                    disks.append(os.path.basename(line.split(',')[0].strip()))
            res = ctx.cluster.execute(['cat', '/proc/devices', '|', 'grep', 'mtip32xx'])
            major = ''
            for line in res.stdout.split('\n'):
                if 'mtip32xx' in line:
                    major = line.split()[0]
            partitions = []
            res = ctx.cluster.execute(['cat', '/proc/partitions', '|', 'grep', major])
            for line in res.stdout.split('\n'):
                if major == line.split()[0]:
                    partitions.append(line.split()[1].strip())
            for disk in disks:
                assert disk in partitions, 'the VG has a disk which is not a micron disk: %s' % disk
        self._logger.info('the VG consists of micron disks')

        # Step  - Check if tonpol module is loaded
        assert ctx.cluster.is_kernel_module_loaded('tonpol'), 'tonpol module is not loaded'
        self._logger.info("tonpol kernel module is loaded")

        # Step  - Check if micron module is installed
        res = ctx.cluster.execute(['modinfo', 'mtip32xx'])
        self._logger.info("micron (mtip32xx) kernel module is installed")

        # Step  - Check if micron module has the correct version
        micron_version = ''
        for line in res.stdout.split('\n'):
            if 'version:' in line:
                micron_version = line.split(':')[1].strip()
                break
        assert micron_version == '3.7.0', 'micron driver\'s version is incorrect: %s' % micron_version
        self._logger.info("micron kernel module version is correct")

        # Step  - Check if mellanox modules are installed
        ctx.cluster.execute(['modinfo', 'mlx4_core'])
        ctx.cluster.execute(['modinfo', 'mlx4_en'])
        self._logger.info("mellanox kernel modules are installed")

        # TODO: need to add mellanox drivers' versions test

        # Step  - Check if the dcns export exists
        res = ctx.cluster.execute(['exportfs', '|', 'grep', '/pd/dcns/exports'])
        self._logger.info("the main export is exported")

        # Check if idmap domain is configured
        res = ctx.cluster.execute(['cat', '/etc/idmapd.conf', '|', 'grep', 'Domain', '|', 'grep', '-v', '\#'])
        self._logger.info("the idmap domain is configured")

        # Check if iptables is enabled
        res = ctx.cluster.execute(['systemctl', 'status', 'firewalld'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'firewalld is not enabled'
                break
        self._logger.info("firewalld is enabled")

        # Check that the iptables service is running
        res = ctx.cluster.service.srv_exe('firewalld', 'status')
        self._logger.info("firewalld is running")

        # TODO: verify iptables required configuration
        # Check that only the required ports are open
    #res = ctx.get_active_dd().execute('iptables', '-L', 'INPUT', '-v', '-n')
        #tcpports = []
        #udpports = []
        #for line in res.stdout.split('\n'):
        #    if 'ACCEPT' in line:
        #        if 'tcp' in  line:
        #            port = line.strip().split()[10].split(':')[1]
        #            assert port in TCPPORTS, 'Found illegal TCP port %s' % port
        #            tcpports.append(port)
        #        elif 'upd' in line:
        #            port = line.strip().split()[10].split(':')[1]
        #            assert port in UDPPORTS, 'Found illegal UDP port %s' % port
        #            udpports.append(port)
        #assert set(tcpports) == set(TCPPORTS) and set(udpports) == set(UDPPORTS), 'Some ports are missing from the iptables configuration'
        #igmp = False
    #res = ctx.get_active_dd().execute('iptables', '-L', 'INPUT', '-v')
        #for line in res.stdout.split('\n'):
        #    if 'ACCEPT' in line and 'igmp' in line:
        #        igmp = True
        #assert igmp, 'igmp rule is missing from iptables configuration'
    #self._logger.info("iptables ports check passed")

        # Check that the NFS service is running
        res = ctx.cluster.service.srv_exe('nfs-server', 'status')
        self._logger.info("NFS server is running")

        # Check if pacemaker is enabled
        res = ctx.cluster.execute(['systemctl', 'status', 'pacemaker'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'pacemaker is not enabled'
                break
        self._logger.info("pacemaker is enabled")

        # Check that the pacemaker service is running
        res = ctx.cluster.service.srv_exe('pacemaker', 'status')
        self._logger.info("pacemaker is running")
        
        # Check if corosync is enabled
        res = ctx.cluster.execute(['systemctl', 'status', 'corosync'])
        for line in res.stdout.split('\n'):
            if 'Loaded' in  line:
                assert 'disabled' not in line, 'corosync is not enabled'
                break
        self._logger.info("corosync is enabled")

        # Check that the corosync service is running
        res = ctx.cluster.service.srv_exe('corosync', 'status')
        self._logger.info("corosync is running")

        # Check that the data mover service is running
        res = ctx.cluster.service.srv_exe('pd-data-mover', 'status')
        self._logger.info("Data mover is running")
        # TODO: 
