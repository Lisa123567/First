from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.product.objects.mount import Mount, NfsVersion
from tonat.context import ctx
from os.path import join
from pd_tests import test_ha
from tonat.os_ctl.os_linux import OsType
import pprint
import uuid
import random
import os
import crypt
import time
from tonat.greenlet_management import GreenletEx
#from tonat.product.objects.classifier.rule import Flags
from tonat.errors import (Fatal, Error, Failure, TimeoutException, CliOperationFailed)
from _pytest.python import yield_fixture
from pd_common.pcap_util import PCAPUtil
from tonat.dispatcher_tuple import dispatcher_tuple
from pd_common.user_utils import UserGroupGen
from pd_common.pcap_engine import PcapEngine as PE, PE_NfsOpType

from gevent import GreenletExit
from tonat.greenlet_management import blockability


class Test_Clients(TestBase):

    @yield_fixture
    def conf_3u_2g(self, get_pd_share):
        self.mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        print 'mnt_tmpl', self.mnt_tmpl
        self.mounts = ctx.clients.nfs.mount(self.mnt_tmpl)
        self.udb = UserGroupGen(ctx.clients)
        self.udb.generate(3,2)
        self.groups = self.udb.get_groups()
        yield
        self.udb.clean()

    @yield_fixture
    def conf_4u_3g(self, get_pd_share):
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        udb = UserGroupGen(ctx.clients)
        udb.generate(4,3)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        yield
        self.udb.clean()

    @yield_fixture
    def conf_2u_1g(self, get_pd_share):
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        udb = UserGroupGen(ctx.clients)
        udb.generate(2,1)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        yield
        self.udb.clean()

    @attr('client_base', jira='PD-5353', name='pNFS_behaviour_one_client')
    def test_pNFS_behaviour_one_client(self, get_pd_share):
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mount = ctx.clients[0].nfs.mount(mnt_tmpl)
        fname = ctx.clients[0].address
        nfsstat_file = join("/tmp", "nfsstat.out")
        path = mount.path + '/test_pNFS_behaviour_01'
        ctx.clients[0].mkdirs(path)

        ctx.clients[0].execute(["nfsstat", "-Z", "1", "-l", ">", nfsstat_file, "&", "KILLPID=$!", "&&", "echo", "verify-pnfs-one", ">", join(path, fname),\
                         "&&", "sync", "&&", "kill", "-2", "$KILLPID" ])
        res = ctx.clients[0].execute(['cat', nfsstat_file])

        nfsinfo = dict()
        for line in res.stdout.splitlines():
            if not line.startswith('nfs'):
                continue
            _, version, _, opt, count = line.split()
            try:
                    nfsinfo[version].update([(opt.strip()[:-1], int(count.strip()))])
            except KeyError:
                    nfsinfo[version] = {opt.strip()[:-1]: int(count.strip())}

        assert nfsinfo['v3']['write'] > 0, 'Test Failed - inband IO {}'.format(nfsinfo)

    @attr('client_base', jira='PD-5354', name='pNFS_behaviour_all_clients')
    def test_pNFS_behaviour_all_clients(self, get_pd_share):

        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = "verify-pnfs-" + client.address

            nfsstat_file = join("/tmp", "nfsstat.out")
            client.execute(["nfsstat", "-Z", "1", "-l", ">", nfsstat_file, "&", "KILLPID=$!", "&&", "echo", "verify-pnfs-all", ">", join(path, fname),\
                         "&&", "sync", "&&", "kill", "-2", "$KILLPID" ])
            res = client.execute(['cat', nfsstat_file])

            nfsinfo = dict()
            for line in res.stdout.splitlines():
                if not line.startswith('nfs'):
                    continue
                _, version, _, opt, count = line.split()

                try:
                    nfsinfo[version].update([(opt.strip()[:-1], int(count.strip()))])
                except KeyError:
                    nfsinfo[version] = {opt.strip()[:-1]: int(count.strip())}

            assert nfsinfo['v3']['write'] > 0, 'Test Failed - inband IO {}'.format(nfsinfo)

    @attr('client_base', jira='PD-5355', name='write_from_one_client_ls_from_others')
    def test_write_from_one_client_ls_from_others(self, get_pd_share):
        num_of_files = 1000
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

        ctx.clients[0].mkdirs(mounts[0].path + '/test_write_from_one_client_ls_from_others')
        for i in xrange(num_of_files):
          ctx.clients[0].execute(['dd', 'if=/dev/zero', 'of=%s/test_write_from_one_client_ls_from_others/file_%s' % (mounts[0].path, i),
                                  'bs=24k', 'count=1', 'conv=fsync', 'conv=notrunc'])

        for (client, mount) in zip(ctx.clients, mounts):
            ret = client.listdir(mount.path + '/test_write_from_one_client_ls_from_others')
            assert len(ret) == num_of_files, 'client {0} incomplete files under dir test_write_from_one_client_ls_from_others'.format(client.address)

    @attr('client_base', jira='PD-5356', name='write_from_all_clients_to_same_file')
    def test_write_from_all_clients_to_same_file(self, get_pd_share):
        num_of_files = 1000
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

        ctx.clients.mkdirs(mounts.path + '/test_smoke')
        for i in xrange(num_of_files):
          ctx.clients.execute(["nfsstat", "-Z", "1", "-l", ">", nfsstat_file, "&", "KILLPID=$!", "&&",'dd', 'if=/dev/zero', 'of=%s/test_smoke/smoke_%s' % (mounts[0].path, i),
                                  'bs=24k', 'count=1', 'conv=fsync', 'conv=notrunc',"&&", "sync", "&&", "kill", "-2", "$KILLPID"])
        res = client.execute(['cat', nfsstat_file])
        nfsinfo = dict()
        for line in res.stdout.splitlines():
                if not line.startswith('nfs'):
                    continue
                _, version, _, opt, count = line.split()

        try:
            nfsinfo[version].update([(opt.strip()[:-1], int(count.strip()))])
        except KeyError:
            nfsinfo[version] = {opt.strip()[:-1]: int(count.strip())}

        try:
            if nfsinfo['v3']['write'] < 1:
                    raise KeyError
        except KeyError:
                raise Exception('Test Failed - inband IO {}'.format(nfsinfo))

    @attr('client_base', jira='PD-5357', name='umount_client_in_the_w_r_time')
    def test_umount_client_in_the_w_r_time(self, get_pd_share):
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        fnames = []

        ctx.clients[0].mkdirs(mounts[0].path + '/test_umount')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_umount', "fileA-" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        fds = ctx.clients.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        fvs = ctx.clients.write_file(fds, 0, 32, 2, block=False)
        time.sleep(1) # Let the writes start

        for client, mount in zip(ctx.clients, mounts):
            try:
                self._logger.debug("client: {0} mount: {1}".format(client, mount))
                res = client.nfs.umount(mount)
                raise Exception("umount succeeded while it was expected to fail")
            except CliOperationFailed as x:
                #print x
                assert x.rc == 16, "Unexpected umount behavior"
#                 if x.rc == 16: # Device busy
#                     pass
#                 else:
#                     raise Fatal

        for fv in fvs:
            fv.get()

        ctx.clients.close_file(fds)
        ctx.clients.close_agent()


    @attr('client_base', jira='PD-5358', name='rpm_uninstall_install')
    def test_rpm_uninstall_install(self, get_pd_share):
   # test that all clients still pnfs- mode
       mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
       mounts = ctx.clients.nfs.mount(mnt_tmpl)

       fedora = []
       sles = []
       rhel = []
       fedora_mounts = []
       sles_mounts = []
       rhel_mounts = []
       rhel_paths = []
       sles_paths = []
       for client, mount in zip(ctx.clients, mounts):
            if client.os.type is OsType.Fedora:
                fedora.append(client)
                fedora_mounts.append(mount)
            if client.os.type is OsType.Sles:
                sles.append(client)
                sles_mounts.append(mount)
                sles_paths.append(mount.path)

            if client.os.type is OsType.Rhel:
                rhel.append(client)
                rhel_mounts.append(mount)
                rhel_paths.append(mount.path)

       fedora = dispatcher_tuple(fedora)

       rhel = dispatcher_tuple(rhel)
       rhel_mounts = dispatcher_tuple(rhel_mounts)
       rhel_rpm = rhel.execute(['rpm', '-qa', '|', 'grep', 'pd-nfs']).stdout

       rhel.nfs.umount(rhel_mounts)
       rhel.execute(['rpm', '-e', '%s' % (rhel_rpm)])
       print "rhel-rpm  is :", rhel_rpm

       rhel.close_agent()
       self._logger.info("Rebooting RHEL clients")
       rhel.reboot()
       self._logger.info("Waiting for RHEL clients to start")
       time.sleep(5)
       rhel.wait_for(attempt=10, interval=10)
       time.sleep(60)

       fnames = []
       rhel.nfs.mount(rhel_mounts)
       for (rhel_client, path) in zip(rhel, rhel_paths):
             print "rhel_mounts.path  is :", path
             fnames.append(join(path, 'rhel-'+rhel_client.address))

       time.sleep(2)
       fnames = dispatcher_tuple(fnames)
       pcaps = PE(rhel)

       fds = rhel.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
       rhel.write_file(fds, 0, 32, 5)
       rhel.read_file(fds, 0, 32, 5)
       rhel.close_file(fds)
       time.sleep(1) # DELME

       results = pcaps.get_nfs_ops(False)
       calls_only = pcaps.get_nfs_ops_by_and_filter(results, is_call=True)
       writes_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.WRITE, is_call=True)
       read_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.READ, is_call=True)
       assert len(writes_only) > 0, "No writes detected during the recorded period"
       assert len(read_only) > 0, "No read detected during the recorded period"

       for (rhel_client, single_client_nfs_ops) in zip(rhel, calls_only):
         self._logger.debug("RHEL CLIENT {}".format(rhel_client.address))
         for nfs_op in single_client_nfs_ops:
                 if nfs_op.op_type == PE_NfsOpType.WRITE or nfs_op.op_type == PE_NfsOpType.READ:
                     self._logger.info('  NFS OP %s at %s' % (nfs_op.op_type.name, nfs_op.time_relative))
                     self._logger.info('  OP DETAILS:\n%s' % pprint.pformat(nfs_op.__dict__))
                     assert nfs_op.ip_dst == ctx.cluster.address, "I/O that doesn't go to the MDS found"
       print "rhel is ok"

 # sles tests
       sles = dispatcher_tuple(sles)
       sles_mounts = dispatcher_tuple(sles_mounts)
       sles_rpm = sles.execute(['rpm', '-qa', '|', 'grep', 'pd-nfs']).stdout
       sles.nfs.umount(sles_mounts)
       sles.execute(['rpm', '-e', '%s' % (sles_rpm)])
       print "sles-rpm  is :", sles_rpm
       sles.close_agent()
       self._logger.info("Rebooting RHEL clients")
       sles.reboot()
       sles.wait_for(attempt=10, interval=10)
       time.sleep(60)
#       sles.execute(['rmmod', 'nfs_layout_flexfiles',])
       fnames = []
       sles.nfs.mount(sles_mounts)
       for (sles_client, path) in zip(sles, sles_paths):
             print "sles_mounts.path  is :", path
             fnames.append(join(path, 'sles-'+sles_client.address))

       time.sleep(2)
       fnames = dispatcher_tuple(fnames)
       pcaps = PE(sles)

       fds = sles.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
       sles.write_file(fds, 0, 32, 5)
       sles.read_file(fds, 0, 32, 5)
       sles.close_file(fds)
       time.sleep(1) # DELME

       results = pcaps.get_nfs_ops(False)
       calls_only = pcaps.get_nfs_ops_by_and_filter(results, is_call=True)
       writes_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.WRITE, is_call=True)
       read_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.READ, is_call=True)
       assert len(writes_only) > 0, "No writes detected during the recorded period"
       assert len(read_only) > 0, "No read detected during the recorded period"

       for (sles_client, single_client_nfs_ops) in zip(sles, calls_only):
            self._logger.debug("RHEL CLIENT {}".format(sles_client.address))
            for nfs_op in single_client_nfs_ops:
                if nfs_op.op_type == PE_NfsOpType.WRITE or nfs_op.op_type == PE_NfsOpType.READ:
                    self._logger.info('  NFS OP %s at %s' % (nfs_op.op_type.name, nfs_op.time_relative))
                    self._logger.info('  OP DETAILS:\n%s' % pprint.pformat(nfs_op.__dict__))
                    assert nfs_op.ip_dst == ctx.cluster.address, "I/O that doesn't go to the MDS found"

       print "sles is ok"

       c=fedora.execute(['uname', '-r']).stdout
       print c

 # rpm_install_back(self, get_pd_share):
       rhel.execute(['rpm', '-ivh', '/opt/' + rhel_rpm[0].rstrip() + '.rpm'])
        # Add reboot
       rhel.close_agent()
       self._logger.info("Rebooting RHEL clients")
       rhel.reboot()
       self._logger.info("Waiting for RHEL clients to start")
       time.sleep(5)
       rhel.wait_for(attempt=10, interval=10)
       time.sleep(60)
       self._logger.info("Mounting RHEL clients")
       rhel.nfs.mount(rhel_mounts)

       self.rhel_pnfs_test(rhel, rhel_mounts, rhel_client, rhel_paths, require_inband=False)
       print "END rhel_pnfs_test"

  # rpm_install_back(self, get_pd_share):
       print "Sles begining"
       sles.execute(['rpm', '-ivh', '/opt/' + sles_rpm[0].rstrip() + '.x86_64.rpm'])
       print "Sles"
        # Add reboot
       sles.close_agent()
       self._logger.info("Rebooting Sles clients")
       sles.reboot()
       self._logger.info("Waiting for Sles clients to start")
       sles.wait_for(attempt=10, interval=10)
       time.sleep(60)
       self._logger.info("Mounting Sles clients")
       sles.nfs.mount(sles_mounts)
       print "START sles_pnfs_test"
       self.sles_pnfs_test(sles, sles_mounts, sles_client, sles_paths, require_inband=False)
       print "END sles_pnfs_test"

    def rhel_pnfs_test(self, rhel, rhel_mounts, rhel_client, rhel_paths, require_inband):
       fnames = []
       for (rhel_client, path) in zip(rhel, rhel_paths):
             print "rhel_mounts.path  is :", path
             fnames.append(join(path, 'rhel-'+rhel_client.address))
       fnames = dispatcher_tuple(fnames)
       self._logger.debug("Starting PCAPs")
       pcaps = PE(rhel)

       self._logger.info("Opening files")
       fds = rhel.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
       self._logger.info("Writing to files")
       rhel.write_file(fds, 0, 32, 5)
       self._logger.info("Reading from files")
       rhel.read_file(fds, 0, 32, 5)
       self._logger.info("Closing files")
       rhel.close_file(fds)

       time.sleep(1) # DELME
       results = pcaps.get_nfs_ops(False)
       calls_only = pcaps.get_nfs_ops_by_and_filter(results, is_call=True)
       writes_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.WRITE, is_call=True)
       read_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.READ, is_call=True)
       assert len(writes_only) > 0, "No writes detected during the recorded period"
       assert len(read_only) > 0, "No read detected during the recorded period"

       for (rhel_client, single_client_nfs_ops) in zip(rhel, calls_only):
         self._logger.debug("RHEL CLIENT {}".format(rhel_client.address))
         for nfs_op in single_client_nfs_ops:
                 if nfs_op.op_type == PE_NfsOpType.WRITE or nfs_op.op_type == PE_NfsOpType.READ:
                     self._logger.info('  NFS OP %s at %s' % (nfs_op.op_type.name, nfs_op.time_relative))
                     self._logger.info('  OP DETAILS:\n%s' % pprint.pformat(nfs_op.__dict__))
                     if require_inband:
                         assert nfs_op.ip_dst == ctx.cluster.address, "I/O that doesn't go to the MDS found"
                     else:
                         assert nfs_op.ip_dst != ctx.cluster.address, "I/O that goes to the MDS found"


    def sles_pnfs_test(self, sles, sles_mounts, sles_client, sles_paths, require_inband):
       fnames = []
       for (sles_client, path) in zip(sles, sles_paths):
             print "sles_mounts.path  is :", path
             fnames.append(join(path, 'sles-'+sles_client.address))

       fnames = dispatcher_tuple(fnames)
       pcaps = PE(sles)
       fds = sles.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
       sles.write_file(fds, 0, 32, 5)
       sles.read_file(fds, 0, 32, 5)
       sles.close_file(fds)
       time.sleep(1) # DELME
       results = pcaps.get_nfs_ops(False)
       calls_only = pcaps.get_nfs_ops_by_and_filter(results, is_call=True)
       writes_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.WRITE, is_call=True)
       read_only = pcaps.get_nfs_ops_by_and_filter(results, op_type=PE_NfsOpType.READ, is_call=True)
       assert len(writes_only) > 0, "No writes detected during the recorded period"
       assert len(read_only) > 0, "No read detected during the recorded period"
       for (sles_client, single_client_nfs_ops) in zip(sles, calls_only):
            self._logger.debug("RHEL CLIENT {}".format(sles_client.address))
            for nfs_op in single_client_nfs_ops:
                if nfs_op.op_type == PE_NfsOpType.WRITE or nfs_op.op_type == PE_NfsOpType.READ:
                    self._logger.info('  NFS OP %s at %s' % (nfs_op.op_type.name, nfs_op.time_relative))
                    self._logger.info('  OP DETAILS:\n%s' % pprint.pformat(nfs_op.__dict__))
                    if require_inband:
                        assert nfs_op.ip_dst == ctx.cluster.address, "I/O that doesn't go to the MDS found"
                    else:
                        assert nfs_op.ip_dst != ctx.cluster.address, "I/O that goes to the MDS found"



    @attr('client_base', jira='PD-5359', name='group_user_and_root')
    def test_group_user_and_root(self, get_pd_share):
        self.conf_3u_2g(get_pd_share)

        fd0 = ctx.clients[0].open_file(fnames[0], os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        fds = ctx.clients[1:].open_file(fnames[1:], os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        fds = dispatcher_tuple((fd0,) + fds)
        self._logger.info("BEFORE WRITE")
        ctx.clients.write_file(fds, 0, 32, 2)
        self._logger.info("BEFORE CLOSE")
        ctx.clients.read_file(fds, 0, 32, 2)
        ctx.clients.close_file(fds)
        self._logger.info("AFTER CLOSE")
        ctx.clients.close_agent()
        ctx.clients[0].execute(['exit'], block=True)
        ctx.clients[0].execute(['userdel', 'client0'], block=True)
        ctx.clients[0].execute(['groupdel', 'smoke'], block=True)

    def get_test_file(self,mount, dir, file):
        return mount.path + dir+ '/'+ file

    @attr('client_base', jira='PD-5360', name='create_open_write_from_first_user_read_from_secound_root')
    def test_create_open_write_from_first_user_read_from_secound_root(self, get_pd_share):

        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

# Create users on the clients
        udb = UserGroupGen(ctx.clients)
        udb.generate(3,2)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        directory = 'group_1'
        path = "{0}{1}{2}".format(mounts[0].path,"/",directory)

        print "kyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"

        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0666)
        fname = path + '/write_from_first_user_read_from_secound'
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        dir = '/group_1'
        ctx.clients[0].mkdirs(dir)
        ctx.clients[0].execute(['chown', '-h', '-R', '--from=' + users[0],':',groups[0], format(my_user), directory])
        fname0 = self.get_test_file(mounts[0], dir, fname)
        fname1 = self.get_test_file(mounts[1], dir, fname)

        self._logger.info("BEFORE OPEN")

        fd0 = ctx.clients[0].open_file(fname0, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644, 'client0')
        time.sleep(2)
        fd1 = ctx.clients[1].open_file(fname1, os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        #fds = dispatcher_tuple((fd0,) + fds)
        self._logger.info("BEFORE WRITE")
        ctx.clients[0].write_file(fd0, 0, 32, 2)
#         self._logger.info("BEFORE CLOSE")
        ctx.clients[1].read_file(fd1, 0, 32, 2)
        ctx.clients[0].close_file(fd0)
        ctx.clients[1].close_file(fd1)
        self._logger.info("AFTER CLOSE")
        ctx.clients.close_agent()

    @attr('client_base', jira='PD-5361', name='write_from_one_list_dir_from_others')
    def test_write_from_one_list_dir_from_others(self, get_pd_share):
        num_of_files = 1000
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

        ctx.clients[0].mkdirs(mounts[0].path + '/test_02')
        for i in xrange(num_of_files):
            ctx.clients[0].execute(['dd', 'if=/dev/zero', 'of=%s/test_02/file_%s' % (mounts[0].path, i),
                                    'bs=24k', 'count=1', 'conv=fsync', 'conv=notrunc'])
        for (client, mount) in zip(ctx.clients, mounts):
            ret = client.listdir(mount.path + '/test_02')
            assert len(ret) != num_of_files, 'client {0} incomplete files under dir test_02'.format(client.address)

    @attr('client_base', jira='PD-5363', name='user_write_to_specific_group')
    def test_user_write_to_specific_group(self, get_pd_share):
        num_of_files = 10000

        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        udb = UserGroupGen(ctx.clients)
        udb.generate(3,2)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        directory = 'user_write_to_specific_group'
        path = "{0}{1}{2}".format(mounts[0].path,"/",directory)
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0666)
        fname = path + '/specific_group'
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        udb.clean()
        ctx.clients[0].close_agent()
        ctx.clients[0].execute(['chown', '{0}:smoke'.format(users[0]), dir])
        ctx.clients[0].execute(['dd', 'if=/dev/zero', 'of=%s/test_02/TTTTTT_%s' % (mounts.path, 1),
                                    'bs=24k', 'count=1', 'conv=fsync', 'conv=notrunc'], user=my_user)
        ctx.clients[0].execute([ 'dd', 'of=/dev/null', 'if=%s/test_02/TTTTTT_%s' % (mounts.path, 1),
                                  'bs=14k', 'count=3', 'iflag=direct'],user=my_user)

    @attr('client_base', jira='PD-5364', name='create_many_files_from_all_clients_and_list')
    def test_create_many_files_from_all_clients_and_list(self, get_pd_share):
        num_of_files = 25000
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

        ctx.clients[0].mkdirs(mounts[0].path + '/test_03')
        for i in xrange(num_of_files):
            for (client, mount) in zip(ctx.clients, mounts):
                client.execute(['dd', 'if=/dev/zero', 'of=%s/test_03/file_%s_%s' % (mount.path, client.address, i),
                                     'bs=24k', 'count=5', 'conv=fsync', 'conv=notrunc'])
        for (client, mount) in zip(ctx.clients, mounts):
            ret = client.listdir(mount.path + '/test_03')
            print " # of actual files =", len(ret)
            print " # of expected files =", num_of_files * len(ctx.clients)
            assert len(ret) == num_of_files * len(ctx.clients), 'client {0} incomplete files under dir test_02'.format(client.address)
        #Add for chech in the writing time

    @attr('client_base', jira='PD-5365', name='wrie_from_owner_cyclically_write_from_others')
    def test_wrie_from_owner_cyclically_write_from_others(self, get_pd_share):
        # chechk only that system not crashed...
        # add chechk for list , more than 3 clients.
        #assert len(ctx.client) >= 3 , "There not enough clients for the test.".len(ctx.client)

        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

        assert len(ctx.clients) >= 3 , "There not enough clients for the test. Must be more them 3 clients.  Existing only - "+str(len(ctx.clients))

        ctx.clients[0].mkdirs(mounts[0].path + '/test_04')
        fv = ctx.clients[0].execute(['dd', 'if=/dev/zero', 'of=%s/test_04/file' % (mounts[0].path),
                                     'bs=1M', 'count=1000', 'conv=fsync', 'conv=notrunc'], block=False)
        time.sleep(1)
          # Add loop for all clients
        for i in range(1,len(ctx.clients)-1):
            ctx.clients[i].execute(['dd', 'of=/dev/null', 'if=%s/test_04/file' % (mounts[i].path),
                                'bs=1M', 'count=1000', 'iflag=direct'])
            ctx.clients[i+1].execute(['dd', 'if=/dev/zero', 'of=%s/test_04/file' % (mounts[i+1].path),
                                     'bs=4k', 'count=1', 'conv=fsync', 'conv=notrunc'])
        fv.get()


    @attr('client_base', jira='PD-5366', name='owner_create_w_r_others_w_r_as_root')
    def test_owner_create_w_r_others_w_r_as_root(self, get_pd_share):

        fname = 'owner'
        num_of_files = 35
        i = 0
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)

# user write.. others read (userrs)

        udb = UserGroupGen(ctx.clients)
        udb.generate(3,2)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        directory = 'owner_1'
        path = "{0}{1}{2}".format(mounts[0].path,"/",directory)

        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0666)
        fname = path + '/basic_3'
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        self._logger.info("BEFORE OPEN")
        fd = clients.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0755)
        self._logger.info("BEFORE WRITE")
        clients.write_file(fd, 0, 32, 2)
        self._logger.info("BEFORE CLOSE")
        clients.read_file(fd, 32, 32, 2)
        clients.close_file(fd)
        self._logger.info("AFTER CLOSE")

    @attr('client_base', jira='PD-5367', name='create_user_basic')
    def test_create_user_basic(self, get_pd_share):

        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs4_2) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        udb = UserGroupGen(ctx.clients)
        udb.generate(3,2)
        groups = udb.get_groups()
        users = udb.get_users(groups[0])
        directory = 'create_user1'
        path = "{0}{1}{2}".format(mounts[0].path,"/",directory)

        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/create_user1'
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        udb.clean()
        ctx.clients[0].close_agent()

    @attr('client_base', jira='PD-5368', name='group_test1')
    def test_group_test1(self, conf_3u_2g):
        directory = 'create_user1'
        path = "{0}{1}{2}".format(self.mounts[0].path,"/",directory)
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/create_user1'
        self._logger.debug("Starting PCAPs")
        pcaps=[]
        pcaps.append(PE(ctx.clients[0]))
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        directory = 'create_user2'
        pcaps.append(PE(ctx.clients[1]))
        path = "{0}{1}{2}".format(self.mounts[1].path,"/",directory)
        users = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[1].chmod(path, 0777)
        fname = path + '/create_user1'
        fd1 = ctx.clients[1].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0655, users[1])
        results = pcaps[0].get_nfs_ops(False)
        results = pcaps[1].get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5369', name='group_test2')
    def test_group_test2(self, conf_3u_2g):
        directory = 'create_user1'
        path = "{0}{1}{2}".format(self.mounts[0].path,"/",directory)
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/create_user1'
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd())
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        directory = 'create_user2'
        path = "{0}{1}{2}".format(self.mounts[1].path,"/",directory)
        users = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[1].chmod(path, 0777)
        fname = path + '/create_user1'
        fd1 = ctx.clients[1].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0655, users[1])
        results = pcaps.get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5370', name='group_test3')
    def test_group_test3(self, conf_3u_2g):
        directory = 'create_user1'
        path = "{0}{1}{2}".format(self.mounts[0].path,"/",directory)
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname1 = path + '/create_user1'
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd())
        fd0 = ctx.clients[0].open_file(fname1, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
        directory = 'create_user2'
        path = "{0}{1}{2}".format(self.mounts[1].path,"/",directory)
        users = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[1].chmod(path, 0777)
        fname2 = path + '/create_user2'
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0655, users[1])
        results = pcaps.get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()

    @blockability
    def _write_loop(self, clients, fd, offset=0, count=32, pattern=4, user='root', timeout=None):
        while True:
            clients.write_file(fd=fd, offset=offset, count=count, pattern=pattern, user=user, timeout=timeout)

    @blockability
    def _read_loop(self, clients, fd, offset=0, count=32, pattern=4, user='root', timeout=None):
        while True:
            clients.read_file(fd=fd, offset=offset, count=count, pattern=pattern, user=user, timeout=timeout)

    @attr('client_base', jira='PD-5371', name='group_test4')
    def test_group_test4(self, conf_3u_2g):

# pcap from dd - chechk that possible write-read to same directory from difference users with difference permitions.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd())
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users[0])
 # write in thr loop
        fv = self._write_loop(ctx.clients[0], fd0, 0, 32, 4, users[0], block=False)

        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        #ctx.clients[1].execute(['cd',path2], user=users[1])
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "file_name", fname2
 #       ctx.clients[1].chmod(path, 0777)
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0777, users2[1])
        ctx.clients[1].write_file(fd1, 32, 64, 4, users2[1])

        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[1].close_file(fd1, users2[1])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5372', name='group_test5')
    def test_group_test5(self, conf_3u_2g):

# pcap from dd - chechk that possible write-read to same directory from difference users with difference permitions, A_SYNC.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(), tmp_dir='/opt')
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in the loop
        fv = self._write_loop(ctx.clients[0], fd0, 0, 32, 4, users[0], block=False)

        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "file_name", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[1].write_file(fd1, 32, 64, 4, users2[1])

        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[1].close_file(fd1, users2[1])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()


    @attr('client_base', jira='PD-5373', name='group_test6')
    def test_group_test6(self, conf_3u_2g):

# pcap from dd - chechk that possible write-read to same directory,
#write from first user, and close, read-only from secound user and deleg...., RDWR from third user with difference permitions.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(),tmp_dir='/opt')
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in the loop
        ctx.clients[0].write_file(fd0, 32, 64, 4, users[0])
        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])
        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[0].close_agent()

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "file_name", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDONLY | os.O_ASYNC, 0777, users2[0])
        fv = self._read_loop(ctx.clients[1], fd1, 0, 32, 4, users2[0], block=False)

#        3- user from secound group write to each directory
        path3 = "{}/{}".format(self.mounts[2].path,directory)
        print "path3", path3
        print "user2[1] = ", users2[1]
        ctx.clients[2].execute(['chmod', '-R', '777', path3])
        fname3 = '{}/{}'.format(path3, 'user_file')
        print "file_name", fname3
        fd2 = ctx.clients[2].open_file(fname3, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[2].write_file(fd2, 32, 64, 4, users2[1])

        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[2].close_file(fd2, users2[1])
        ctx.clients[1].close_file(fd1, users2[0])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[2].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5374', name='group_test7')
    def test_group_test7(self, conf_3u_2g):

 # pcap from dd - chechk that possible write-read to same directory,
 #write from first user, and close, read-only from secound user and deleg....,  RDWR from third user. rdOnly (read)-4-user.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        print "fname = ", fname
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(),tmp_dir='/opt')
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in thr loop
        ctx.clients[0].write_file(fd0, 32, 64, 4, users[0])
        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])
        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[0].close_agent()

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "fname2 = ", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDONLY | os.O_ASYNC, 0777, users2[0])
        fv = self._read_loop(ctx.clients[1], fd1, 0, 32, 4, users2[0], block=False)

#        3- user from secound group write to each directory
        path3 = "{}/{}".format(self.mounts[2].path,directory)
        print "path3", path3
        print "user2[1] = ", users2[1]
        ctx.clients[2].execute(['chmod', '-R', '777', path3])
        fname3 = '{}/{}'.format(path3, 'user_file')
        print "fname3 = ", fname3
        fd2 = ctx.clients[2].open_file(fname3, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[2].write_file(fd2, 32, 64, 4, users2[1])

#       4- user from secound group write to each directory
        path4 = "{}/{}".format(self.mounts[3].path,directory)
        print "path4", path4
        print "user2[2] = ", users2[2]
        ctx.clients[3].execute(['chmod', '-R', '777', path4])
        fname4 = '{}/{}'.format(path4, 'user_file')
        print "fname4 = ", fname4
        fd3 = ctx.clients[3].open_file(fname4, os.O_CREAT | os.O_RDONLY | os.O_ASYNC , 0777, users2[2])
        ctx.clients[3].read_file(fd3, 32, 64, 4, users2[2])


        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[3].close_file(fd2, users2[2])
        ctx.clients[2].close_file(fd2, users2[1])
        ctx.clients[1].close_file(fd1, users2[0])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[2].close_agent()
        ctx.clients[3].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5375', name='group_test8')
    def test_group_test8(self, conf_3u_2g):
# Negative-test:pcap from dd - chechk that possible write-read to same directory,
# write from first user, and close, read-only from secound user and deleg....,
#RDWR from third user. rdOnly (write)-4-user, expected_result_false.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        print "fname = ", fname
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(),tmp_dir='/opt')
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in thr loop
        ctx.clients[0].write_file(fd0, 32, 64, 4, users[0])
        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])
        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[0].close_agent()

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "fname2 = ", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDONLY | os.O_ASYNC, 0777, users2[0])
        fv = self._read_loop(ctx.clients[1], fd1, 0, 32, 4, users2[0], block=False)

#        3- user from secound group write to each directory
        path3 = "{}/{}".format(self.mounts[2].path,directory)
        print "path3", path3
        print "user2[1] = ", users2[1]
        ctx.clients[2].execute(['chmod', '-R', '777', path3])
        fname3 = '{}/{}'.format(path3, 'user_file')
        print "fname3 = ", fname3
        fd2 = ctx.clients[2].open_file(fname3, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[2].write_file(fd2, 32, 64, 4, users2[1])

#       4- user from secound group write to each directory ctx.clients[3].write_file(fd3, 32, 64, 4, users2[2],
        path4 = "{}/{}".format(self.mounts[3].path,directory)
        print "path4", path4
        print "user2[2] = ", users2[2]
        ctx.clients[3].execute(['chmod', '-R', '777', path4])
        fname4 = '{}/{}'.format(path4, 'user_file')
        print "fname4 = ", fname4
        fd3 = ctx.clients[3].open_file(fname4, os.O_CREAT | os.O_RDONLY | os.O_ASYNC , 0777, users2[2])
        with self.raises(Failure):
            ctx.clients[3].write_file(fd3, 32, 64, 4, users2[2])

        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[2].close_file(fd2, users2[1])
        ctx.clients[1].close_file(fd1, users2[0])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[2].close_agent()
        ctx.clients[3].close_agent()
        ctx.clients[1].close_agent()

    @attr('client_base', jira='PD-5376', name='group_test9')
    def test_group_test9(self, conf_3u_2g):

# Negative-test:pcap from dd - chechk that possible write-read to same directory,
# write from first user, and close, read-only from secound user and deleg....,
#RDWR from third user. rdOnly (write)-4-user, expected_result_false.
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        print "fname = ", fname
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(),tmp_dir='/opt')
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in thr loop
        ctx.clients[0].write_file(fd0, 32, 64, 4, users[0])
        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])
        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[0].close_agent()

#  secound user from secound group write to each directory
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2
        users2 = []
        users2 = self.udb.get_users(self.groups[1])
        print "group =", groups[1]
        ctx.clients[1].execute(['chmod', '-R', '777', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "fname2 = ", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDONLY | os.O_ASYNC, 0777, users2[0])
        fv = self._read_loop(ctx.clients[1], fd1, 0, 32, 4, users2[0], block=False)

#        3- user from secound group write to each directory
        path3 = "{}/{}".format(self.mounts[2].path,directory)
        print "path3", path3
        print "user2[1] = ", users2[1]
        ctx.clients[2].execute(['chmod', '-R', '777', path3])
        fname3 = '{}/{}'.format(path3, 'user_file')
        print "fname3 = ", fname3
        fd2 = ctx.clients[2].open_file(fname3, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[2].write_file(fd2, 32, 64, 4, users2[1])

#       4- user from secound group write to each directory ctx.clients[3].write_file(fd3, 32, 64, 4, users2[2],
        path4 = "{}/{}".format(self.mounts[3].path,directory)
        print "path4", path4
        print "user2[2] = ", users2[2]
        ctx.clients[3].execute(['chmod', '-R', '777', path4])
        fname4 = '{}/{}'.format(path4, 'user_file')
        print "fname4 = ", fname4
        fd3 = ctx.clients[3].open_file(fname4, os.O_CREAT | os.O_RDWR| os.O_ASYNC , 0777, users2[2])
        ctx.clients[3].write_file(fd3, 32, 64, 4, users2[2])

        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[2].close_file(fd2, users2[1])
        ctx.clients[1].close_file(fd1, users2[0])
        self._logger.info("AFTER CLOSE")

        results = pcaps.get_nfs_ops(False)
        ctx.clients[2].close_agent()
        ctx.clients[3].close_agent()
        ctx.clients[1].close_agent()

