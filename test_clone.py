#__author__ = 'mleopold'

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pd_common.workload import Workload
# from pd_common.pcap_engine import PcapEngine, Nfs4_Op, NfsOp_CB
import uuid
import random
import time
import os
from pytest import mark
from pd_common.helper import Helper as hlp
from pd_common.helper import DD_MNT_PREFIX
from client.models.ObjectType import ObjectType
from pytest import yield_fixture
from pd_tools.fio_caps import FioCaps
from pd_tools.fsx_caps import FsxCaps
from contextlib import contextmanager

clone_timeout = 600

NFS4ERR_DELAY = 10008
NFS4_OK = 0


class TestClone(TestBase):
    """
    A clone related test suite.
    """
    @yield_fixture
    def setup_env(self, get_mount_points):
        """
        Acts a general fixture for all tests.
        """
        self.mounts = get_mount_points
        self.mount = self.mounts[0]
        self.spath = self.mount.share.path
        self.client = ctx.clients[0]
        nodes = [dd.node for dd in ctx.cluster.get_nodes_by_type(ObjectType.DATA_SPHERE)]
        self.dd_node = nodes[0]
        yield       
    
    @yield_fixture    
    def nfs3_fixture(self, setup_env):
        """
        Prepares the ground for instance inspection
        """
        self.mntdirs = {}
        for ds in dict(ctx.cluster.data_stores).values():
            ds_ip = ds.logical_volume.ip_addresses[0].address
            ds_export = ds.logical_volume.export_path
            mntdir = '/mnt/' + str(uuid.uuid4())
            self.mntdirs[ds.internal_id] = mntdir
            self.client.mkdirs(mntdir)
            self.client.execute(['mount', '-o', "nfsvers=3", "%s:%s" % (ds_ip, ds_export), mntdir])
        yield
        for m in self.mntdirs.values():
            self.client.execute(['umount', m])
            self.client.remove(m)
        
    def create_file(self, size=100, name="/tmp/tmpfile", quick=False):
        """
        Creates a file from /dev/urandom.
        :param size: The size of the file in MB
        :param name: The path to the file
        :return: The path to the file
        """
        if not quick:
            self.client.execute(["dd", "if=/dev/urandom", "of=%s" % name, "bs=1M", "count=%d" % size]);
        else:
            seed = random.getrandbits(128) 
            self.client.execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                                 'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                 '-nosalt', '|', 'dd', 'of=%s' % name, 'bs=1M', 
                                 'count=%d' % size, 'oflag=direct', 'iflag=fullblock'], timeout=1200)
        return name

    def create_random_sparse(self, size=100, seed=None):
        """
        Creates a randomly partially sparse file
        """
        assert size > 30, 'this method only works with a >30M file size'
        if seed == None:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(13), '-s', '1'])
        else:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(13), '-s', '1'], seed=31)            
        self.client.execute_tool(fsx, self.mount)
        name = self.client.execute(['find', self.mount.path, '-name', fsx._data]).stdout.split("\n")[0]
        self.client.execute(['dd', 'if=/dev/urandom', 'of=' + name, 'bs=10M', 'oflag=direct', 'seek=1', 'count=1'])
        return name

    def basic_setup(self, size=200):
        name = self.create_file(size=size)
        name2 = name + '_mirror'
        self.client.execute(["cp", "-p", name, name2])
        src = str(uuid.uuid4())
        srcfull = self.mounts[0].path + '/' + src
        tgt = str(uuid.uuid4()) 
        tgtfull = self.mounts[0].path + '/' + tgt
        self.client.execute(["dd", "if=%s" % name, "of=%s" % srcfull, "bs=1M", "oflag=direct"]);
        return name, name2, src, srcfull, tgt, tgtfull

    # UNDER REVIEW: check that the expcted behavior is as defined in the test plan
    @attr('file_clone', jira='PD-12395', complexity=1, name='clone_basic')
    def test_clone_basic(self, setup_env):
        """
        Test - clones and compares data, no IOs
        """
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup()
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        # the next line became deprecated, we might be able to find something else to replace it using offline data
        # self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, tgtfull))
        hlp.fatality(self.client.compare_files_md5(name, srcfull))

    # UNDER REVIEW: check that the expcted behavior is as defined in the test plan
    @attr('file_clone', jira='PD-16533', complexity=1, name='clone_md_basic')
    def test_clone_md_basic(self, setup_env):
        """
        Test - clones, compares sizes & blocks, does IO, compares sizes
        """
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=1024)
        ssize = long(self.client.execute(['stat', '-c', '%s', srcfull]).stdout.split('\n')[0].strip())
        sblocks = long(self.client.execute(['stat', '-c', '%b', srcfull]).stdout.split('\n')[0].strip())
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        if random.choice([True, False]):
            ssize = long(self.client.execute(['stat', '-c', '%s', srcfull]).stdout.split('\n')[0].strip())
        tsize = long(self.client.execute(['stat', '-c', '%s', tgtfull]).stdout.split('\n')[0].strip())
        tblocks = long(self.client.execute(['stat', '-c', '%b', tgtfull]).stdout.split('\n')[0].strip())
        dsize = long(ctx.cluster.execute(['stat', '-c', '%s', 
                                          DD_MNT_PREFIX + self.spath + "/" + tgt]).stdout.split('\n')[0].strip())
        dblocks = long(ctx.cluster.execute(['stat', '-c', '%b', 
                                          DD_MNT_PREFIX + self.spath + "/" + tgt]).stdout.split('\n')[0].strip())
        assert ssize == tsize, "the sizes are different between src and tgt"
        assert ssize == dsize, "the sizes are different between src and tgt on dd share"
        assert sblocks == tblocks, "the blocks are different between src and tgt"
        assert sblocks == dblocks, "the blocks are different between src and tgt on dd share"
        self.client.execute(['dd', 'if=/dev/zero', 'of=%s' % tgtfull, 'bs=1', 'count=1', 'conv=notrunc', 'oflag=direct'])
        # the next line became deprecated, we might be able to find something else to replace it using offline data
        # self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ssize = long(self.client.execute(['stat', '-c', '%s', srcfull]).stdout.split('\n')[0].strip())
        tsize = long(self.client.execute(['stat', '-c', '%s', tgtfull]).stdout.split('\n')[0].strip())
        dsize = long(ctx.cluster.execute(['stat', '-c', '%s', 
                                          DD_MNT_PREFIX + self.spath + "/" + tgt]).stdout.split('\n')[0].strip())
        assert ssize == tsize, "the sizes are different between src and tgt"
        assert ssize == dsize, "the sizes are different between src and tgt on dd share"
        ctx.clients.drop_caches()
        ssize = long(self.client.execute(['stat', '-c', '%s', srcfull]).stdout.split('\n')[0].strip())
        tsize = long(self.client.execute(['stat', '-c', '%s', tgtfull]).stdout.split('\n')[0].strip())
        dsize = long(ctx.cluster.execute(['stat', '-c', '%s', 
                                          DD_MNT_PREFIX + self.spath + "/" + tgt]).stdout.split('\n')[0].strip())
        assert ssize == tsize, "the sizes are different between src and tgt"
        assert ssize == dsize, "the sizes are different between src and tgt on dd share"

    # UNDER REVIEW
    # TODO: Need to verify this test is working properly
    @attr('file_clone', jira='PD-12659', complexity=2, name='clone_io_basic')
    def test_clone_io_basic(self, setup_env):
        """
        Test - clones a file while another greenlet is writing to it
        """
#         name = self.create_file()
        name = '/tmp/meir'
        src = str(uuid.uuid4())
        srcfull = self.mounts[0].path + '/' + src
        tgt = str(uuid.uuid4()) 
        tgtfull = self.mounts[0].path + '/' + tgt
#         self.client.close_file(self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644))
        workload = Workload(self.client, size=104857600, sync=True)
        workload.gen_sequential_write()
        thread = workload.run([srcfull], block=False)
        time.sleep(5)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        thread.get(timeout=360)
        workload.run([name])
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))

    @attr('file_clone', jira='PD-12396', complexity=1, name='clone_write_uncopied_source')
    def test_clone_write_uncopied_source(self, setup_env):
        """
        Test - clones a file, writes to an uncopied section on source, verifies data
        """
        size = 1024
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 128, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 128, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        
    @attr('file_clone', jira='PD-12397', complexity=1, name='clone_write_uncopied_target')
    def test_clone_write_uncopied_target(self, setup_env):
        """
        Test - clones a file, writes to source, writes to an uncopied section on target, 
        verifies data
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            # check the target behavior
            fd3 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 128, 128, 2)
            self.client.close_file(fd3)
            self.client.write_file(fd4, size * 1024 * 2 - 128, 128, 2)
            self.client.close_file(fd4)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12398', complexity=1, name='clone_write_copied_source')
    def _test_clone_write_copied_source(self, setup_env):
        """
        Test - clones a file, writes to source, rewrite to the same section on source, 
        verifies data
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 128, 128, 2)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 128, 128, 2)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 128, 128, 3)
            self.client.write_file(fd1, 0, 128, 3)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 128, 128, 3)
            self.client.write_file(fd2, 0, 128, 3)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12399', complexity=1, name='clone_write_copied_target')
    def _test_clone_write_copied_target(self, setup_env):
        """
        Test - clones a file, writes to source, , write to target, 
        rewrite to the same section on target, verifies data
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            # check the target behavior
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 128, 128, 2)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 128, 128, 2)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        try:
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 128, 128, 3)
            self.client.write_file(fd1, 0, 128, 3)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 128, 128, 3)
            self.client.write_file(fd2, 0, 128, 3)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12400', complexity=1, name='clone_write_uncopied_cross_eof_source')
    def _test_clone_write_uncopied_cross_eof_source(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 64, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 64, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12401', complexity=1, name='clone_write_uncopied_cross_eof_target')
    def _test_clone_write_uncopied_cross_eof_target(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            # check the target behavior
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 64, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 64, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12402', complexity=1, name='clone_write_append_source')
    def _test_clone_write_append_source(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_APPEND, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_APPEND, 0644)
            self.client.write_file(fd1, size * 1024 * 2, 128, 1)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2, 128, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12403', complexity=1, name='clone_write_append_target')
    def _test_clone_write_append_target(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            # check the target behavior
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_APPEND, 0644)
            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_APPEND, 0644)
            self.client.write_file(fd1, size * 1024 * 2, 128, 1)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2, 128, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12404', complexity=1, name='clone_write_partially_copied_source')
    def _test_clone_write_partially_copied_source(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 250, 4, 2)
            self.client.write_file(fd1, size * 1024 * 2 - 256, 16, 3)
            self.client.write_file(fd1, size * 1024 * 2 - 260, 8, 1)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 250, 4, 2)
            self.client.write_file(fd2, size * 1024 * 2 - 256, 16, 3)
            self.client.write_file(fd2, size * 1024 * 2 - 260, 8, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-12405', complexity=1, name='clone_write_partially_copied_target')
    def _test_clone_write_partially_copied_target(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 250, 4, 2)
            self.client.write_file(fd1, size * 1024 * 2 - 256, 16, 3)
            self.client.write_file(fd1, size * 1024 * 2 - 260, 8, 1)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 250, 4, 2)
            self.client.write_file(fd2, size * 1024 * 2 - 256, 16, 3)
            self.client.write_file(fd2, size * 1024 * 2 - 260, 8, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # TODO: this test needs to be completely refactored IMHO (Signed: mleopold)
    @attr('file_clone', jira='PD-12406', complexity=1, name='clone_read_source')
    @mark.skipif(True, reason="the test needs refactoring")
    def _test_clone_read_source(self, setup_env):
        """
        """
        size = 200
        name = '/tmp/' + str(uuid.uuid4())
        try:
            fd1 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            for i in xrange(size * 1024 * 2 / 128):
                self.client.write_file(fd1, i * 128, 128, 1)
            self.client.close_file(fd1)
        finally:
            self.client.close_agent()
        name2 = name + '_mirror'
        self.client.execute(["cp", "-p", name, name2])
        src = str(uuid.uuid4())
        srcfull = self.mounts[0].path + '/' + src
        tgt = str(uuid.uuid4()) 
        tgtfull = self.mounts[0].path + '/' + tgt
        self.client.execute(["dd", "if=%s" % name, "of=%s" % srcfull, "bs=1M", "oflag=direct"]);
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.read_file(fd1, 0, 128, 1)
            self.client.write_file(fd1, size * 1024 * 2 - 256, 64, 2)
            self.client.read_file(fd1, size * 1024 * 2 - 128, 64, 1)
            ctx.clients.drop_caches()
            self.client.read_file(fd1, size * 1024 * 2 - 256, 64, 2)
            self.client.read_file(fd1, size * 1024 * 2 - 320, 64, 1)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 250, 64, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-20254', complexity=1, name='clone_short_read_source')
    def test_clone_short_read_source(self, setup_env):
        """
        Test - does a short read from source while clone is running
        """
        size = 1000
        name, _, src, srcfull, tgt, _ = self.basic_setup(size=size)
        name3 = name + '_3'
        name4 = name + '_4'
        bs = 1048576
        count = 1000
        skip = bs * count - 4096

        self.client.execute(['dd', 'if={}'.format(srcfull), 'of={}'.format(name3),
                             'skip={}'.format(skip), 'iflag=skip_bytes', 'iflag=direct',
                             'bs={}'.format(bs), 'count=1'])
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 64, 2)
            self.client.close_file(fd2)
            ctx.clients.drop_caches()
            self.client.execute(['dd', 'if={}'.format(srcfull), 'of={}'.format(name4),
                                 'skip={}'.format(skip), 'iflag=skip_bytes', 'iflag=direct',
                                 'bs={}'.format(bs), 'count=1'])
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name3, name4))
        finally:
            self.client.close_agent()

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-20255', complexity=1, name='clone_short_read_target')
    def test_clone_short_read_target(self, setup_env):
        """
        Test - does a short read from source while clone is running
        """
        size = 1000
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        name3 = name + '_3'
        name4 = name + '_4'
        bs = 1048576
        count = 1000
        skip = bs * count - 4096

        self.client.execute(['dd', 'if={}'.format(srcfull), 'of={}'.format(name3),
                             'skip={}'.format(skip), 'iflag=skip_bytes', 'iflag=direct',
                             'bs={}'.format(bs), 'count=1'])
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 64, 2)
            self.client.close_file(fd2)
            ctx.clients.drop_caches()
            self.client.execute(['dd', 'if={}'.format(tgtfull), 'of={}'.format(name4),
                                 'skip={}'.format(skip), 'iflag=skip_bytes', 'iflag=direct',
                                 'bs={}'.format(bs), 'count=1'])
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name3, name4))
        finally:
            self.client.close_agent()

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-12407', complexity=1, name='clone_truncate_up_write_source')
    def test_clone_truncate_up_write_source(self, setup_env):
        """
        """
        size = 20
        new_size = size * 1024 * 1024 + random.randint(1, 131072)
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.write_file(fd1, size / 2, 64, 1)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.write_file(fd2, size / 2, 64, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-12408', complexity=3, name='clone_truncate_down_source')
    def test_clone_truncate_down_source(self, setup_env):
        """
        """
        size=20
        new_size = size * 1024 * 1024 - random.randint(1, 131072)
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # OBSOLETE
    @attr('file_clone', jira='PD-12409', complexity=3, name='clone_truncate_zero_source')
    def _test_clone_truncate_zero_source(self, setup_env):
        """
        """
        size = 20        
        new_size = 0
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-12410', complexity=3, name='clone_truncate_up_target')
    def test_clone_truncate_up_target(self, setup_env):
        """
        """
        size = 20
        new_size = size * 1024 * 1024 + random.randint(1, 131072)
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # READY FOR OFFLOADED CLONES - I'm leaving it for now, although this might be not needed
    @attr('file_clone', jira='PD-12411', complexity=3, name='clone_truncate_down_target')
    def test_clone_truncate_down_target(self, setup_env):
        """
        """
        size = 20
        new_size = size * 1024 * 1024 - random.randint(1, 131072)
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # OBSOLETE
    @attr('file_clone', jira='PD-12412', complexity=3, name='clone_truncate_zero_target')
    def _test_clone_truncate_zero_target(self, setup_env):
        """
        """
        size = 20
        new_size = 0
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.truncate_file(fd1, new_size)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.truncate_file(fd2, new_size)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull, timeout=90))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull, timeout=90))

    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-12413', complexity=1, name='clone_between_shares')
    def test_clone_between_shares(self, setup_env):
        """
        Test - clone between different shares, expect failure
        """
        second_share = None
        passed = False
        if (len(ctx.cluster.shares) < 3):
            second_share = ctx.cluster.cli.share_create(name='second_share', path='/second_share',
                                         export_option='*,RW,root-squash')
        else:
            second_share = random.choice([s for s in dict(ctx.cluster.shares).values() 
                                          if s.name != self.mounts[0].share.name] 
                                          and s.name != "root") 
        _, _, src, _, tgt, _ = self.basic_setup()
        try:
            ctx.cluster.clone(self.spath + "/" +  src, second_share.path + "/" +  tgt)
        except:
            passed = True
        assert passed, "Clone between shares should have failed"

    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-12414', complexity=1, name='clone_between_file_systems')
    def test_clone_between_file_systems(self, setup_env):
        """
        Test - clone between file systems, expect failure
        """
        passed = False
        _, _, src, _, tgt, _ = self.basic_setup()
        try:
            ctx.cluster.clone(self.spath + "/" +  src, 
                            "/tmp/" +  tgt)
        except:
            passed = True
        assert passed, "Clone between file systems should have failed"

    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-12415', complexity=1, name='clone_sync_rand_write')
    def test_clone_sync_rand_write(self, setup_env):
        """
        """
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup()
        workload = Workload(self.client, size=1073741824)
        workload.gen_rand_write(10000)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" + tgt)
        workload.run([srcfull], sync=True)
        workload.run([name], sync=True)
        #self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    @attr('file_clone', jira='next', complexity=1, name='clone_direct_rand_write_both')
    def _test_clone_direct_rand_write_both(self, setup_env):
        """
        """
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup()
        workload = Workload(self.client, size=1073741824, fcount=2)
        workload.gen_rand_write(10000)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" + tgt)
        workload.run([srcfull, tgtfull], direct=True)
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        workload.run([name, name2], direct=True)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # READY FOR OFFLOADED CLONES
    # This is a really important test that tests the PIT (Point In Time) consistency of a clone
    @attr('file_clone', jira='PD-12416', complexity=1, name='clone_sync_rand_write_dme_down')
    def test_clone_sync_rand_write_dme_down(self, setup_env):
        """
        """
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup()
        workload = Workload(self.client)
        workload.gen_rand_write(1000)
        ctx.cluster.service.stop('pd-dme')
        t1 = t2 = time.time()        
        while t2 - t1 < 60:
            try:
                ctx.cluster.service.status()
            except:
                break
            t2 = time.time()
        assert t2 - t1 < 60, "the pd-dme service did not stop within 60s"
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" + tgt)
        thread = workload.run([srcfull], sync=True, block=False)
        ctx.cluster.service.start('pd-dme')
        ctx.cluster.service.wait_for('pd-dme')
        thread.get()
        workload.run([name], sync=True)
        #self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-12417', complexity=1, name='clone_async_rand_write')
    def test_clone_async_rand_write(self, setup_env):
        """
        """
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup()
        workload = Workload(self.client)
        workload.gen_rand_write(100)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" + tgt)
        workload.run([srcfull], sync=False)
        workload.run([name], sync=False)
        #self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    # TODO: add a check to see that the instance gets deleted eventually
    @attr('file_clone', jira='PD-13507', complexity=3, name='clone_delete_source_after_cow')
    def _test_clone_delete_source_after_cow(self, setup_env):
        """
        """
        size = 200
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.remove(srcfull)
            self.client.close_file(fd1)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, tgtfull))

    # OBSOLETE
    # TODO: add a check to see that the instance gets deleted eventually
    @attr('file_clone', jira='PD-13508', complexity=3, name='clone_delete_target_after_cow')
    def _test_clone_delete_target_after_cow(self, setup_env):
        """
        """
        size = 200
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.remove(tgtfull)
            self.client.close_file(fd1)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-13509', complexity=3, name='clone_delete_source_before_cow')
    def _test_clone_delete_source_before_cow(self, setup_env):
        """
        """
        size = 200
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            self.client.remove(srcfull)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-13510', complexity=3, name='clone_delete_target_before_cow')
    def _test_clone_delete_target_before_cow(self, setup_env):
        """
        """
        size = 200
        name, _, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            self.client.remove(tgtfull)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))

    # TODO: finish up this test, it's not ready
    @attr('file_clone', jira='next', complexity=2, name='clone_revert_source')
    def _test_clone_revert_source(self, setup_env):
        """
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.close_file(fd1)
            ctx.cluster.clone(self.spath + "/" +  tgt, self.spath + "/" +  src)
            
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name2, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # TODO: finish up this test, it's not ready
    @attr('file_clone', jira='next', complexity=2, name='clone_revert_target')
    def _test_clone_revert_target(self, setup_env):
        """
        """
        size = 200
        _, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.close_file(fd1)
            ctx.cluster.clone(self.spath + "/" +  tgt, self.spath + "/" +  src)

            fd2 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name2, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    def create_fio_conf_file(self, filename, iops=10, period=60, rwmixread=0, bsrange="4K-4K"):
        """
        Generates a FIO configuration file
        :param filename: A path to the file which will endure IOs
        :param iops: IO per seconds rate
        :param period: A time limit for the FIO tool's execution
        :param rwmixread: Read/Write mix, read percentage
        :param bsrange: Block size range, e.g. 4K-64K
        :return: A path to the FIO configuration file
        """
        f = "/tmp/fio.conf" + str(time.time())
        contents = ["\[global\]", "time_based", "runtime=%ss" % str(period), "rw=randrw",
                    "direct=1", "ioengine=libaio", "bsrange=%s" % bsrange, "rwmixread=%s" % rwmixread, "iodepth=64",
                    "\[job1\]", "name=job1", "filename=%s" % filename, "size=100M",
                    "rate_iops=%s" % str(iops)]
        if self.client.exists(f) and self.client.isfile(f):
            self.client.remove(f)
        for line in contents:
            self.client.execute(["echo", line, ">>", f])
        return f

    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-17773', complexity=2, name='clone_fio_source')
    def test_clone_fio_source(self, setup_env):
        """
        Test - Performs multiple clones while running fio
        """
        copies = 22
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        f = self.create_fio_conf_file(fname, 150, 300)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mounts[0], block=False)
        time.sleep(15)
        for i in xrange(copies):
            ctx.cluster.clone(self.spath + "/" +  name, self.spath + "/" +  name + "_" + str(i))
        # need to find some other way to see that the clone is done
        #for i in xrange(copies):
        #    self.client.wait_for_clone([fname, fname + "_" + str(i)], self.mount.share)
        thread.get(timeout=330)

    @attr('file_clone', jira='next', complexity=2, name='clone_fio_source_multiple')
    def _test_clone_fio_source_multiple(self, setup_env):
        """
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        f = self.create_fio_conf_file(fname, 150, 300)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        i = 0
        thread = self.client.execute_tool(fio, self.mounts[0], block=False)
        time.sleep(15)
        while not thread.ready():
            ctx.cluster.clone(self.spath + "/" +  name, 
                            self.spath + "/" +  name + "_" + str(i % 10))
            i += 1
        thread.get()

    @attr('file_clone', jira='next', complexity=2, name='clone_fio_both_multiple')
    def _test_clone_fio_both_multiple(self, setup_env):
        """
        """
        name = str(uuid.uuid4())
        name2 = name + "_1"
        fname = self.mounts[0].path + '/' + name
        fname2 = self.mounts[0].path + '/' + name2
        f = self.create_fio_conf_file(fname, 150, 300)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        f2 = self.create_fio_conf_file(fname2, 150, 300)
        fio2 = FioCaps(conf_file=f2)
        fio2.generic([])
        thread = self.client.execute_tool(fio, self.mounts[0], block=False)
        thread2 = self.client.execute_tool(fio2, self.mounts[0], block=False)
        time.sleep(30)
        while not thread.ready():
            ctx.cluster.clone(self.spath + "/" +  name, self.spath + "/" +  name2)
            time.sleep(30)
        thread.get()
        thread2.get()

    def get_instance_path(self, info):
        """
        Returns an instance path on the DS, can only be used by tests that use the internal
        nfs3_fixture
        @param param: A file_decalre object
        @return: A path to a file on the DS
        """
        obses = [instance.data_store for instance in info.instances]
        mnt1 = self.mntdirs[obses[0].internal_id]
        return mnt1 + '/' + '/'.join(info.instances[0].path.split('/')[2:])

    def clone_sparse(self, size=100, seed=None):
        """
        clones a partially sparse file and verify that the clone takes about the 
        same amount of blocks
        @param size: the size of the file
        @param seed: a seed for fsx  
        """
        name = self.create_random_sparse(size=size, seed=seed)
        name2 = name + "_1"
        dd_name = self.mount.share.path + '/' + '/'.join(name.split('/')[3:])
        dd_name2 = dd_name + "_1" 
        ctx.cluster.clone(dd_name, dd_name2)
        try:
            fd1 = self.client.open_file(name2, os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 64, 1)
            self.client.write_file(fd2, 0, 64, 1)
            self.client.close_file(fd1)
            self.client.close_file(fd2)            
        finally:
            self.client.close_agent()
        self.client.wait_for_clone([name, name2], self.mount.share)
        info1 = self.client.file_declare(name, self.mounts[0].share)
        info2 = self.client.file_declare(name2, self.mounts[0].share)
        file1 = self.get_instance_path(info1)
        file2 = self.get_instance_path(info2)
        blocks1 = self.client.execute(['stat', '-c', '\"%b\"', file1]).stdout.split("\n")[0]
        blocks2 = self.client.execute(['stat', '-c', '\"%b\"', file2]).stdout.split("\n")[0]
        rsd = hlp.get_rsd([float(blocks1), float(blocks2)])
        assert rsd <= 0.25, "the rsd between the two block counts is larger than 0.25"

    
    # OBSOLETE
    @attr('file_clone', jira='PD-16534', complexity=1, name='clone_sparse')
    def _test_clone_sparse(self, nfs3_fixture):
        """
        Test - clones a partially sparse file and verify that the clone takes about the 
        same amount of blocks, the sparseness is not deterministic
        """
        self.clone_sparse()

    # OBSOLETE
    @attr('file_clone', jira='PD-16951', complexity=1, name='clone_sparse_deter')
    def _test_clone_sparse_deter(self, nfs3_fixture):
        """
        Test - clones a partially sparse file and verify that the clone takes about the 
        same amount of blocks, the sparseness is deterministic
        """
        self.clone_sparse(seed=31)
    
    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-17035', complexity=3, name='clone_rm_no_cow')
    def test_clone_rm_no_cow(self, setup_env):
        """
        Test - verifies that cow is not triggered when we delete a file in a clone
        no cow scenario + verifies the file's data
        """
        local = self.create_file(size=1000, quick=True)
        # delete the source in a clone no cow scenario
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        self.client.execute(['dd', 'if=%s' % local, 'of=%s' % fname, 'bs=1M', 'oflag=direct'])
        info1 = self.client.file_declare(fname , self.mounts[0].share)
        self.client.drop_caches()
        ctx.cluster.clone(self.spath + "/" +  name, self.spath + "/" +  name + "_1")
        self.client.remove(fname)
        info2 = self.client.file_declare(fname + "_1", self.mounts[0].share)
        assert info1.path == info2.path, "cow was activated without a need"
        hlp.fatality(self.client.compare_files_md5(fname + "_1", local))
        # delete the target in a clone no cow scenario - less efective not that we are in offloaded clones
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        self.client.execute(['dd', 'if=%s' % local, 'of=%s' % fname, 'bs=1M', 'oflag=direct'])
        self.client.drop_caches()
        ctx.cluster.clone(self.spath + "/" +  name, self.spath + "/" +  name + "_1")
        self.client.remove(fname + "_1")
        info2 = self.client.file_declare(fname, self.mounts[0].share)
        assert info1.path == info2.path, "cow was activated without a need"
        hlp.fatality(self.client.compare_files_md5(fname, local))
        
    # UNDER REVIEW: check that the expcted behavior is as defined in the test plan
    @attr('file_clone', jira='PD-17079', complexity=3, name='clone_rm_source')
    def test_clone_rm_source(self, nfs3_fixture):
        """
        Test - verifies the behavior when we delete a file in a clone + cow scenario
        first IO goes to the source
        """
        #dis = [str(di.internal_id) for di in ctx.cluster.get_nodes_by_type(ObjectType.DATA_MOVER)]
        local = self.create_file(size=1000, quick=True)
        name1 = str(uuid.uuid4())
        fname1 = self.mounts[0].path + '/' + name1
        name2 = name1 + "_1"
        fname2 = self.mounts[0].path + '/' + name2
        self.client.execute(['dd', 'if=%s' % local, 'of=%s' % fname1, 'bs=1M', 'oflag=direct'])
        self.client.drop_caches()
        ctx.cluster.clone(self.spath + "/" +  name1, self.spath + "/" +  name2)
        try:
            # get files info
            info1 = self.client.file_declare(fname1, self.mounts[0].share)
            file1 = self.get_instance_path(info1)
            # do an IO to start cow (the target inode has the source object)
            fd1 = self.client.open_file(fname1, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.remove(fname2)
            # verify that the file was deleted from the namespace
            assert not self.client.exists(fname2), 'the file wasn\'t deleted'
            # verify that the file wasnot deleted from the DS
            self.client.execute(['stat', file1])
            # verify that the file is still proxied
            info = ctx.cluster.get_file_info(inode=info1.inode, share=self.mounts[0].share)
            # HOW CAN WE VERIFY THIS NOW???
            #assert len([i for i in info.instances if i.obs_id in dis]) != 0, "the file should be proxied"
            # remove the second file
            self.client.remove(fname1)
            # verify that the file was deleted from the namespace
            assert not self.client.exists(fname1), 'the file wasn\'t deleted'
            time.sleep(5)
            # verify that the file was deleted from the DS
            assert not self.client.exists(file1)
        finally:
            self.client.close_agent()

    # UNDER REVIEW: check that the expcted behavior is as defined in the test plan
    @attr('file_clone', jira='PD-17113', complexity=3, name='clone_rm_target')
    def test_clone_rm_target(self, nfs3_fixture):
        """
        Test - verifies the behavior when we delete a file in a clone + cow scenario 
        first IO goes to the target
        """
        #dis = [str(di.internal_id) for di in ctx.cluster.get_nodes_by_type(ObjectType.DATA_MOVER)]
        local = self.create_file(size=1000, quick=True)
        name1 = str(uuid.uuid4())
        fname1 = self.mounts[0].path + '/' + name1
        name2 = name1 + "_1"
        fname2 = self.mounts[0].path + '/' + name2
        self.client.execute(['dd', 'if=%s' % local, 'of=%s' % fname1, 'bs=1M', 'oflag=direct'])
        self.client.drop_caches()
        ctx.cluster.clone(self.spath + "/" +  name1, self.spath + "/" +  name2)
        try:
            # get the files' info
            info1 = self.client.file_declare(fname1, self.mounts[0].share)        
            # do an IO to start cow (the target inode has the source object)
            fd2 = self.client.open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            self.client.remove(fname2)
            # verify that the file was deleted from the namespace
            assert not self.client.exists(fname2), 'the file wasn\'t deleted'
            time.sleep(5)
            # verify that the file is no longer proxied
            info = ctx.cluster.get_file_info(inode=info1.inode, share=self.mounts[0].share)
            # HOW CAN WE VERIFY THIS NOW???
            #assert len([i for i in info.instances if i.obs_id in dis]) == 0, "the file should not be proxied"
        finally:
            self.client.close_agent()

    # UNDER REVIEW: check that the expcted behavior is as defined in the test plan
    @attr('file_clone', jira='PD-17368', complexity=1, name='clone_override')
    def test_clone_override(self, nfs3_fixture):
        """
        Test - verifies that it is possible to clone to an exiting file and that the inode remains the same
        """
        local = self.create_file(size=100, quick=True)
        name1 = str(uuid.uuid4())
        fname1 = self.mounts[0].path + '/' + name1
        name2 = name1 + "_1"
        fname2 = self.mounts[0].path + '/' + name2
        self.client.execute(['dd', 'if=%s' % local, 'of=%s' % fname1, 'bs=1M', 'oflag=direct'])
        ssize = long(self.client.execute(['stat', '-c', '%s', fname1]).stdout.split('\n')[0].strip())
        sblocks = long(self.client.execute(['stat', '-c', '%b', fname1]).stdout.split('\n')[0].strip())
        self.create_file(size=99, quick=True, name=fname2)
        info1 = self.client.file_declare(fname2, self.mounts[0].share)
        ctx.cluster.clone(self.spath + "/" +  name1, self.spath + "/" +  name2)
        info2 = self.client.file_declare(fname2, self.mounts[0].share)
        assert info1.inode == info2.inode, "the inode number has changed"
        tsize = long(self.client.execute(['stat', '-c', '%s', fname2]).stdout.split('\n')[0].strip())
        tblocks = long(self.client.execute(['stat', '-c', '%b', fname2]).stdout.split('\n')[0].strip())
        assert tsize == ssize, 'expected size {} found {}' .format(ssize, tsize)
        assert tblocks == sblocks, 'expected blocks {} found {}'.format(sblocks, tblocks)
        try:
            fd1 = self.client.open_file(local, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.close_file(fd2)
            self.client.remove(fname1)
            assert self.client.compare_files_md5(fname2, local)
        finally:
            self.client.close_agent()

    # OBSOLETE
    @attr('file_clone', jira='PD-18252', complexity=1, name='clone_write_uncopied_sprase_source')
    def _test_clone_write_uncopied_sprase_source(self, setup_env):
        """
        Test - tries to write to an uncopied area which is partially hole  
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        self.client.truncate(name, str(size + 1) + "M")
        self.client.truncate(name2, str(size + 1) + "M")
        self.client.truncate(srcfull, str(size + 1) + "M")
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 8, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 8, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    # OBSOLETE
    @attr('file_clone', jira='PD-18253', complexity=1, name='clone_write_uncopied_sprase_target')
    def _test_clone_write_uncopied_sprase_target(self, setup_env):
        """
        Test - tries to write to an uncopied area which is partially hole
        """
        size = 200
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        self.client.truncate(name, str(size + 1) + "M")
        self.client.truncate(name2, str(size + 1) + "M")
        self.client.truncate(srcfull, str(size + 1) + "M")
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            # check the target behavior
            fd3 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 8, 128, 2)
            self.client.close_file(fd3)
            self.client.write_file(fd4, size * 1024 * 2 - 8, 128, 2)
            self.client.close_file(fd4)
        finally:
            self.client.close_agent()
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        ctx.clients.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        
    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-20016', complexity=1, name='clone_vmclone_scenario1')
    def test_clone_vmclone_scenario1(self, setup_env):
        size=200
        # create A
        name1 = self.create_file(size=size)
        name2 = name1 + '_2'
        name3 = name1 + '_3'
        dd1 = str(uuid.uuid4())
        dd1full = self.mounts[0].path + '/' + dd1
        dd2 = str(uuid.uuid4()) 
        dd2full = self.mounts[0].path + '/' + dd2
        dd3 = str(uuid.uuid4()) 
        dd3full = self.mounts[0].path + '/' + dd3
        self.client.execute(["dd", "if=%s" % name1, "of=%s" % dd1full, "bs=1M", "oflag=direct"])
        # clone A to B
        self.client.execute(["cp", "-p", name1, name2])
        ctx.cluster.clone(self.spath + "/" +  dd1, self.spath + "/" +  dd2)        
        # write A
        try:
            fd1 = self.client.open_file(name1, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(dd1full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.write_file(fd2, 0, 128, 2)
            # clone B to C
            self.client.execute(["cp", "-p", name2, name3])
            ctx.cluster.clone(self.spath + "/" +  dd2, self.spath + "/" +  dd3)
            # delete B
            self.client.remove(name2)
            self.client.remove(dd2full)
            # write C
            fd3 = self.client.open_file(name3, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(dd3full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 4096, 128, 2)
            self.client.write_file(fd4, size * 1024 * 2 - 4096, 128, 2)          
            self.client.close_file(fd1)
            self.client.close_file(fd2)
            self.client.close_file(fd3)
            self.client.close_file(fd4)
            
            # Data verification
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name1, dd1full))
            hlp.fatality(self.client.compare_files_md5(name3, dd3full))
            # need to wait for clone in another way
            #self.client.wait_for_clone([dd1full, dd3full], self.mount.share)
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name1, dd1full))
            hlp.fatality(self.client.compare_files_md5(name3, dd3full))
            fd1 = self.client.open_file(dd1full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(dd3full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 4096, 128, 2)
            self.client.write_file(fd2, size * 1024 * 2 - 4096, 128, 2)           
            self.client.close_file(fd1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        
    # READY FOR OFFLOADED CLONES
    @attr('file_clone', jira='PD-20017', complexity=1, name='clone_vmclone_scenario2')
    def test_clone_vmclone_scenario2(self, setup_env):
        size=200
        # create A
        name1 = self.create_file(size=size)
        name2 = name1 + '_2'
        name3 = name1 + '_3'
        dd1 = str(uuid.uuid4())
        dd1full = self.mounts[0].path + '/' + dd1
        dd2 = str(uuid.uuid4()) 
        dd2full = self.mounts[0].path + '/' + dd2
        dd3 = str(uuid.uuid4()) 
        dd3full = self.mounts[0].path + '/' + dd3
        self.client.execute(["dd", "if=%s" % name1, "of=%s" % dd1full, "bs=1M", "oflag=direct"])
        # clone A to B
        self.client.execute(["cp", "-p", name1, name2])
        ctx.cluster.clone(self.spath + "/" +  dd1, self.spath + "/" +  dd2)        
        # write A
        try:
            fd1 = self.client.open_file(name1, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(dd1full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.write_file(fd2, 0, 128, 2)
            # clone B to C
            self.client.execute(["cp", "-p", name2, name3])
            ctx.cluster.clone(self.spath + "/" +  dd2, self.spath + "/" +  dd3)
            # write C
            fd3 = self.client.open_file(name3, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(dd3full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 4096, 128, 2)
            self.client.write_file(fd4, size * 1024 * 2 - 4096, 128, 2)          
            self.client.close_file(fd1)
            self.client.close_file(fd2)
            self.client.close_file(fd3)
            self.client.close_file(fd4)
            # delete B
            self.client.remove(name2)
            self.client.remove(dd2full)
            
            # Data verification
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name1, dd1full))
            hlp.fatality(self.client.compare_files_md5(name3, dd3full))
            # need to wait for clone in another way
            #self.client.wait_for_clone([dd1full, dd3full], self.mount.share)
            # TODO: verify that the instance of the delete file was removed from the DS
            ctx.clients.drop_caches()
            hlp.fatality(self.client.compare_files_md5(name1, dd1full))
            hlp.fatality(self.client.compare_files_md5(name3, dd3full))
            fd1 = self.client.open_file(dd1full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(dd3full, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 4096, 128, 2)
            self.client.write_file(fd2, size * 1024 * 2 - 4096, 128, 2)           
            self.client.close_file(fd1)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()

#     @attr(jira='next', complexity=1, name='clone_many')
#     def test_clone_many(self, setup_env):
#         files = []
#         for _ in xrange(100):
#             files.append((self.create_file(size=10, name=self.mounts[0].path + '/' + str(uuid.uuid4())), 
#                           self.mounts[0].path + '/' + str(uuid.uuid4())))
#         for source, target in files:
#             ctx.cluster.clone(source, target)
#         # need to add verification here

#     @attr(jira='next', complexity=1, name='clone_setup_ro_layouts_source')
#     def test_clone_ro_layouts_source(self, setup_env):
#         client = self.client
#         src = str(uuid.uuid4())
#         srcfull = self.mounts[0].path + '/' + src
#         tgt = str(uuid.uuid4()) 
#         try:
#             fd = client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#             client.write_file(fd, 0, 32, 2)
#             client.write_file(fd, 64, 32, 2)
#             client.close_file(fd)
#             t1 = t2 = time.time()
#             no_rw_layouts = False
#             while (t2 - t1 < 100 and not no_rw_layouts):
#                 file_details = self.client.file_declare(srcfull, self.mounts[0].share)
#                 if not file_details.rw_layouts:
#                     no_rw_layouts = True
#                 t2 = time.time()
#             assert t2 - 100 < t1, "The rw layouts were did no return after more than 100s after a file close"
#             fd = client.open_file(srcfull, os.O_CREAT | os.O_RDONLY, 0644)
#             client.read_file(fd, 0, 32, 2)
#             file_details = self.client.file_declare(srcfull, self.mounts[0].share)
#             assert file_details.ro_layouts, "the file did not get a read only layout"
#             pe = PcapEngine(client=ctx.cluster)
#             ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
#             pkts = pe.get_nfs_ops(cleanup=False)
#             found = {'clone_err': False, 'cb_layout_recall': False, 'clone_ok': False}
#             for pkt in pkts:
#                 for op in pkt:
#                     if not op.is_cb and not op.is_call and op.op_type == Nfs4_Op.CLONE:
#                         if op.status != NFS4_OK:
#                             found['clone_err'] = True
#                             self._logger.info("found a clone error packet: %s", str(op))
#                             break
#                         else:
#                             found['clone_ok'] = True
#                     elif not op.is_call and op.is_cb and op.op_type == NfsOp_CB.OP_CB_LAYOUTRECALL:
#                         found['cb_layout_recall'] = True
#                         self._logger.info("found a cb_layout_recall packet: %s", str(op))
#                         break
#             client.read_file(fd, 64, 32, 2)
#             client.close_file(fd)
#             assert not found['clone_err'], "Found a clone error reply"
#             assert not found['cb_layout_recall'],"Found a cb_layout_recall" 
#             assert found['clone_ok'], "did not find a clone NFS4_OK reply"
#         finally:
#             client.close_agent()

    @contextmanager
    def clone_no_cow(self, size=200):
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        yield
        i1 = self.client.file_declare(srcfull, self.mounts[0].share)
        i2 = self.client.file_declare(tgtfull, self.mounts[0].share)
        assert i1.obj_id == i2.obj_id, "object id is different between the inodes"
        try:
            # check the target behavior
            fd3 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 256, 128, 2)
            self.client.close_file(fd3)
            self.client.write_file(fd4, size * 1024 * 2 - 256, 128, 2)
            self.client.close_file(fd4)
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, size * 1024 * 2 - 8192, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, size * 1024 * 2 - 8192, 128, 2)
            self.client.close_file(fd2)
        finally:
            self.client.close_agent()
        self.client.drop_caches()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))        
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))
        self.client.wait_for_clone([srcfull, tgtfull], self.mount.share)
        
        
    @contextmanager
    def clone_cow(self, size=200):
        name, name2, src, srcfull, tgt, tgtfull = self.basic_setup(size=size)
        ctx.cluster.clone(self.spath + "/" +  src, self.spath + "/" +  tgt)
        try:
            # trigger clone from source
            fd1 = self.client.open_file(srcfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd2 = self.client.open_file(name, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 0, 128, 2)
            self.client.close_file(fd1)
            self.client.write_file(fd2, 0, 128, 2)
            self.client.close_file(fd2)
            yield
            # check the target behavior
            fd3 = self.client.open_file(tgtfull, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            fd4 = self.client.open_file(name2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd3, size * 1024 * 2 - 256, 128, 2)
            self.client.close_file(fd3)
            self.client.write_file(fd4, size * 1024 * 2 - 256, 128, 2)
            self.client.close_file(fd4)
        finally:
            self.client.close_agent()
        hlp.fatality(self.client.compare_files_md5(name, srcfull))
        hlp.fatality(self.client.compare_files_md5(name2, tgtfull))

    @attr(jira='next', complexity=4, name='clone_hardboot_no_cow')
    def _test_clone_clone_hardboot_no_cow(self, setup_env):
        """
        Test - clones a file, hard reboot, no cow
        """
        with self.clone_no_cow():
            ctx.cluster.force_reboot()

    @attr(jira='next', complexity=4, name='clone_reboot_no_cow')
    def _test_clone_clone_reboot_no_cow(self, setup_env):
        """
        Test - clones a file, reboot, no cow
        """
        with self.clone_no_cow():
            ctx.cluster.reboot()

    @attr(jira='next', complexity=4, name='clone_kill_dme_no_cow')
    def _test_clone_clone_kill_dme_no_cow(self, setup_env):
        """
        Test - clones a file, kill dme, no cow
        """
        with self.clone_no_cow():
            pid = ctx.cluster.execute(['pgrep', '-f', 'com.primarydata.dme.main.Main']).stdout.split('\n')[0]
            ctx.cluster.execute(['kill', '-9', pid])
            ctx.cluster.execute(['systemctl', 'start', 'pd-dme']).stdout.split('\n')[0]

    @attr(jira='next', complexity=4, name='clone_kill_dme_long_no_cow')
    def _test_clone_clone_kill_dme_long_no_cow(self, setup_env):
        """
        Test - clones a file, kill dme, sleep, no cow
        """
        with self.clone_no_cow():
            pid = ctx.cluster.execute(['pgrep', '-f', 'com.primarydata.dme.main.Main']).stdout.split('\n')[0]
            ctx.cluster.execute(['kill', '-9', pid])
            ctx.cluster.execute(['pcs', 'resource', 'disable', 'PdDmeResource'])
            time.sleep(90)
            ctx.cluster.execute(['pcs', 'resource', 'enable', 'PdDmeResource'])

    @attr(jira='next', complexity=4, name='clone_kill_protod_no_cow')
    def _test_clone_clone_kill_protod_no_cow(self, setup_env):
        """
        Test - clones a file, kill protod, no cow
        """
        with self.clone_no_cow():
            pid = ctx.cluster.execute(['pgrep', '-f', 'pd-protod']).stdout.split('\n')[0]
            ctx.cluster.execute(['kill', '-9', pid])

    @attr(jira='next', complexity=4, name='clone_hardboot_cow')
    def _test_clone_clone_hardboot_cow(self, setup_env):
        """
        Test - clones a file, starts cow, hard reboot
        """
        with self.clone_cow():
            ctx.cluster.force_reboot()

    @attr(jira='next', complexity=4, name='clone_reboot_cow')
    def _test_clone_clone_reboot_cow(self, setup_env):
        """
        Test - clones a file, starts cow, reboot
        """
        with self.clone_cow():
            ctx.cluster.reboot()
