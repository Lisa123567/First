#__author__ = 'mleopold'
# -*- coding: UTF-8 -*-


##################################################################
#                                                                #
# This is the Phase-1 data mobility test suite                   #
#                                                                #
##################################################################

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
import uuid
import random
import os
import time
import gevent
from pd_common.helper import Helper as hlp
from tonat.greenlet_management import GreenletEx
from tonat.errors import Fatal
from pd_tools.fio_caps import FioCaps
from pd_tools.fsx_caps import FsxCaps
from pytest import yield_fixture
from contextlib import contextmanager
from client.models.ObjectType import ObjectType
from pprint import pformat


TIMEOUT = 180
COPY_TIMEOUT = 600
MAX_TIMEOUT = 14400
DI_DME_TIMEOUT = 30
DELETE_SLEEP = 21
DI_SERVICE = 'pd-di'
DI_BINARY = 'data_instantiator'
DI_SYSTEMD = '/usr/lib/systemd/system/pd-di.service'
DMEFILE = '/opt/pd/pddm/conf/configuration.ini'
# deprecated
CLASSIFIER = 'special_classifier'
DATAPROFILE = 'special_dp1'
DATAPROFILE2 = 'special_dp2'
MAINPATH = '/pd/dcns'
MDFSPATH = MAINPATH + '/mdfs'
OBSPATH = MAINPATH + '/obs'
EXPORTSPATH = MAINPATH + '/exports'
MOVECOMBS = [[['A'], ['A']],
             [['A'], ['B']],
             [['A'], ['A', 'A']],
             [['A'], ['A', 'B']],
             [['A'], ['B', 'A']],
             [['A'], ['B', 'B']],
             [['A'], ['B', 'C']],
             [['A', 'A'], ['A']],
             [['A', 'A'], ['B']],
             [['A', 'A'], ['A', 'A']],
             [['A', 'A'], ['A', 'B']],
             [['A', 'A'], ['B', 'A']],
             [['A', 'A'], ['B', 'B']],
             [['A', 'A'], ['B', 'C']],
             [['A', 'B'], ['A']],
             [['A', 'B'], ['B']],
             [['A', 'B'], ['C']],
             [['A', 'B'], ['A', 'A']],
             [['A', 'B'], ['A', 'B']],
             [['A', 'B'], ['B', 'A']],
             [['A', 'B'], ['B', 'B']],
             [['A', 'B'], ['B', 'C']],
             [['A', 'B'], ['A', 'C']],
             [['A', 'B'], ['C', 'B']],
             [['A', 'B'], ['C', 'A']],
             [['A', 'B'], ['C', 'D']]]
MOVECOMBS_STATIC = [[['A'],['A']],
                    [['A', 'A'],['A', 'A']],
                    [['A', 'B'],['A', 'B']],
                    [['A', 'B'],['B', 'A']]]

@yield_fixture(autouse=True, scope='session')
def no_grader():
    ctx.cluster.edit_conf(DMEFILE, kwargs={'disableMobilityDecisions ': ' true'})
    time.sleep(10)
    yield
    ctx.cluster.edit_conf(DMEFILE, kwargs={'disableMobilityDecisions ': ' false'})


class MoveConfiguration():
    
    def __init__(self, sources, targets, pdds=False):
        assert len(sources) >= 1 and len(sources) <= 2, 'invalid sources for a move configuration'
        assert len(targets) >= 1 and len(targets) <= 2, 'invalid targets for a move configuration'
        self.sources = list(sources)
        self.targets = list(targets)
        self.s_labels = []
        self.t_labels = []
        self._get_labels()
        

class TestMobility(TestBase):

    @yield_fixture
    def setup_env(self, get_mount_points):
        self.mounts = get_mount_points
        self.mount = self.mounts[0]
        self.client = ctx.clients[0]
#         debug_nodes = ctx.clients + ctx.data_portals
#         debug_nodes.execute(['rpcdebug', '-m', 'nfs', '-s', 'all'])
        yield
#         debug_nodes.execute(['rpcdebug', '-m', 'nfs', '-c', 'all'])

    @yield_fixture    
    def nfs3_fixture(self, setup_env):
        """
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

    def create_random_sparse(self, size=100, seed=None):
        """
        Creates a randomly partially sparse file
        """
        if seed == None:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(1013), '-s', '1'])
        else:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(1013), '-s', '1'], seed=seed)
        self.client.execute_tool(fsx, self.mount)
        name = self.client.execute(['find', self.mount.path, '-name', fsx._data]).stdout.split("\n")[0]
        self.client.execute(['dd', 'if=/dev/urandom', 'of=' + name, 'bs=10M', 'oflag=direct', 'seek=1', 'count=1'])
        return name              

    def get_client_file_md5(self, client, cfile):
        res = client.execute(['md5sum', '-b', '%s' % cfile])
        sig = res.stdout.split('\n')[0].split()[0]
        return sig

    def data_mobility_scenario(self, scenario='basic', size=10, name=None, timeout=COPY_TIMEOUT, 
                               sparse=False, io=False, quick=False, seed=None, verify=True):
        # create a file on a share
        if name == None:
            fname = self.mounts[0].path + '/' + str(uuid.uuid4())
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=1', 'oflag=direct'], timeout=1200)
        else:
            fname = self.mounts[0].path + '/' + name
        
        # get file details (inode, gen number, etc.)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
        source_instances = [instance for instance in file_details.instances]
        source_obses = [instance.data_store for instance in source_instances]
        obs_file = file_details.instances[0].path
        
        # create a file on a share with a size of 1GB with random content
        if not io:
            if not sparse and not quick:
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=%d' % size, 'oflag=direct'], timeout=1200)
            elif not sparse:
                seed = random.getrandbits(128) 
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    ctx.clients[0].execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                                            'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                            '-nosalt', '|', 'dd', 'of=%s' % fname, 'bs=1M', 
                                            'count=%d' % size, 'oflag=direct', 'iflag=fullblock'], timeout=1200)
            else:
                fname = self.create_random_sparse(size=size, seed=seed)
                info1 = self.client.file_declare(fname, self.mounts[0].share)
                instances1 = [instance for instance in info1.instances]
                obses1 = [instance.data_store for instance in instances1]
                mnt1 = self.mntdirs[obses1[0].internal_id]
                file1 = '/'.join(instances1[0].path.split('/')[2:])
                # TODO: put back the assertion when PD-17010 is fixed
                blocks1 = self.client.execute(['stat', '-c', '\"%b\"', mnt1 + '/' + file1]).stdout.split("\n")[0]

            # get md5sum of the file
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                sig = self.get_client_file_md5(ctx.clients[0], fname)

        # get a target DS
        dd_obses = dict(ctx.cluster.data_stores).values()
        assert len(dd_obses) > 1, 'Cannot run this test when the number of dses on dd < 2'
        target_obs = random.choice([ds for ds in dd_obses if ds not in source_obses])

        # invoke file move to a specific DS
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__, target_obs=target_obs):
            ctx.cluster.dmc.enqueue_mobility_job(file_details.obj_id, self.mounts[0].share, [target_obs])

        # poll get file info till only the source DS disappears, when that happens take a timestamp
        if verify:
            t2 = t1 = time.time()
            while (t2 - t1  < timeout):
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    file_details2 = ctx.cluster.get_file_info(share=self.mounts[0].share, inode=file_details.inode)   
                if set([target_obs]) == set([inst.data_store for inst in file_details2.instances]):
                    break
                gevent.sleep(1)
                t2 = time.time()
                self.dbg_assert(t2 - t1 < timeout, 'the move process took more than %d seconds' % timeout,
                                fname=fname, file_details=file_details2.__dict__, share=self.mount.share)

        # fail in case of timeout, else get another md5sum for the file and compare with the previous run
        if not io:
            # TODO: METADATA check - need to add it
            
            ctx.clients[0].drop_caches()
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                sig2 = self.get_client_file_md5(ctx.clients[0], fname)
            self.dbg_assert(sig == sig2, 
                            'the data of the file that had been moved is corrupted',
                            fatal=True,
                            fname=fname,
                            file_details=file_details2.__dict__,
                            original_sig=sig,
                            new_sig=sig2, share=self.mount.share)
            # TODO: put back the assertion when PD-17010 is fixed
            if sparse:
                info2 = self.client.file_declare(fname, self.mounts[0].share)
                instances2 = [instance for instance in info2.instances]
                obses2 = [instance.data_store for instance in instances2]
                mnt2 = self.mntdirs[obses2[0].internal_id]
                file2 = '/'.join(instances2[0].path.split('/')[2:])
                blocks2 = self.client.execute(['stat', '-c', '\"%b\"', mnt2 + '/' + file2]).stdout.split("\n")[0]
                rsd = hlp.get_rsd([float(blocks1), float(blocks2)])
                assert rsd <= 0.05, "the rsd between the two block counts is larger than 0.05"

#             self.mounts[0].share.verify_mddb_coherency()
        
        # verify that there is no file in the real location anymore
        # TODO: make this last check run via mount v3 of the ds

        if verify:
            # wait 15s to let DME delete the instance
            gevent.sleep(DELETE_SLEEP)
            source_obs_ip = source_obses[0].node.mgmt_ip_address.address
            physical_ds = ctx.get_ds('address', source_obs_ip)
            assert not physical_ds.exists(obs_file), 'the source file was not removed from the source obs'
            self._logger.info('one move passed')
        return fname      

    @attr('data_mobility', jira='PD-3188', complexity=1, name='data_mobility_basic')
    def test_data_mobility_basic(self, setup_env):
        """
        Test - moves a file, no IO
        """
        self.data_mobility_scenario(quick=True)

    @attr('data_mobility', jira='PD-3189', complexity=1, name='data_mobility_unicode')
    def test_data_mobility_unicode(self, setup_env):
        """
        Test - moves a file, unicode name
        """
        name = 'אם תרצו אין זו אגדה'
        fname = self.mounts[0].path + '/' + name
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=1', 'oflag=direct'], timeout=1200)
        self.data_mobility_scenario(name=name, quick=True)

    @attr('data_mobility', jira='PD-3190', complexity=1, name='data_mobility_large')
    def test_data_mobility_large(self, setup_env):
        """
        Test - moves a 2G file, no IO
        """
        self.data_mobility_scenario(size = 2048, quick=True)

    @attr('data_mobility', jira='PD-4358', complexity=1, name='data_mobility_larger')
    def test_data_mobility_larger(self, setup_env):
        """
        Test - moves a 4G file, no IO
        """
        self.data_mobility_scenario(size = 4096, quick=True)

# became irrelevant       
#     @attr(jira='PD-3191', complexity=1, name='data_mobility_same_obs')
#     def test_data_mobility_same_obs(self, setup_env):
#         self.data_mobility_scenario('same_obs')

# became irrelevant        
#     @attr(jira='PD-3192', complexity=1, name='data_mobility_non_existent_obs')
#     def test_data_mobility_non_existent_obs(self, setup_env):
#         self.data_mobility_scenario('non_existent_obs')
#         ctx.cluster.get_pid_by_proc_name('pd-data-mover')
#         ctx.cluster.get_pid_by_proc_name('pd-data-mgr')


# CSM version of this test
#     @attr(jira='PD-3193', complexity=1, name='data_mobility_serial_moves_same_file')
#     def test_data_mobility_serial_moves_same_file(self, setup_env):
#         fbase = str(uuid.uuid4())
#         fname = self.mounts[0].path + '/' + fbase        
#         combs = list(MOVECOMBS)
#         ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=20', 'oflag=direct'], timeout=1200)
#         for _i in xrange(18):
#             chosen = random.choice(combs)
#             mc = MoveConfiguration(chosen[0], chosen[1])
#             self.csm_move(fbase, mc, create_file=False)
#         ctx.clients[0].remove(fname)

    @attr('data_mobility', jira='PD-3193', complexity=1, name='data_mobility_serial_moves_same_file')
    def test_data_mobility_serial_moves_same_file(self, setup_env):
        """
        Test - moves a file 18 times in a row
        """
        fbase = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + fbase        
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=20', 'oflag=direct'], timeout=1200)
            ctx.clients[0].execute(['cp', '-p', fname, '/tmp/original'])
        for _i in xrange(18):
            self.data_mobility_scenario(name=fbase, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            ctx.clients[0].remove(fname)
        
# became irrelevant        
#    @attr(jira='PD-3194', complexity=1, name='data_mobility_obs_nocreate')
#    def test_data_mobility_obs_nocreate(self, setup_env):
#        self.data_mobility_scenario('obs_nocreate')

# became irrelevant        
#    @attr(jira='PD-3195', complexity=1, name='data_mobility_shard_nocreate')
#    def test_data_mobility_shard_nocreate(self, setup_env):
#        self.data_mobility_scenario('shard_nocreate')

# deprecated test
#     @attr(jira='PD-3330', complexity=1, name='data_mobility_non_existent_file')
#     def test_data_mobility_non_existent_file(self, setup_env):
#         share_path = '%s/%s' % (EXPORTSPATH, self.mounts[0].share.name)
#         share_uuid = self.get_bag_uuid(share_path)
#         ddfile = '%s/%s/non_existent_file' % (MDFSPATH, share_uuid)
#         dd_obses = self.get_dd_obses()
#         assert len(dd_obses.keys()) > 1, 'this test requires at least two obses'
#         source_obs = random.choice(dd_obses.keys())
#         
#         target = self.get_target_label(source_obs)
#         self.move_by_rebalance(target)
#         
#         ctx.cluster.get_pid_by_proc_name('pd-data-mover')
#         ctx.cluster.get_pid_by_proc_name('pd-data-mgr')

    @attr('data_mobility', jira='PD-3196', complexity=1, name='data_mobility_sparse')
    def test_data_mobility_sparse(self, nfs3_fixture):
        """
        Test - moves a file with random holes, verifies its data consumption didn't change by much
        """
        self.data_mobility_scenario(sparse = True, size = 400)

    @attr('data_mobility', jira='PD-16955', complexity=1, name='data_mobility_sparse_deter')
    def test_data_mobility_sparse_deter(self, nfs3_fixture):
        """
        Test - moves a file with random holes (static seed), verifies its data consumption didn't change by much
        """
        self.data_mobility_scenario(sparse = True, size = 400, seed=31)

    def client_full_copy_random(self, source,  target, bs, count, oflag=None):
        blocks = {}
        for i in xrange(count):
            blocks[str(i)] = True
        while blocks:
            block = random.choice(blocks.keys())
            with self.dbg_wrapper(source_file=source, target_file=target, share=self.mount.share):
                if oflag == None:
                    ctx.clients[0].execute(['dd', 'if=%s' % source, 'of=%s' % target, 'bs=%s' % bs, 'count=1', 'skip=%s' % block, 'seek=%s' % block, 'conv=notrunc', '&'])
                else:
                    ctx.clients[0].execute(['dd', 'if=%s' % source, 'of=%s' % target, 'bs=%s' % bs, 'count=1', 'skip=%s' % block, 'seek=%s' % block, 'oflag=%s' % oflag, 'conv=notrunc', '&'])
            del blocks[block]
            if len(blocks) % 100 == 0:
                self._logger.info("%d blocks left" % len(blocks)) 
        # pgrep -f 'dd if'
        t2 = t1 = time.time()
        while (t2 - t1  < MAX_TIMEOUT):
            try:
                ctx.clients[0].execute(['pgrep', '-f', 'dd if=%s' % source])
            except:
                break
            gevent.sleep(1)
            t2 = time.time()
        self.dbg_assert(t2 - t1 < MAX_TIMEOUT, 'it seems that the random copy hangs', timeout_in_sec=MAX_TIMEOUT,
                        source_file=source, target_file=target, share=self.mount.share)

        
    # TODO: implement the "quick" functionality for IO tests
    def data_mobility_io_basic(self, scenario = 'basic', oflag = None, non_append = False, aligned=True, quick=False):
        MEGA = 1024 * 1024
        if aligned:
            SIZE = 2000 * 1024 * 1024
            BS = 64 * 1024
            COUNT = SIZE / BS
        else:
            SIZE = 189 * 1024 * 1024
            BS = 63 * 1024
            COUNT = SIZE / BS
        
        # generate a big ddfile in /tmp
        tmpfile = '/root/lala'
        if (not quick):
            ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % tmpfile, 'bs=%d' % MEGA, 'count=%d' % (SIZE / MEGA)])
            ctx.clients.drop_caches()
        else:
            seed = random.getrandbits(128) 
            ctx.clients[0].execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % (SIZE / MEGA), '|',
                                    'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                    '-nosalt', '|', 'dd', 'of=%s' % tmpfile, 'bs=1M', 
                                    'count=%d' % (SIZE / MEGA), 'oflag=direct', 'iflag=fullblock'], timeout=1200)
        # calculate md5sum
        md5sum = self.get_client_file_md5(ctx.clients[0], tmpfile)
        # copy the file to the share on background, get the pid
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        if non_append:
            with self.dbg_wrapper(fname=fname, share=self.mount.share):        
                ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=%d' % BS, 'count=%d' % COUNT, 'oflag=direct'])
        if oflag == None:
            if scenario == 'random_write':
                thread = GreenletEx.spawn(self.client_full_copy_random, tmpfile, fname, str(BS), COUNT)
            else:
                with self.dbg_wrapper(fname=fname, share=self.mount.share):        
                    if non_append:
                        thread = ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=%d' % BS, 'count=%d' % COUNT, 'conv=notrunc'], block=False, timeout=1800)
                    else:
                        thread = ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=%d' % BS, 'count=%d' % COUNT], block=False, timeout=1800)
        else:
            if scenario == 'random_write':
                thread = GreenletEx.spawn(self.client_full_copy_random, tmpfile, fname, str(BS), COUNT, 'direct')
            else:
                with self.dbg_wrapper(fname=fname, share=self.mount.share):        
                    if non_append:
                        thread = ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=%d' % BS, 'count=%d' % COUNT, 'oflag=%s' % oflag, 'conv=notrunc'], block=False, timeout=1800)
                    else:
                        thread = ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=%d' % BS, 'count=%d' % COUNT, 'oflag=%s' % oflag], block=False, timeout=1800)
        # verify that the copy completed
        time.sleep(1)
        file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
        while not thread.ready():
            # perform a basic move
            self.data_mobility_scenario(name=name, io=True)
            time.sleep(1)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get(timeout=1)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            ctx.clients.drop_caches()
            # verify md5sum
            res = ctx.clients[0].execute(['md5sum', '-b', '%s' % fname])
        md5sum2 = res.stdout.split('\n')[0].split()[0]
        self.dbg_assert(md5sum == md5sum2, 'the copied file got corrupted', fatal=True, fname=fname,
                        file_details=file_details.__dict__, share=self.mount.share)
        # remove the temp file
        #ctx.clients[0].remove('%s' % tmpfile)
        ctx.clients[0].remove(fname)
#         self.mounts[0].share.verify_mddb_coherency()

    @attr('data_mobility', jira='PD-3331', complexity=1, name='data_mobility_io_basic')
    def test_data_mobility_io_basic(self, setup_env):
        """
        Test - moves a file under IO
        """
        self.data_mobility_io_basic()

    @attr('data_mobility', jira='PD-3332', complexity=1, name='data_mobility_io_basic_non_append')
    def test_data_mobility_io_basic_non_append(self, setup_env):
        """
        Test - copies a file to the share, overrides the file and moves it at the same time
        """
        self.data_mobility_io_basic(non_append=True)

    @attr('data_mobility', jira='PD-3333', complexity=1, name='data_mobility_io_random_write')
    def test_data_mobility_io_random_write(self, setup_env):
        """
        Test - copies a file to the share and moves it at the same time, copy is done by random multiple 64KB chunks
        """
        self.data_mobility_io_basic('random_write')

    @attr('data_mobility', jira='PD-3442', complexity=1, name='data_mobility_io_random_write_non_aligned')
    def test_data_mobility_io_random_write_non_aligned(self, setup_env):
        """
        Test - copies a file to the share and moves it at the same time, copy is done by random multiple 63KB chunks
        """
        self.data_mobility_io_basic('random_write',oflag='direct',aligned=False)

    @attr('data_mobility', jira='PD-3334', complexity=1, name='data_mobility_io_basic_direct')
    def test_data_mobility_io_basic_direct(self, setup_env):
        """
        Test - copies a file to the share and moves it at the same time (O_DIRECT)
        """
        self.data_mobility_io_basic(oflag='direct', quick=True)

    @attr('data_mobility', jira='PD-3443', complexity=1, name='data_mobility_io_basic_direct_non_aligned')
    def test_data_mobility_io_basic_direct_non_aligned(self, setup_env):
        """
        Test - copies a file to the share and moves it at the same time, copy is done by multiple 63KB chunks
        """
        self.data_mobility_io_basic(oflag='direct',aligned=False, quick=True)

    @attr('data_mobility', jira='PD-3335', complexity=1, name='data_mobility_io_basic_direct_non_append')
    def test_data_mobility_io_basic_direct_non_append(self, setup_env):
        """
        Test - copies a file to the share, overrides the file and moves it at the same time (O_DIRECT)
        """
        self.data_mobility_io_basic(oflag='direct', non_append=True)

    @attr('data_mobility', jira='PD-3336', complexity=1, name='data_mobility_io_open_rw')
    def test_data_mobility_io_open_rw(self, setup_env):
        """
        Test - moves a file when there are outstanding RO + RW layouts, does IO
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        client = ctx.clients[0]
        try:
            fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            client.write_file(fd, 0, 32, 2)
            client.close_file(fd)
            hlp.wait_for_no_layouts(fname, self.mount.share, timeout=240)
            fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            client.read_file(fd, 0, 32, 2)
            client.write_file(fd, 64, 32, 2)
            file_details = client.file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.ro_layouts, "there are no outstanding ro layouts",
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            self.dbg_assert(file_details.rw_layouts, "there are no outstanding rw layouts",
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            self.data_mobility_scenario(name=name, io=True)
            client.drop_caches()
            with self.dbg_wrapper(fname=fname, file_details=file_details.__dict__,
                                  share=self.mount.share):
                client.write_file(fd, 128, 32, 2)
                client.read_file(fd, 0, 32, 2)
                client.read_file(fd, 64, 32, 2)
                client.read_file(fd, 128, 32, 2)
                client.close_file(fd)
        finally:
            client.close_agent()

    @attr('data_mobility', jira='PD-3337', complexity=1, name='data_mobility_io_open_ro')
    def test_data_mobility_io_open_ro(self, setup_env):
        """
        Test - moves a file when there is an outstanding RO layout, does IO
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        client = ctx.clients[0]
        try:
            fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            client.write_file(fd, 0, 32, 2)
            client.write_file(fd, 64, 32, 2)
            client.close_file(fd)
            hlp.wait_for_no_layouts(fname, self.mount.share, timeout=240)
            fd = client.open_file(fname, os.O_CREAT | os.O_RDONLY, 0644)
            client.read_file(fd, 0, 32, 2)
            file_details = client.file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.ro_layouts, "there are no outstanding ro layouts",
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            self.dbg_assert(not file_details.rw_layouts, "there are outstanding rw layouts",
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            self.data_mobility_scenario(name=name, io=True)
            client.drop_caches()
            with self.dbg_wrapper(fname=fname, file_details=file_details.__dict__,
                                  share=self.mount.share):
                client.read_file(fd, 64, 32, 2)
                client.close_file(fd)
        finally:
            client.close_agent()

    @attr('data_mobility', jira='PD-3338', complexity=1, name='data_mobility_io_open_wr')
    def test_data_mobility_io_open_wr(self, setup_env):
        """
        Test - moves a file when there is an outstanding RW layout, does IO
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        client = ctx.clients[0]
        try:
            fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            client.write_file(fd, 0, 32, 2)
            client.write_file(fd, 64, 32, 2)
            client.close_file(fd)
            hlp.wait_for_no_layouts(fname, self.mount.share, timeout=240)
            fd = client.open_file(fname,  os.O_WRONLY | os.O_LARGEFILE | os.O_SYNC, 0644)
            client.write_file(fd, 1024, 32, 1)
            file_details = client.file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.rw_layouts, "there are no outstanding rw layouts",            
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            self.data_mobility_scenario(name=name, io=True)
            client.write_file(fd, 2048, 32, 1)
            client.close_file(fd)
            client.drop_caches()
            fd = client.open_file(fname, os.O_RDWR | os.O_SYNC, 0644)
            with self.dbg_wrapper(fname=fname, file_details=file_details.__dict__, share=self.mount.share):
                client.read_file(fd, 0, 32, 2)
                client.read_file(fd, 64, 32, 2)
                client.read_file(fd, 1024, 32, 1)
                client.read_file(fd, 2048, 32, 1)
                client.close_file(fd)
        finally:
            client.close_agent()

    @attr('data_mobility', jira='PD-15104', complexity=1, name='data_mobility_io_short_read')
    def test_data_mobility_io_short_read(self, setup_env):
        """
        Test - does a short read while in mobility
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        fname2 = self.mounts[0].path + '/' + name + '_2'
        fname3 = self.mounts[0].path + '/' + name + '_3'
        client = ctx.clients[0]
        bs = 1048576
        count = 1024
        skip = bs * count - 4096
        # generate a file
        client.execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=%d' % bs, 'count=%d' % count])
        client.execute(['dd', 'if=%s' % fname, 'of=%s' % fname2, 'skip=%d' % skip, 
                        'iflag=skip_bytes', 'iflag=direct', 'bs=%d' % bs, 'count=1'])
        file_details = client.file_declare(fname, self.mounts[0].share)
        ctx.clients.drop_caches()
        self.data_mobility_scenario(name=name, io=True)
        time.sleep(5)
        client.execute(['dd', 'if=%s' % fname, 'of=%s' % fname3, 'skip=%d' % skip, 
                        'iflag=skip_bytes', 'iflag=direct', 'bs=%d' % bs, 'count=1'])
        ctx.clients.drop_caches()
        self.dbg_assert(client.compare_files_md5(fname2, fname3), 'data corruption', fatal=True,
                        fname=fname, file_details=file_details.__dict__, share=self.mount.share)
        size2 = int(client.execute(['stat', '-c', '%s', fname2]).stdout.split('\n')[0])
        self.dbg_assert(size2 == 4096, 'incorrect size', fname=fname, 
                        file_details=file_details.__dict__, share=self.mount.share)
        size3 = int(client.execute(['stat', '-c', '%s', fname3]).stdout.split('\n')[0])
        self.dbg_assert(size3 == 4096, 'incorrect size', fname=fname,
                        file_details=file_details.__dict__, share=self.mount.share)

    @attr('data_mobility', jira='PD-3339', complexity=1, name='data_mobility_io_open_trunc')
    def test_data_mobility_io_open_trunc(self, setup_env):
        """
        Test- opens the file with O_TRUNC while in mobility
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        client = ctx.clients[0]        
        client.execute(['dd', 'if=/dev/zero', 'of=%s' % fname, 'bs=1M', 'count=2048', 'conv=fsync'])
        # move the file
        file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
        try:
            thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True)
            time.sleep(5)
            # try to truncate the file while it is being moved
            fd = client.open_file(fname, os.O_TRUNC | os.O_RDWR | os.O_SYNC, 0644)
            # verify that the file has being truncated
            client.drop_caches()
            file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.size == 0, 'the file wasn\'t truncated',
                         fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            client.close_file(fd)
            thread.get(timeout=DELETE_SLEEP+10)     
        finally: 
            client.close_agent()

    @attr('data_mobility', jira='PD-13052', complexity=1, name='data_mobility_io_trunc_up')
    def test_data_mobility_io_trunc_up(self, setup_env):
        """
        Test - trucates a file upwards while is mobility 
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        client = ctx.clients[0]        
        client.execute(['dd', 'if=/dev/zero', 'of=%s' % fname, 'bs=1M', 'count=2048', 'conv=fsync'])
        # move the file
        file_details = client.file_declare(fname, self.mounts[0].share)
        new_size = file_details.size + random.randint(1, 131072)
        try:
            fd = client.open_file(fname, os.O_RDWR | os.O_SYNC, 0644)
            thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True)
            time.sleep(5)
            # try to truncate the file up while it is being moved
            #client.truncate_file(fd, new_size) - dft's truncate is no good?
            client.execute(['truncate', '-s', str(new_size), fname])
            # verify that the file has being truncated
            client.drop_caches()
            file_details = client.file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.size == new_size, 'expected size %d got %d'  % (new_size, file_details.size),
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            client.close_file(fd)
            thread.get(timeout=120)     
            client.drop_caches()
            file_details = client.file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.size == new_size, 'expected size %d got %d'  % (new_size, file_details.size),
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
        finally: 
            client.close_agent()

    @attr('data_mobility', jira='PD-13053', complexity=1, name='data_mobility_io_truc_down')
    def test_data_mobility_io_trunc_down(self, setup_env):
        """
        Test - truncates a file downwards while in mobility
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        client = ctx.clients[0]        
        client.execute(['dd', 'if=/dev/zero', 'of=%s' % fname, 'bs=1M', 'count=2048', 'conv=fsync'])
        # move the file
        file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
        new_size = file_details.size - random.randint(1,131072)
        try:
            fd = client.open_file(fname, os.O_RDWR | os.O_SYNC, 0644)
            thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True)
            time.sleep(5)
            # try to truncate the file down while it is being moved
            client.truncate_file(fd, new_size)
            # verify that the file has being truncated
            client.drop_caches()
            file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.size == new_size, 'the file has an incorrect size',
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
            client.close_file(fd)
            thread.get(timeout=2)     
            client.drop_caches()
            file_details = ctx.clients[0].file_declare(fname, self.mounts[0].share)
            self.dbg_assert(file_details.size == new_size, 'the file has an incorrect size',
                            fname=fname, file_details=file_details.__dict__, share=self.mount.share)
        finally: 
            client.close_agent()

    @attr('data_mobility', jira='PD-3340', complexity=1, name='data_mobility_io_rm')
    def test_data_mobility_io_rm(self, nfs3_fixture):
        """
        Test - deletes a file while in mobility
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        size = 2000
        seed = random.getrandbits(128)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            self.client.execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                                    'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                    '-nosalt', '|', 'dd', 'of=%s' % fname, 'bs=1M', 
                                    'count=%d' % size, 'oflag=direct', 'iflag=fullblock'], timeout=1200)
            info1 = self.client.file_declare(fname, self.mounts[0].share)
        thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True, verify=False)
        time.sleep(15)
        # try to delete the file while it is being moved
        self.client.remove(fname)
        time.sleep(DELETE_SLEEP)
        # verify that the file was deleted from the namespace
        self.dbg_assert(not self.client.exists(fname), 'the file wasn\'t deleted',
                        fname=fname, file_details=info1.__dict__, share=self.mount.share)
        # verify that the file was deleted from the DS
        instances1 = [instance for instance in info1.instances]
        obses1 = [instance.data_store for instance in instances1]
        mnt1 = self.mntdirs[obses1[0].internal_id]
        file1 = '/'.join(instances1[0].path.split('/')[2:])
        self.dbg_assert(not self.client.exists(mnt1 + '/' + file1), 'the ds file wasn\'t deleted',
                        fname=fname, ds_fname=mnt1+'/'+file1, file_details=info1.__dict__, share=self.mount.share)
        # mobility should be aborted
        thread.get(timeout=60)

    @attr('data_mobility', jira='PD-3341', complexity=1, name='data_mobility_io_mv')
    def test_data_mobility_io_mv(self, setup_env):
        """
        Test - renames a file while in mobility
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        fname2 = self.mounts[0].path + '/' + str(uuid.uuid4())
        # create a large file
        client = ctx.clients[0]
        try:
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(2048):
                    client.write_file(fd, i * 2048, 2048, 2)
                client.close_file(fd)
            # move the file
            thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True)
            time.sleep(5)            
            # try to rename the file while it is being moved
            with self.dbg_wrapper(fname=fname, fname2=fname2, share=self.mount.share):
                client.rename(fname, fname2)
            # verify that the file was renamed and that it's not corrupted            
            self.dbg_assert(client.exists(fname2), 'the file wasn\'t renamed',
                            fname=fname, fname2=fname2, share=self.mount.share)
            self.dbg_assert(not client.exists(fname), 'the file got duplicated',
                            fname=fname, fname2=fname2, share=self.mount.share)
            client.drop_caches()
            with self.dbg_wrapper(fname=fname, fname2=fname2, share=self.mount.share):
                fd = client.open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(2048):
                    client.read_file(fd, i * 2048, 2048, 2)
                client.close_file(fd)
                thread.get(timeout=240)
        finally:
            client.close_agent()

    @attr('data_mobility', jira='PD-13054', complexity=1, name='data_mobility_io_obj')
    def test_data_mobility_io_obj(self, setup_env):
        """
        Test - change objectives while in mobility
        """
        name = str(uuid.uuid4())
        bo = 'basic_obj_002'
        fname = self.mounts[0].path + '/' + name
        if (bo not in ctx.cluster.basic_objectives):
            ctx.cluster.cli.basic_objective_create(name=bo, min_write_iops=0)
        # create a large file
        client = ctx.clients[0]        
        try:
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(2048):
                    client.write_file(fd, i * 2048, 2048, 2)
                client.close_file(fd)
            # move the file
            thread = GreenletEx.spawn(self.data_mobility_scenario, name=name, io=True)
            time.sleep(5)            
            # try to change the objective while it is being moved
            ctx.cluster.cli.share_objective_add(name=self.mounts[0].share.name, path='/%s' % name, objective=bo)
            # verify that the file is not corrupted            
            client.drop_caches()
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(2048):
                    client.read_file(fd, i * 2048, 2048, 2)
                client.close_file(fd)
            thread.get(timeout=300)
            client.drop_caches()
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(2048):
                    client.read_file(fd, i * 2048, 2048, 2)
                client.close_file(fd)
        finally:
            client.close_agent()

    @attr('data_mobility', jira='PD-14334', complexity=1, name='data_mobility_io_no_layouts')
    def test_data_mobility_io_no_layouts(self, setup_env):
        """
        Test - trigger mobility when there are no outstanding layouts
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        client = ctx.clients[0]        
        try:
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(1024):
                    client.write_file(fd, i * 1024, 1024, 2)
                client.close_file(fd)
                hlp.wait_for_no_layouts(fname, self.mount.share, timeout=240)
            # move the file
            self.data_mobility_scenario(name=name, io=True)
            time.sleep(5)            
            # verify that the file is not corrupted
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                client.drop_caches()
                fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
                for i in xrange(1024):
                    client.read_file(fd, i * 1024, 1024, 2)
                client.close_file(fd)
        finally:
            client.close_agent()


    def create_fio_conf_file(self, filename, iops=10, period=60, rwmixread=0, bsrange="4K-4K"):
        f = "/tmp/fio.conf"
        contents = ["\[global\]", "time_based", "runtime=%ss" % str(period), "rw=randrw",
                    "direct=1", "ioengine=libaio", "bsrange=%s" % bsrange, "rwmixread=%s" % rwmixread, "iodepth=64",
                    "\[job1\]", "name=job1", "filename=%s" % filename, "size=100M",
                    "rate_iops=%s" % str(iops)]
        if self.client.exists(f) and self.client.isfile(f):
            self.client.remove(f)
        for line in contents:
            self.client.execute(["echo", line, ">>", f])
        return f

    @attr('data_mobility', jira='PD-14150', complexity=1, name='data_mobility_fio')
    def test_data_mobility_fio(self, setup_env):
        """
        Test - moves a file while fio is running
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        f = self.create_fio_conf_file(fname, 150, 300)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        gevent.sleep(15)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        # move the file
        while not thread.ready():
            self.data_mobility_scenario(name=name, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get()

    @attr('data_mobility', jira='PD-14581', complexity=1, name='data_mobility_fio_write')
    def test_data_mobility_fio_write(self, setup_env):
        """
        Test - moves a file while fio is running
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        f = self.create_fio_conf_file(fname, 150000, 300)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        gevent.sleep(15)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        # move the file
        while not thread.ready():
            self.data_mobility_scenario(name=name, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get()

    @attr('data_mobility', jira='PD-14582', complexity=1, name='data_mobility_fio_rw')
    def test_data_mobility_fio_rw(self, setup_env):
        """
        Test - moves a file while fio is running
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        f = self.create_fio_conf_file(fname, 150, 300, 50)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        gevent.sleep(15)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        # move the file
        while not thread.ready():
            self.data_mobility_scenario(name=name, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get()

    @attr('data_mobility', jira='PD-14636', complexity=1, name='data_mobility_fio_write_var_bs')
    def test_data_mobility_fio_write_var_bs(self, setup_env):
        """
        Test - moves a file while fio is running
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        f = self.create_fio_conf_file(fname, 2000, 300, 50, "4K-1024K")
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        gevent.sleep(15)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        # move the file
        while not thread.ready():
            self.data_mobility_scenario(name=name, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get()

    @attr('data_mobility', jira='PD-14637', complexity=1, name='data_mobility_fio_rw_var_bs')
    def test_data_mobility_fio_rw_var_bs(self, setup_env):
        """
        Test - moves a file while fio is running
        """
        name = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + name
        # create a large file
        f = self.create_fio_conf_file(fname, 2000, 300, 50, "4K-1024K")
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        gevent.sleep(15)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        # move the file
        while not thread.ready():
            self.data_mobility_scenario(name=name, io=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__):
            thread.get()


# maybe we should add this back some day
#     @attr(jira='PD-3342', complexity=1, name='data_mobility_pers_dmv_down')
#     def test_data_mobility_pers_dmv_down(self, setup_env):
#         self.mounts = get_pnfs_mount_points
#         fname = self.mounts[0].path + '/' + str(uuid.uuid4())
#         # create a large file
#         client = ctx.clients[0]
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.write_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         # move the file
#         share_path = '%s/%s' % (EXPORTSPATH, self.mounts[0].share.name)
#         share_uuid = self.get_bag_uuid(share_path)
#         ddfile = '%s/%s/%s' % (MDFSPATH, share_uuid, os.path.basename(fname))
#         dd_obses = self.get_dd_obses()
#         assert len(dd_obses.keys()) > 1, 'this test requires at least two obses'
#         file_details = ctx.cluster.file_declare(ddfile)
#         source_obs = file_details.obses[0][0]
#         del dd_obses[source_obs]
#         target_obs = random.choice(dd_obses.keys())
#         self.move_dd_file(ddfile, source_obs, target_obs)
#         # stop the dmv
#         self.stop_dmv(force=True)
#         time.sleep(60)
#         self.stop_dmgr()
#         # verify that the file is stucked in the middle
#         file_details = ctx.cluster.file_declare(ddfile)
#         try:
#             assert file_details.obses[0][0] == source_obs, 'the move completed before we stopped the data mover'
#             assert file_details.obses[0][1] == target_obs, 'the move did not start before we stopped the data mover'            
#             time.sleep(5)
#         except:        
#             self.start_dmv()
#             time.sleep(1)
#             self.start_dmgr()
#             raise
#         time.sleep(5)
#         self.start_dmv()
#         time.sleep(1)
#         self.start_dmgr()
#         # rerun the move
#         self.move_dd_file(ddfile, source_obs, target_obs)
#         # verify that the move completes
#         t1 = t2 = time.time()
#         while time.time() - t1 < COPY_TIMEOUT: 
#             file_details = ctx.cluster.file_declare(ddfile)
#             if file_details.obses[0][0] == target_obs:
#                 break
#             time.sleep(5)
#             t2 = time.time()
#         assert t2 - t1 < COPY_TIMEOUT, 'the copy did not finish within the given timeout of %d seconds' % COPY_TIMEOUT
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.read_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         client.close_agent()

# maybe we should add this back some day
#     @attr(jira='PD-3343', complexity=1, name='data_mobility_pers_dmgr_down')
#     def test_data_mobility_pers_dmgr_down(self, setup_env):
#         self.mounts = get_pnfs_mount_points
#         fname = self.mounts[0].path + '/' + str(uuid.uuid4())
#         # create a large file
#         client = ctx.clients[0]
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.write_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         # move the file
#         share_path = '%s/%s' % (EXPORTSPATH, self.mounts[0].share.name)
#         share_uuid = self.get_bag_uuid(share_path)
#         ddfile = '%s/%s/%s' % (MDFSPATH, share_uuid, os.path.basename(fname))
#         dd_obses = self.get_dd_obses()
#         assert len(dd_obses.keys()) > 1, 'this test requires at least two obses'
#         file_details = ctx.cluster.file_declare(ddfile)
#         source_obs = file_details.obses[0][0]
#         del dd_obses[source_obs]
#         target_obs = random.choice(dd_obses.keys())
#         self.move_dd_file(ddfile, source_obs, target_obs)
#         # stop the dmgr
#         time.sleep(1)
#         self.stop_dmgr(force=True)
#         time.sleep(5)
#         # verify that the file is stucked in the middle
#         file_details = ctx.cluster.file_declare(ddfile)
#         try:
#             assert file_details.obses[0][0] == source_obs, 'the move completed before we stopped the data mover'
#             assert file_details.obses[0][1] == target_obs, 'the move did not start before we stopped the data mover'
#             time.sleep(COPY_TIMEOUT)
#             self.start_dmgr()
#             # rerun the move
#             self.move_dd_file(ddfile, source_obs, target_obs)
#             # verify that the move completes
#             t1 = t2 = time.time()
#             while time.time() - t1 < COPY_TIMEOUT: 
#                 file_details = ctx.cluster.file_declare(ddfile)
#                 if file_details.obses[0][0] == target_obs:
#                     break
#                 time.sleep(5)
#                 t2 = time.time()
#             assert t2 - t1 < COPY_TIMEOUT, 'the copy did not finish within the given timeout of %d seconds' % COPY_TIMEOUT
#         except:
#             self.start_dmgr()
#             raise
#         self.start_dmgr()
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.read_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         client.close_agent()

# maybe we should add this back some day
#     @attr(jira='PD-3344', complexity=1, name='data_mobility_pers_dmgr_dmv_down')
#     def test_data_mobility_pers_dmgr_dmv_down(self, setup_env):
#         self.mounts = get_pnfs_mount_points
#         fname = self.mounts[0].path + '/' + str(uuid.uuid4())
#         # create a large file
#         client = ctx.clients[0]
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.write_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         # move the file
#         share_path = '%s/%s' % (EXPORTSPATH, self.mounts[0].share.name)
#         share_uuid = self.get_bag_uuid(share_path)
#         ddfile = '%s/%s/%s' % (MDFSPATH, share_uuid, os.path.basename(fname))
#         dd_obses = self.get_dd_obses()
#         assert len(dd_obses.keys()) > 1, 'this test requires at least two obses'
#         file_details = ctx.cluster.file_declare(ddfile)
#         source_obs = file_details.obses[0][0]
#         del dd_obses[source_obs]
#         target_obs = random.choice(dd_obses.keys())
#         self.move_dd_file(ddfile, source_obs, target_obs)
#         # stop the dmv
#         self.stop_dmgr(force=True)
#         time.sleep(1)
#         self.stop_dmv(force=True)
#         # verify that the file is stucked in the middle
#         file_details = ctx.cluster.file_declare(ddfile)
#         try:
#             assert file_details.obses[0][0] == source_obs, 'the move completed before we stopped the data mover'
#             assert file_details.obses[0][1] == target_obs, 'the move did not start before we stopped the data mover'
#             time.sleep(60)
#         except:
#             self.start_dmv()
#             time.sleep(1)
#             self.start_dmgr()
#             raise
#         self.start_dmv()
#         time.sleep(1)
#         self.start_dmgr()
#         # rerun the move
#         self.move_dd_file(ddfile, source_obs, target_obs)
#         # verify that the move completes
#         t1 = t2 = time.time()
#         while time.time() - t1 < COPY_TIMEOUT: 
#             file_details = ctx.cluster.file_declare(ddfile)
#             if file_details.obses[0][0] == target_obs:
#                 break
#             time.sleep(5)
#             t2 = time.time()
#         assert t2 - t1 < COPY_TIMEOUT, 'the copy did not finish within the given timeout of %d seconds' % COPY_TIMEOUT
#         fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#         for i in xrange(2048):
#             client.read_file(fd, i * 2048, 2048, 2)
#         client.close_file(fd)
#         client.close_agent()

#    @attr(jira='next', complexity=1, name='data_mobility_mutli_basic')
#     def _test_data_mobility_multi_basic(self, get_pnfs_mount_points):
#         self.mounts = get_pnfs_mount_points   
#         client = ctx.clients[0]
#         num_of_files = 100
#         with self.step("Creating files"):        
#             for i in xrange(num_of_files):
#                 fname = self.mounts[0].path + '/file%d' % i
#                 fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#                 for j in xrange(30):
#                     client.write_file(fd, j * 2048, 2048, (i % 3) + 1)
#                 client.close_file(fd)
#         with self.step("Rebalancing"):
#             share = ctx.cluster.cli.share_list(name=self.mounts[0].share.name)[0]
#             cls = ctx.cluster.cli.classifier_list(name=share.classifier.name)[0]
#             for rule in cls.rules:
#                 if rule.flags == Flags.CREATE or rule.flags == Flags.CREATE_DEFAULT:
#                     l = rule.label.name
#             lnames = dict(ctx.cluster.labels).keys()
#             lnames.remove(l)
#             l2 = random.choice(lnames)
#             ctx.cluster.cli.classifier_update(name=cls.name, rule=["lala,WRITE\>0/1h,%s,CREATE" % l2])
#             ctx.cluster.cli.share_rebalance(name=share.name)
#         targets = {}
#         label = ctx.cluster.labels[l2]
#         obses = [obs.id for obs in label.obses]
#         cmax = 0
#         with self.step("Monitoring"):
#             while (True):
#                 count = 0
#                 for i in xrange(num_of_files):
#                     data = ctx.cluster.file_declare('%s/%s/file%d' % (EXPORTSPATH, self.mounts[0].share.name, i))
#                     if len(data.obses[0]) > 1:
#                         # next 2 lines are for debug puposes only
#                         _obs00 = data.obses[0][0]
#                         _obs01 = data.obses[0][1]
#                         
#                         obspath = '%s/%s%s' % (OBSPATH, data.obses[0][1], data.objs[0])                       
#                         size = int(ctx.cluster.execute(['stat', '-c', '%s', obspath]).stdout.split('\n')[0].strip())
#                         if size > 0:
#                             if size < 104857600:
#                                 targets[obspath] = 1
#                             elif obspath in targets:
#                                 del targets[obspath]
#                     elif data.obses[0][0] in obses:
#                         count +=1
#                 if count == num_of_files:
#                     break
#                 elif count > cmax:
#                     cmax = count
#                 self._logger.info('Number of files being moved: %d' % len(targets))
#             assert cmax > 31, 'The max number of concurrent moves was %d < 32' % cmax
#         with self.step("Verifing"):
#             for i in xrange(num_of_files):
#                 fname = self.mounts[0].path + '/file%d' % i
#                 fd = client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
#                 for j in xrange(30):
#                     client.read_file(fd, j * 2048, 2048, (i % 3) + 1)
#                 client.close_file(fd)           
#             pass
#         client.close_agent()

    def objs_identical(self, fbase):
        finfo = self.mounts[0].share.file_declare(fbase)
        obses = [obs[0] for obs in finfo.obses]
        if obses == 1:
            return True
        paths = ['/pd/dcns/mdfs/obs/%s%s' % (obs, finfo.paths[obs]) for obs in obses]        
        res1 = ctx.cluster.execute('md5sum', '-b', paths[0])
        res2 = ctx.cluster.execute('md5sum', '-b', paths[1])
        if res1.stdout.split('\n')[0] == res2.stdout.split('\n'):
            return True 
        return False
    
    def csm_move(self, fbase, mc, size=20, create_file=True, verify=True):
        # pre mobility operations
        fname = self.mounts[0].path + '/' + fbase        
        if create_file:
            ctx.cluster.cli.data_profile_update(name=DATAPROFILE, label=mc.s_labels)
            ctx.cluster.cli.classifier_update(CLASSIFIER, rule=['lala,NOOP,%s' % DATAPROFILE])
            ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=%d' % size, 'oflag=direct'], timeout=1200)

        finfo = self.mounts[0].share.file_declare(fbase)
        s_obses = set(obs[0] for obs in finfo.obses)        
        s_paths = finfo.paths
        sig = self.get_client_file_md5(ctx.clients[0], fname)
        
        # rebalance
        ctx.cluster.cli.data_profile_update(name=DATAPROFILE2, label=mc.t_labels)
        ctx.cluster.cli.classifier_update(CLASSIFIER, rule=['lala,NOOP,%s' % DATAPROFILE2])
        ctx.cluster.cli.share_rebalance(name=self.share.name)
        time.sleep(30)
        ended = self.verify_mobility_ended(fbase, mc.t_labels, timeout=COPY_TIMEOUT)
        
        # verification
        assert ended, 'The mobility did not complete within %d seconds' % COPY_TIMEOUT
        if verify:
            removed = s_obses - set(obs[0] for obs in finfo.obses)                
            for obs in removed:
                assert not ctx.cluster.exists('%s/%s%s' % (OBSPATH, self.mounts[0].share.id, s_paths[obs]))
    
            ctx.clients[0].drop_caches()
            sig2 = self.get_client_file_md5(ctx.clients[0], fname)
            if sig != sig2:
                raise Fatal('the data of the file that had been moved is corrupted: %s' % fbase)
    
            if not self.objs_identical(fbase): 
                raise Fatal('the objects are not identical') 
            
            self.mounts[0].share.verify_mddb_coherency()
    
#    @attr(jira='PD-4359', name='data_mobility_csm_basic')
    def _test_data_mobility_csm_basic(self, setup_env):
        combs = list(MOVECOMBS)
        while combs:
            chosen = random.choice(combs)
            combs.remove(chosen)
            mc = MoveConfiguration(chosen[0], chosen[1])
            fbase = str(uuid.uuid4())
            fname = self.mounts[0].path + '/' + fbase        
            self.csm_move(fbase, mc, 20)
            ctx.clients[0].remove(fname)

    def csm_io(self, direct=False, append=True, random=False, align=True):      
        combs = list(MOVECOMBS)
        fbase = str(uuid.uuid4())
        fname = self.mounts[0].path + '/' + fbase
        tmpfile = '/root/lala'
        size = 200
        bs = 64
        count = 3200        
        if not align:
            size = 198
            bs = 63
            count = 3072
        cmd = ['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=%sK' % bs, 'count=%d' % count]
        
        if direct:
            cmd.append('oflag=direct')
        if not append:
            cmd.append('conv=notrunc')
        
        ctx.clients[0].execute(['dd', 'if=/dev/urandom', 'of=%s' % tmpfile, 'bs=1M', 'count=%d' % size])
        sig = self.get_client_file_md5(ctx.clients[0], tmpfile)

        if not append:
            ctx.clients[0].execute(['dd', 'if=%s' % tmpfile, 'of=%s' % fname, 'bs=64K', 'count=3200', 'oflag=direct'])

        thread = None 
        if random:
            thread = GreenletEx.spawn(self.client_full_copy_random, tmpfile, fname, '%dK' % bs, count)
        else:
            thread = ctx.clients[0].execute(cmd, block=False, timeout=1800)
        
        time.sleep(10)

        while not thread.ready():
            chosen = random.choice(combs)
            mc = MoveConfiguration(chosen[0], chosen[1])
            self.csm_move(fbase, mc, create_file=False, verify=False)
                        
        time.sleep(20)
        ctx.clients.drop_caches()
        time.sleep(10)            

        sig2 = self.get_client_file_md5(ctx.clients[0], fname)

        if sig != sig2:
            Fatal('The copied file got corrupted')

        ctx.clients[0].remove('%s' % tmpfile)
        ctx.clients[0].remove(fname)
        thread.get(timeout=1)
        self.mounts[0].share.verify_mddb_coherency()

#    @attr(jira='PD-4360', name='data_mobility_csm_io_basic')
    def _test_data_mobility_csm_io_basic(self, setup_env):
        self.csm_io()
    
#    @attr(jira='PD-4361', name='data_mobility_csm_io_basic_non_append')
    def _test_data_mobility_csm_io_basic_non_append(self, setup_env):
        self.csm_io(append=False)
    
#    @attr(jira='PD-4362', name='data_mobility_csm_io_basic_direct')
    def _test_data_mobility_csm_io_basic_direct(self, setup_env):
        self.csm_io(direct=True)

#    @attr(jira='PD-4363', name='data_mobility_csm_io_basic_not_aligned')
    def _test_data_mobility_csm_io_basic_not_aligned(self, setup_env):
        self.csm_io(align=False)

#    @attr(jira='PD-4364', name='data_mobility_csm_io_basic_direct_not_append')
    def _test_data_mobility_csm_io_basic_direct_not_append(self, setup_env):
        self.csm_io(direct=True, append=False)

#    @attr(jira='PD-4365', name='data_mobility_csm_io_basic_direct_not_aligned')
    def _test_data_mobility_csm_io_basic_direct_not_aligned(self, setup_env):
        self.csm_io(direct=True, align=False)


#    @attr(jira='PD-4366', name='data_mobility_csm_io_random')
    def _test_data_mobility_csm_io_random(self, setup_env):
        self.csm_io(random=True)
    
#    @attr(jira='PD-4367', name='data_mobility_csm_io_random_non_append')
    def _test_data_mobility_csm_io_random_non_append(self, setup_env):
        self.csm_io(append=False, random=True)
    
#    @attr(jira='PD-4368', name='data_mobility_csm_io_random_direct')
    def _test_data_mobility_csm_io_random_direct(self, setup_env):
        self.csm_io(direct=True, random=True)

#    @attr(jira='PD-4369', name='data_mobility_csm_io_random_not_aligned')
    def _test_data_mobility_csm_io_random_not_aligned(self, setup_env):
        self.csm_io(align=False, random=True)

#    @attr(jira='PD-4370', name='data_mobility_csm_io_random_direct_not_append')
    def _test_data_mobility_csm_io_random_direct_not_append(self, setup_env):
        self.csm_io(direct=True, append=False, random=True)

#    @attr(jira='PD-4371', name='data_mobility_csm_io_random_direct_not_aligned')
    def _test_data_mobility_csm_io_random_direct_not_aligned(self, setup_env):
        self.csm_io(direct=True, align=False, random=True)

#     @attr(name='check')
#     def test_check(self, setup_env):
#         self.mounts[0].share.verify_mddb_coherency()
    
    def create_file(self, size=100, name="/tmp/100m"):
        seed = random.getrandbits(128) 
        ctx.clients[0].execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                                'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                '-nosalt', '|', 'dd', 'of=%s' % name, 'bs=1M', 
                                'count=%d' % size, 'oflag=direct', 'iflag=fullblock'])
        return name

    @attr('data_mobility', jira='PD-21343', name='data_mobility_decom')
    def test_data_mobility_decom(self, setup_env):
        quantity = 100
        size = 100
        files = []
        files_to_move = {}
        curr_time = time.time()
        for _ in xrange(quantity):
            files.append(self.create_file(size=size, name=self.mount.path+'/'+str(uuid.uuid4())))
        total = time.time() - curr_time
        self._logger.info('files creation took %d seconds' % total)
        dd_obses = dict(ctx.cluster.data_stores).values()
        assert len(dd_obses) > 1, 'Cannot run this test when the number of dses on dd < 2'
        obs_to_decom = random.choice(dd_obses)
        dd_obses.remove(obs_to_decom)
        for f in files:
            i = self.client.file_declare(f, self.mount.share)
            source_instances = [instance for instance in i.instances]
            source_obses = [instance.data_store for instance in source_instances]
            if obs_to_decom in source_obses:
                files_to_move[f] = (i.obj_id, self.client.get_md5(f))
        jobs = []
        for f in files_to_move.keys():
            jobs.append((files_to_move[f][0], self.mount.share, [random.choice(dd_obses)]))
        assert jobs, "Did not find any files to move, data placement issue?"
        curr_time = time.time()
        ctx.cluster.dmc.enqueue_mobility_jobs(jobs)
        total = time.time() - curr_time
        self._logger.info('activating mobility took %d seconds' % total)
        files_to_monitor = files_to_move.keys()
        curr_time = time.time()
        self._logger.info('number of files to monitor %d', len(files_to_monitor))
        while files_to_monitor:
            for f in files_to_monitor:
                i = self.client.file_declare(f, self.mount.share)
                source_instances = [instance for instance in i.instances]
                if obs_to_decom not in [instance.data_store for instance in source_instances]:
                    files_to_monitor.remove(f)
                else:
                    break
                time.sleep(1)
            self._logger.info('number of files to monitor %d', len(files_to_monitor))
        total = time.time() - curr_time
        self._logger.info('monitoring took %d seconds' % total)
        self.client.drop_caches()
        for f in files_to_move.keys():
            sig = self.client.get_md5(f)
            self.dbg_assert(files_to_move[f][1] == sig, 'data corruption',
                            fatal=True, fname=f, sig1=files_to_move[f][1], sig2=sig)

    def get_target_obs(self, sobses):
        obses = dict(ctx.cluster.data_stores).values()
        return random.choice(list(set(obses) - set(sobses)))

    def verify_mobility_ended(self, fname, share, target_id, timeout):
        i = ctx.clients[0].file_declare(fname, share)
        t2 = t1 = time.time()
        while (t2 - t1  < timeout):
            i = ctx.cluster.get_file_info(share=share, inode=i.inode)   
            if set([target_id]) == set([inst.data_store for inst in i.instances]):
                break
            gevent.sleep(1)
            t2 = time.time()
        assert t2 - t1 < timeout, 'the move process took more than %d seconds' % timeout

    def verify_mobilities_ended(self, file_target_pairs, share, timeout=600):
        pairs = list(file_target_pairs)
        for (f, t) in pairs:
            self.verify_mobility_ended(f, share, t, timeout)

    def verify_writes(self, files):
        """
        Verifies that all the files in the list can be written to
        """
        for f in files:
            fd1 = self.client.open_file(f, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            self.client.write_file(fd1, 128 * 1024, 128, 3)
            self.client.close_file(fd1)

    def get_active_di_nodes(self, files):
        """
        Returns a list of all active DIs
        """
        dis = [node for node in ctx.cluster.get_nodes_by_type(ObjectType.DATA_MOVER)]
        actv_dis = set()
        for f in files.keys():
            info = ctx.clients[0].file_declare(f, self.share)
            actv_dis.update([di for di in dis if di.internal_id in [i.obs_id for i in info.instances]])
        return actv_dis

    def kill_dis(self, dis, no_restart=True):
        for di in dis:
            if no_restart:
                di.execute(['sed', '-i', '\'s/Restart=on-failure/Restart=no/\'', DI_SYSTEMD ])
                di.execute(['systemctl', 'daemon-reload'])
            di.execute(['pkill', '-9', '-f', DI_BINARY])

    def start_dis(self, dis):
        for di in dis:
            di.service.start(DI_SERVICE)
            di.execute(['sed', '-i', '\'s/Restart=no/Restart=on-failure/\'', DI_SYSTEMD ])
            di.execute(['systemctl', 'daemon-reload'])

    @contextmanager
    def many_mobilities_no_io(self, size, quantity, di_suicide=False):
        """
        Creates many files of a certain size, starts mobility for all of them, yields, 
        verifies that all files moved, verifies data, verifies write, verifies no proxy,
        verifies write once more 
        """
        sigs = {}
        jobs = {}
        targets = []
        for _ in xrange(quantity):
            f = self.create_file(size=size, name=self.mount.path + '/' + str(uuid.uuid4()))
            i = self.client.file_declare(f, self.mount.share)
            m = self.client.get_md5(f)
            source_obses = [instance.data_store for instance in i.instances]
            t = self.get_target_obs(source_obses)
            sigs[f] = m
            jobs[f] = (i.obj_id, self.mount.share, t)
            targets.append((f, t))
        ctx.cluster.dmc.enqueue_mobility_jobs(jobs.values())
        time.sleep(2)
        yield jobs.keys()
        self.client.drop_caches()
        for f in jobs.keys():
            m = self.client.get_md5(f)
            self.dbg_assert(m == sigs[f], 'data corruption detected', fatal=True,
                            fname=f, sig1=sigs[f], sig2=m)
        try:
            self.verify_writes(sigs.keys())
            self.verify_mobilities_ended(targets, self.mount.share)
            self.verify_writes(sigs.keys())
        finally:
            self.client.close_agent()

    @attr('crash-recovery', jira='PD-13055', name='data_mobility_hard_reboot')
    def test_data_mobility_hard_reboot(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        executes force reboot, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10):
            ctx.cluster.force_reboot()

    @attr('crash-recovery', jira='PD-20172', name='data_mobility_soft_reboot')
    def test_data_mobility_soft_reboot(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        executes reboot, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10):
            ctx.cluster.reboot()

    @attr('crash-recovery', jira='PD-20265', name='data_mobility_kill_dme_short')
    def test_data_mobility_kill_dme_short(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        kills dme, restarts it, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10):
            ctx.cluster.kill_and_disable_service('pd-dme')
            ctx.cluster.enable_service('pd-dme')

    @attr('crash-recovery', jira='PD-20266', name='data_mobility_kill_dme_long')
    def test_data_mobility_kill_dme_long(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        kills dme, waits till the dme-di heartbeat is dead, restarts dme  
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10, di_suicide=True):
            ctx.cluster.kill_and_disable_service('pd-dme')
            gevent.sleep(DI_DME_TIMEOUT)
            ctx.cluster.enable_service('pd-dme')

    @attr('crash-recovery', jira='PD-21476', name='data_mobility_kill_protod')
    def test_data_mobility_kill_protod(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        kills protod, restarts protod,
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10):
            ctx.cluster.kill_and_disable_service('pd-protod')
            ctx.cluster.enable_service('pd-protod')

    @attr('crash-recovery', jira='PD-21427', name='data_mobility_kill_dis')
    def test_data_mobility_kill_dis(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M, mobilizes them, no ios,
        kills active dis, waits till the dme-di heartbeat is dead, restarts dis  
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_no_io(size=500, quantity=10) as files:
            dis = self.get_active_di_nodes(files)
            self.kill_dis(dis)
            gevent.sleep(60)
            self.kill_dis(dis)

    @contextmanager
    def many_mobilities_io(self, size, quantity):
        """
        Creates & writes to many files till a certain size, starts mobility for all of them, 
        yields, verifies that all files moved, verifies data, verifies write, verifies no proxy,
        verifies write once more 
        """
        sigs = {}
        jobs = {}
        targets = []
        lfiles = []
        rfiles = []
        results = []
        for _ in xrange(quantity):
            f = self.create_file(size=size, name='/root/' + str(uuid.uuid4()))
            lfiles.append(f)
            rf = os.path.join(self.mount.path, os.path.basename(f))
            rfiles.append(rf)
            self.client.execute(['touch', rf])
            m = self.client.get_md5(f)
            i = self.client.file_declare(rf, self.mount.share)
            source_obses = [instance.data_store for instance in i.instances]
            t = self.get_target_obs(source_obses)
            sigs[rf] = m
            jobs[rf] = (i.obj_id, self.mount.share, t)
            targets.append((rf, t))
        for f, rf in zip(lfiles, rfiles):
            results.append(self.client.execute(['dd', 'if=' + f, 'of=' + rf, 
                                                'oflag=direct', 'bs=65536'], block=False))
        time.sleep(1)
        ctx.cluster.dmc.enqueue_mobility_jobs(jobs.values())
        time.sleep(2)
        yield rfiles
        try:
            for res in results:
                res.get(timeout=300)
            self.client.drop_caches()
            for rf in jobs.keys():
                m = self.client.get_md5(rf)
                self.dbg_assert(m == sigs[rf], 'data corruption detected', fatal=True,
                                fname=rf, sig1=sigs[rf], sig2=m)
            self.verify_writes(sigs.keys())
            self.verify_mobilities_ended(targets, self.mount.share)
            self.verify_writes(sigs.keys())
        finally:
            self.client.close_agent()
            for lf in lfiles:
                self.client.remove(lf)

    @attr('crash-recovery', jira='PD-20267', name='data_mobility_io_hard_reboot')
    def test_data_mobility_io_hard_reboot(self, nfs3_fixture):
        """
        Test - copies 10 files with a size of 500M in the background, mobilizes them,
        executes force reboot, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10):
            ctx.cluster.force_reboot()

    @attr('crash-recovery', jira='PD-20268', name='data_mobility_io_soft_reboot')
    def test_data_mobility_io_soft_reboot(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M in the background, mobilizes them,
        executes reboot, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10):
            ctx.cluster.reboot()

    @attr('crash-recovery', jira='PD-21474', name='data_mobility_io_kill_dme_short')
    def test_data_mobility_io_kill_dme_short(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M in the background, mobilizes them,
        kills dme, restarts it, verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10):
            ctx.cluster.kill_and_disable_service('pd-dme')
            ctx.cluster.enable_service('pd-dme')

    @attr('crash-recovery', jira='PD-21475', name='data_mobility_io_kill_dme_long')
    def test_data_mobility_io_kill_dme_long(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M in the background, mobilizes them,
        kills dme, waits till the dme-di heartbeat is dead, restarts dme  
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10):
            ctx.cluster.kill_and_disable_service('pd-dme')
            gevent.sleep(DI_DME_TIMEOUT)
            ctx.cluster.enable_service('pd-dme')

    @attr('crash-recovery', jira='PD-21477', name='data_mobility_io_kill_protod')
    def test_data_mobility_io_kill_protod(self, nfs3_fixture):
        """
        Test - creates 10 files with a size of 500M in the background, mobilizes them,
        kills protod, restarts protod
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10):
            ctx.cluster.kill_and_disable_service('pd-protod')
            ctx.cluster.enable_service('pd-protod')

    @attr('crash-recovery', jira='PD-21428', name='data_mobility_io_kill_dis')
    def test_data_mobility_io_kill_dis(self, setup_env):
        """
        Test - creates 10 files with a size of 500M in the background, mobilizes them,
        kills active dis, waits till the dme-di heartbeat is dead, restarts dis  
        verifies data, mobility status and ability to write
        """
        with self.many_mobilities_io(size=500, quantity=10) as files:
            dis = self.get_active_di_nodes(files)
            self.kill_dis(dis)
            gevent.sleep(60)
            self.kill_dis(dis)
