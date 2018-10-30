#__author__ = 'mleopold'
# -*- coding: UTF-8 -*-


##################################################################
#                                                                #
# This is the Phase-1 cloud mover test suite                     #
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
from pd_tools.fsx_caps import FsxCaps
from pytest import yield_fixture
from client.models.ObjectType import ObjectType


COPY_TIMEOUT = 600

@yield_fixture(autouse=True, scope='session')
def no_grader():
    ctx.cluster.edit_conf(DMEFILE, kwargs={'disableMobilityDecisions ': ' true'})
    time.sleep(10)
    yield
    ctx.cluster.edit_conf(DMEFILE, kwargs={'disableMobilityDecisions ': ' false'})


class TestMobility(TestBase):

    @yield_fixture
    def setup_env(self, get_mount_points):
        self.mounts = get_mount_points
        self.mount = self.mounts[0]
        self.client = ctx.clients[0]
        # add two cloud nodes (different endpoints)
        
        # add an object volume from each node

        yield

    def create_random_sparse(self, size=100, seed=None):
        """
        Creates a randomly partially sparse file
        """
        if seed == None:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(1013), '-s', '1'])
        else:
            fsx = FsxCaps().generic(['-l', str(size * 1024 * 1024), '-N', str(1013), '-s', '1'], seed=seed)
        self.client.execute_tool(fsx, self.mount)
        return self.client.execute(['find', self.mount.path, '-name', fsx._data]).stdout.split("\n")[0]               

    def get_client_file_md5(self, client, cfile):
        res = client.execute(['md5sum', '-b', '%s' % cfile])
        sig = res.stdout.split('\n')[0].split()[0]
        return sig

    def cloud_mover_scenario(self, to_cloud=True, size=20, name=None, timeout=COPY_TIMEOUT, 
                             fexists=False, sparse=False, quick=False, seed=None, verify=True):
        # create a file on a share
        if name == None:
            fname = self.mount.path + '/' + str(uuid.uuid4())
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                self.client.execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=1', 'oflag=direct'], timeout=1200)
        else:
            fname = self.mount.path + '/' + name
        
        # get file details (inode, gen number, etc.)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            file_details = self.client.file_declare(fname, self.mount.share)
        source_instances = [instance for instance in file_details.instances]
        source_obses = [instance.data_store for instance in source_instances]
        obs_file = file_details.instances[0].path
        
        # create a file on a share with a certain size and random contents
        if not fexists:
            if not sparse and not quick:
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    self.client.execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=%d' % size, 'oflag=direct'], timeout=1200)
            elif not sparse:
                seed = random.getrandbits(128) 
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    self.client.execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                                         'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                                         '-nosalt', '|', 'dd', 'of=%s' % fname, 'bs=1M', 
                                         'count=%d' % size, 'oflag=direct', 'iflag=fullblock'], timeout=1200)
            else:
                fname = self.create_random_sparse(size=size, seed=seed)
                info1 = self.client.file_declare(fname, self.mount.share)
                instances1 = [instance for instance in info1.instances]
                obses1 = [instance.data_store for instance in instances1]
                mnt1 = self.mntdirs[obses1[0].internal_id]
                file1 = '/'.join(instances1[0].path.split('/')[2:])
                # TODO: put back the assertion when PD-17010 is fixed
                # blocks1 = self.client.execute(['stat', '-c', '\"%b\"', mnt1 + '/' + file1]).stdout.split("\n")[0]

            # get md5sum of the file
            with self.dbg_wrapper(fname=fname, share=self.mount.share):
                sig = self.get_client_file_md5(ctx.clients[0], fname)

        # get a target DS
        dd_obses = dict(ctx.cluster.data_stores).values()
        assert len(dd_obses) > 1, 'Cannot run this test when the number of dses on dd < 2'
        target_obs = random.choice([ds for ds in dd_obses if ds not in source_obses])

        # invoke file move to a specific DS
        with self.dbg_wrapper(fname=fname, share=self.mount.share, file_details=file_details.__dict__, target_obs=target_obs):
            ctx.cluster.dmc.enqueue_mobility_job(file_details.obj_id, self.mount.share, [target_obs])

        # poll get file info till only the source DS disappears, when that happens take a timestamp
        if verify:
            t2 = t1 = time.time()
            while (t2 - t1  < timeout):
                with self.dbg_wrapper(fname=fname, share=self.mount.share):
                    file_details2 = ctx.cluster.get_file_info(share=self.mount.share, inode=file_details.inode)   
                if set([target_obs]) == set([inst.data_store for inst in file_details2.instances]):
                    break
                gevent.sleep(1)
                t2 = time.time()
                self.dbg_assert(t2 - t1 < timeout, 'the move process took more than %d seconds' % timeout,
                                fname=fname, file_details=file_details2.__dict__, share=self.mount.share)

        # fail in case of timeout, else get another md5sum for the file and compare with the previous run
        # TODO: METADATA check - need to add it
  
        self.client.drop_caches()
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            sig2 = self.get_client_file_md5(self.client, fname)
        self.dbg_assert(sig == sig2, 
                        'the data of the file that had been moved is corrupted',
                        fatal=True,
                        fname=fname,
                        file_details=file_details2.__dict__,
                        original_sig=sig,
                        new_sig=sig2, share=self.mount.share)
        if sparse:
            # TODO: put back the assertion when PD-17010 is fixed
            # info2 = self.client.file_declare(fname, self.mounts[0].share)
            # instances2 = [instance for instance in info2.instances]
            # obses2 = [instance.data_store for instance in instances2]
            # mnt2 = self.mntdirs[obses2[0].internal_id]
            # file2 = '/'.join(instances2[0].path.split('/')[2:])
            # blocks2 = self.client.execute(['stat', '-c', '\"%b\"', mnt2 + '/' + file2]).stdout.split("\n")[0]
            # rsd = hlp.get_rsd([float(blocks1), float(blocks2)])
            # assert rsd <= 0.05, "the rsd between the two block counts is larger than 0.05"
            pass
        
        # verify that there is no file in the real location anymore
        # TODO: make this last check run via mount v3 of the ds

        if verify:
            # wait 10s to let DME delete the instance
            gevent.sleep(10)
            source_obs_ip = source_obses[0].node.mgmt_ip_address.address
            physical_ds = ctx.get_ds('address', source_obs_ip)
            assert not physical_ds.exists(obs_file), 'the source file was not removed from the source obs'
            self._logger.info('one move passed')
        return fname      

    @attr('cloud_mover', jira='next', complexity=1, name='cloud_mover_basic')
    def test_cloud_mover_basic(self, setup_env):
        """
        Test - moves a file, no IO
        """
        self.cloud_mover_scenario(quick=True)

    @attr('cloud_mover', jira='next', complexity=1, name='cloud_mover_serial_moves_same_file')
    def test_cloud_mover_serial_moves_same_file(self, setup_env):
        """
        Test - moves a file 18 times in a row
        """
        fbase = str(uuid.uuid4())
        fname = self.mount.path + '/' + fbase        
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            self.client.execute(['dd', 'if=/dev/urandom', 'of=%s' % fname, 'bs=1M', 'count=20', 'oflag=direct'], timeout=1200)
            self.client.execute(['cp', '-p', fname, '/tmp/original'])
        for _i in xrange(18):
            self.cloud_mover_scenario(name=fbase, fexists=True)
        with self.dbg_wrapper(fname=fname, share=self.mount.share):
            self.client.remove(fname)
        
    @attr('cloud_mover', jira='next', complexity=1, name='cloud_mover_sparse')
    def test_cloud_mover_sparse(self, nfs3_fixture):
        """
        Test - moves a file with random holes, verifies its data consumption didn't change by much
        """
        self.cloud_mover_scenario(sparse = True, size = 400)

    @attr('cloud_mover', jira='next', complexity=1, name='cloud_mover_sparse_deter')
    def test_cloud_mover_sparse_deter(self, nfs3_fixture):
        """
        Test - moves a file with random holes (static seed), verifies its data consumption didn't change by much
        """
        self.cloud_mover_scenario(sparse = True, size = 400, seed=31)

    def create_file(self, size=100, name="/tmp/100m"):
        seed = random.getrandbits(128) 
        self.client.execute(['dd', 'if=/dev/zero', 'bs=1M', 'count=%d' % size, '|',
                             'openssl', 'enc', '-aes-256-ctr', '-pass', 'pass:%032x' % seed,
                             '-nosalt', '|', 'dd', 'of=%s' % name, 'bs=1M', 
                             'count=%d' % size, 'oflag=direct', 'iflag=fullblock'])
        return name

