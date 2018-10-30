
__author__ = 'rkissos'

from time import sleep
from pytest import mark

from tonat.context import ctx
from tonat.test_base import TestBase, attr
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.rand_ex import RandPath

from pd_tools.fio_caps import FioCaps


class TestDme(TestBase):
    @mark.last
    @attr(jira='PD-9457', name='test_dme_restart')
    def test_dme_restart(self, get_pnfs_mount_points):
        mounts = get_pnfs_mount_points
        fio = FioCaps().short()
        ctx.clients.execute_tool(fio, mounts)

        ctx.cluster.service.stop('pd-dme')
        sleep(5)
        ctx.cluster.service.wait_for('pd-dme')

        fio = FioCaps().short()
        ctx.clients.execute_tool(fio, mounts)

    @attr(jira='next', name='test_dme_recovery')
    def test_dme_recovery(self, get_nfs4_1_mount_points):
        mount = get_nfs4_1_mount_points[0]
        fvs = []
        filenames = []
        instances = set([])
        num_of_files = 10
        for _ in xrange(num_of_files):
            filename = str(RandPath(mount.path))
            filenames.append(filename)
            fvs.append(ctx.clients[0].write_to_file(filename, bs='4k',
                                                    count=1024, block=False))
        fvs = dispatcher_tuple(fvs)

        sleep(3)
        ctx.cluster.service.stop('pd-dme')
        sleep(5)
        ctx.cluster.service.wait_for('pd-dme')
        fvs.get()

        for filename in filenames:
            fd = ctx.clients[0].file_declare(filename, mount.share)
            instances.add(fd.instances[0].instance_id)
        assert len(instances) == num_of_files, 'files got same instance_id'
