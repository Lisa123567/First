import functools
import os
import random
import time

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
#from tonat.errors import CliOperationFailed
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.greenlet_management import blockability

from pd_tools.fio_caps import FioCaps
from pd_common.vcenter_fixture import Vcenter

class VcenterFio(FioCaps):

    def __init__(self, *args, **kw):
        self.repeat = kw.pop('repeat', True)
        super(VcenterFio, self).__init__(*args, **kw)

    @property
    def _conf_params(self):
        result = super(VcenterFio, self)._conf_params
        result.update(
            size=30*1024*1024,
            runtime='300s',
            directory='/tmp',
            filename='vcenter',
        )
        return result

    def vcenter(self, timeout=1200, teardown=True):
        '''
        :summary: run fio on a small file for a short period of time
        '''
        self._set_execution({}, timeout, teardown)
        return self


class TestVcenterFio(TestBase):
    @staticmethod
    def _get_vmdk_path(client, prefix, vmname):
        path = os.path.join(prefix, vmname)
        result = [fname
                  for fname in client.listdir(path)
                  if fname.endswith('.vmdk')][0]
        return os.path.join(path, result)

    @blockability
    def mobwait(self, vmdk_details, *args, **kw):
        kw['block'] = True
        vmdk_details.wait_for_mobility(*args, **kw)
        self.fio.repeat = False
        self._logger.info('Mobility finished')

    @attr('vcenter_E2E', jira='PD-21253', name='test_vcenter_e2e_basic')
    def test_vcenter_e2e_basic(self, vcenter, get_mount_points):
        share = vcenter.share
        mounts = get_mount_points
        mount = mounts[0]

        assert len(ctx.clients) > 1, 'Need at least 2 clients'
        pdclient = ctx.clients[0]
        vcclient = ctx.clients[-1]

        self.fio = VcenterFio().vcenter()
        params = self.fio._conf_params
        directory = params['directory']
        fname = params['filename']
        vcclient.mkdirs(directory)
        filename = os.path.join(directory, fname)

        with self.step('deploy on clients'):
            ctx.clients.deploy(['fio'])

        with self.step('gather info'):
            vmdk_path = self._get_vmdk_path(
                pdclient, mount.path, vcenter.vm_name)
            vmdk_details = pdclient.file_declare(vmdk_path, mount.share)
            data_stores = set(ctx.cluster.data_stores.values())
            source_obses = set((instance.data_store
                                for instance in vmdk_details.instances))
            non_source_obses = data_stores - source_obses
            target_obs = random.choice(tuple(non_source_obses))

        with self.step('execute tool'):
            execute_task = vcclient.execute_tool(self.fio, mount, block=False)

        with self.step('mob job'):
            ctx.cluster.dmc.enqueue_mobility_job(
                vmdk_details.obj_id, mount.share, [target_obs])

            mobwait = self.mobwait(vmdk_details,
                target_obs, attempt=300, interval=10, block=False)

        with self.step('run in parallel and wait for everything to finish'):
            dispatcher_tuple((execute_task.get, mobwait.get))()
