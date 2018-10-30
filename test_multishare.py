#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = 'fgelcer'

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from tonat.product.objects.mount import Mount, NfsVersion
#from tonat.product.objects.share.dd_share import RandDdShare
from tonat.dispatcher_tuple import dispatcher_tuple
from pd_tools.fio_caps import FioCaps
from pd_common import utils

from pytest import yield_fixture


SHARES = 5

class TestMultiShares(TestBase):
    @yield_fixture()
    def setup(self):
        num_of_shares = max(len(ctx.clients), SHARES)
        for _ in xrange(num_of_shares):
            ctx.cluster.cli.share_create_by_obj(RandDdShare())
        mnt_templates = []
        clients = []
        for idx, share in enumerate(ctx.cluster.shares.values()):
            mnt_templates.append(Mount(share, NfsVersion.nfs4_1))
            clients.append(ctx.clients[idx % len(ctx.clients)])
        mnt_templates = dispatcher_tuple(mnt_templates)
        clients = dispatcher_tuple(clients)
        mounts = clients.nfs.mount(mnt_templates)
        yield mounts, clients
        clients[0].clean_dir(mounts[0].path, timeout=1200)
        clients.nfs.umount(mounts)
        clients.remove(mounts.path)

    @attr('cli', 'multishare', jira='PD-5122', tool='cli', complexity=1, name='test_1_parallel_io_all_shares')
    def test_1_parallel_io_all_shares(self, setup):
        mounts = setup[0]
        clients = setup[1]
        fvs = []
        for client, mount in zip(clients, mounts):
            fio = FioCaps(conf_file='tonian_settings/' + utils.uld_mix_generator(15)).short()
            fvs.append(client.execute_tool(fio, mount, block=False))
        for fv in fvs:
            fv.get()

    @attr('cli', 'multishare', jira='PD-5123', tool='cli', complexity=1, name='test_2_parallel_io_all_shares_one_mobility')
    def test_2_parallel_io_all_shares_one_mobility(self):
        pass

    @attr('cli', 'multishare', jira='PD-5124', tool='cli', complexity=1, name='test_3_parallel_io_half_shares_half_mobility')
    def test_3_parallel_io_half_shares_half_mobility(self):
        pass

    @attr('cli', 'multishare', jira='PD-5125', tool='cli', complexity=1, name='test_4_parallel_io_dd_failover')
    def test_4_parallel_io_dd_failover(self):
        pass

    @attr('multishare', 'cli', jira='PD-5126', tool='cli', complexity=1, name='test_5_parallel_io_ds_reboot')
    def test_5_parallel_io_ds_reboot(self):
        pass

    @attr('cli', 'multishare', jira='PD-5173', tool='cli', complexity=1, name='test_6_parallel_io_share_update')
    def test_6_parallel_io_share_update(self):
        pass
