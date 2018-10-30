#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = 'fgelcer'

from random import choice
from paramiko.ssh_exception import SSHException
from _pytest.python import yield_fixture
from client.models import NodeType
import pytest

from tonat.libtests import attr
from tonat.context import ctx
from tonat.test_base import TestBase
from client.models.NodeType import NodeType
from tonat.errors import CliOperationFailed, Failure
import pd_common.utils as utils
from pd_common.mgmt_cli import node_add


def add_isilon(_cost=None):
    # create exports in the storage (if needed)

    # verify if there are enough of this node type (?)
    node_add(NodeType.EMC_ISILON)
    ctx.cluster.cli.ds_add(name=ctx.cluster.nodes.name, logical_volume_name=ctx.cluster.nodes, cost=_cost)

def add_nexenta():
    pass

def add_netapp_7mode():
    pass

def add_netapp_cmode():
    pass

def add_pd_ds():
    # run pd-node-setup on the DS - pointing to the DD (or cluster) IP
    node_name = ctx.get_ds()

    # verify it is in the node list, and add it as a managed node
    node_list = ctx.cluster.cli.node_list(unauthorized=True)
    if node_name in node_list:
        ctx.cluster.cli.node_add()
    ctx.cluster.cli.ds_add()


class TestDSMgmtCli(TestBase):

    @yield_fixture
    def setup_test(self):
        self.clean_all()
        yield
        self.clean_all()

    def clean_all(self):
        pass

    @pytest.mark.skipif(len(ctx.data_stores.filter(NodeType.EMC_ISILON)) < 1,
                        reason='Insufficient resources - Supply at least one Isilon')
    @attr('cli', 'ds_mgmt_cli', jira='', tool='cli', complexity=1, name='test_1_add_1_ds_isilon')
    def test_1_add_1_ds_isilon(self, setup_test):
        logic_vol_nodes = dict()
        for node_name, node in ctx.cluster.nodes.iteritems():

            for platform_service in node.platform_services:
                if platform_service._type == 'LOGICAL_VOLUME':
                    l = logic_vol_nodes.get(node_name, [])
                    l.append(platform_service)
                    logic_vol_nodes[node_name] = l



        nodes = ctx.cluster.nodes.values()
        self._logger.info(nodes)
        for volume in node.logical_volume:
            self._logger.info(volume)

        print nodes

        add_isilon()