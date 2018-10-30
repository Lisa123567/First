#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = 'fgelcer'

from _pytest.python import yield_fixture
import pytest
import os

from tonat.libtests import attr
from tonat.context import ctx
from tonat.test_base import TestBase
from client.models.NodeType import NodeType
from pd_common.mgmt_cli import node_add
import pd_common.utils as utils


class TestNodeMgmtCli(TestBase):

    cuser = 'admin'
    cpasswd = 'admin'

    @yield_fixture
    def setup_test(self):
        self.clean_all()
        yield
        self.clean_all()

    def clean_all(self):
        dses = ctx.cluster.data_stores.values()
        for node in self.nodes_are_not_dds():
            is_ds = False
            for ds in dses:
                if node.name == ds.name.split('::', 1)[0]:
                    # here to add DS remove once it is implemented
                    is_ds = True
            if not is_ds:
                ctx.cluster.cli.node_remove(name=node.name)
                                            # , clish_username=self.cuser, clish_password=self.cpasswd)

    def node_add_isilon(self, name_size=12, cert=None, verify=True):
        name = utils.lower_generator(size=name_size)
        node_add(NodeType.EMC_ISILON, name, cert=cert)
        if verify:
            self.verification(node_name=name, node_type=NodeType.EMC_ISILON)

    def node_add_nexenta(self, name_size=12, cert=None, verify=True):
        name = utils.lower_generator(size=name_size)
        node_add(NodeType.NEXENTA, name, cert=cert)
        if verify:
            self.verification(node_name=name, node_type=NodeType.NEXENTA)

    def node_add_netapp_7mode(self, name_size=12, cert=None, verify=True):
        name = utils.lower_generator(size=name_size)
        node_add(NodeType.NETAPP_7MODE, name, cert=cert)
        if verify:
            self.verification(node_name=name, node_type=NodeType.NETAPP_7MODE)

    def node_add_netapp_cmode(self, name_size=12, cert=None, verify=True):
        name = utils.lower_generator(size=name_size)
        node_add(NodeType.NETAPP_CMODE, name, cert=cert)
        if verify:
            self.verification(node_name=name, node_type=NodeType.NETAPP_CMODE)

    def node_add_pd_dp(self, name_size=12, cert=None, verify=True):
        pass

    def node_add_pd_ds(self, name_size=12, cert=None, verify=True):
        # run pd-node-setup on the DS - pointing to the DD (or cluster) IP
        node_name = ctx.get_ds()

        # verify it is in the node list, and add it as a managed node
        node_list = ctx.cluster.cli.node_list(unauthorized=True)
        if node_name in node_list:
            ctx.cluster.cli.node_add()
        ctx.cluster.cli.ds_add()

    def verification(self, node_name, node_type):

        with self.step('Verify node appears in node-list result'):
            node = ctx.cluster.cli.node_list(name=node_name)[0]

        if node_type == NodeType.EMC_ISILON:
            # verification for Isilon
            node_address = ctx.cluster.nodes[node_name].mgmt_ip_address.address
            ds = [ds for ds in ctx.data_stores.filter(NodeType.EMC_ISILON) if ds.conn.address == node_address][0]
            exports = ds.get_shares()
            # network_if = ds.get_network_ifs
            for node_export in node['Platform Services']:
                match = False
                if node_export['Type'] == 'LOGICAL_VOLUME':
                    for export in exports:
                        if os.path.abspath(node_export['Exported Path']) == os.path.abspath(export.name):
                            match = True
                            break
                    if not match:
                        raise Exception('One or more exports in the DD don\'t exist in the node ' + str(node_export))
            exports2 = ds.get_shares()
            for export2 in exports2:
                match = False
                for node_export in node['Platform Services']:
                    if node_export['Type'] != 'LOGICAL_VOLUME':
                        continue
                    if os.path.abspath(export2.name) == os.path.abspath(node_export['Exported Path']):
                        match = True
                        break
                if not match:
                    raise Exception('One or more exports missing in the DD ' + str(export2))

        elif node_type == NodeType.NETAPP_7MODE:
            # verification for Netapp 7Mode
            node_address = ctx.cluster.nodes[node_name].mgmt_ip_address.address
            ds = [ds for ds in ctx.data_stores.filter(NodeType.NETAPP_7MODE) if ds.conn.address == node_address][0]
            exports = ds.get_shares()
            # network_if = ds.get_network_ifs
            for node_export in node['Platform Services']:
                match = False
                if node_export['Type'] == 'LOGICAL_VOLUME':
                    for export in exports:
                        if os.path.abspath(node_export['Exported Path']) == os.path.abspath(export.name):
                            match = True
                            break
                    if not match:
                        raise Exception('One or more exports in the DD don\'t exist in the node ' + str(node_export))
            exports2 = ds.get_shares()
            for export2 in exports2:
                match = False
                for node_export in node['Platform Services']:
                    if node_export['Type'] != 'LOGICAL_VOLUME':
                        continue
                    if os.path.abspath(export2.name) == os.path.abspath(node_export['Exported Path']):
                        match = True
                        break
                if not match:
                    raise Exception('One or more exports missing in the DD ' + str(export2))

        elif node_type == NodeType.NETAPP_CMODE:
            # verification for Netapp CMode
            node_address = ctx.cluster.nodes[node_name].mgmt_ip_address.address
            ds = [ds for ds in ctx.data_stores.filter(NodeType.NETAPP_CMODE) if ds.conn.address == node_address][0]
            exports = ds.get_shares()
            # network_if = ds.get_network_ifs
            for node_export in node['Platform Services']:
                match = False
                if node_export['Type'] == 'LOGICAL_VOLUME':
                    for export in exports:
                        if os.path.abspath(node_export['Exported Path']) == os.path.abspath(export.name):
                            match = True
                            break
                    if not match:
                        raise Exception('One or more exports in the DD don\'t exist in the node ' + str(node_export))
            exports2 = ds.get_shares()
            for export2 in exports2:
                match = False
                for node_export in node['Platform Services']:
                    if node_export['Type'] != 'LOGICAL_VOLUME':
                        continue
                    if os.path.abspath(export2.name) == os.path.abspath(node_export['Exported Path']):
                        match = True
                        break
                if not match:
                    raise Exception('One or more exports missing in the DD ' + str(export2))

            pass
        elif node_type == NodeType.NEXENTA:
            # verification for Nexenta
            pass
        elif node_type == NodeType.PD:
            # here i have to test if the node is DS, DD or DP, and run specific tests for each
            pass
        elif node_type == NodeType.OTHER:
            # understand what means Other type, and write specific verification
            pass

    def nodes_are_not_dds(self):
        ret = []
        for node in ctx.cluster.nodes.values():
            is_dd = False
            for service in node.system_services:
                if service._type == 'DATA_DIRECTOR':
                    is_dd = True
            if not is_dd:
                ret.append(node)
        return ret

    def add_shares(self, amount, nodes):
        for i in xrange(amount):
            share_name = utils.lower_generator(10)
            for node in nodes:
                node.create_share()

    @pytest.mark.skipif(len(ctx.data_stores.filter(NodeType.EMC_ISILON)) < 1
                        or len(ctx.data_stores.filter(NodeType.EMC_ISILON)[0].get_shares()) < 2,
                        reason='INSUFFICIENT RESOURCES - Supply at least one Isilon with at least 2 exports')
    @attr('cli', 'node_mgmt_cli', jira='PD-10713', tool='cli', complexity=1, name='test_1_add_1_node_isilon')
    def test_1_add_1_node_isilon(self, setup_test):
        self.node_add_isilon()

    @pytest.mark.skipif(len(ctx.data_stores.filter(NodeType.NETAPP_7MODE)) < 1,
                        reason='INSUFFICIENT RESOURCES - Supply at least one Netapp 7-Mode')
    @attr('cli', 'node_mgmt_cli', jira='PD-10715', tool='cli', complexity=1, name='test_2_add_1_node_netapp_7mode')
    def test_2_add_1_node_netapp_7mode(self, setup_test):
        self.node_add_netapp_7mode()

    @pytest.mark.skipif(len(ctx.data_stores.filter(NodeType.NETAPP_CMODE)) < 1,
                        reason='INSUFFICIENT RESOURCES - Supply at least one Netapp C-Mode')
    @attr('cli', 'node_mgmt_cli', jira='PD-10716', tool='cli', complexity=1, name='test_3_add_1_node_netapp_cmode')
    def test_3_add_1_node_netapp_cmode(self, setup_test):
        self.node_add_netapp_cmode()