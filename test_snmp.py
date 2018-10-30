#__author__ = 'mleopold'

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pytest import mark
from pytest import yield_fixture
import uuid
import random
import time
import os
import gevent
from contextlib import contextmanager

PORT = '8443'
MIB = 'PRIMARY-DATA-MIB.txt'
URL = '/mgmt/v1.2/mib/'
MIBFILE = '/root/' + MIB
COMMUNITY='public2'

@yield_fixture(autouse=True, scope='session')
def get_mib():
    ctx.clients[0].execute(['wget', '--no-check-certificate', 
                            'https://' + ctx.cluster.address + ':' + PORT + URL + MIB,
                            '-O', MIBFILE])
    ctx.clients[0].execute(['yum', 'install', '-y', 'net-snmp-utils'])
    yield


class TestSNMP(TestBase):
    """
    A snmp related test suite.
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
        self.nodes = [ctx.cluster]
        self.nodes.extend(ctx.data_stores)
        self.nodes.extend(ctx.data_portals)
        yield       
        # SNMP clear also disables the SNMP service
        ctx.cluster.cli.snmp_clear()

    def verify_snmp_enabled(self):
       ctx.cluster.execute(['pgrep', 'snmpd'])
       for ds in ctx.data_stores:
           ds.execute(['pgrep', 'snmpd'])
       for dp in ctx.data_portals:
           dp.execute(['pgrep', 'snmpd'])

    def verify_snmp_disabled(self):
        with self.verify_negative('the snmp service is still running'):
            ctx.cluster.execute(['pgrep', 'snmpd'])
        for ds in ctx.data_stores:
            with self.verify_negative('the snmp service is still running'):
                ds.execute(['pgrep', 'snmpd'])
        for dp in ctx.data_portals:
            with self.verify_negative('the snmp service is still running'):
                dp.execute(['pgrep', 'snmpd'])

    def verify_snmp_get(self, community=COMMUNITY, version='2c', mib='ALL', negative=False,
                        nodes=[ctx.cluster], oid='sysName.0'):
       if negative:
           for node in nodes:
                with self.verify_negative():
                   self.client.execute(['snmpget', node.address, '-c' + community, oid, 
                                        '-m' + mib, '-v' + version])
       else:
           for node in nodes:
               self.client.execute(['snmpget', node.address, '-c' + community, oid, 
                                    '-m' + mib, '-v' + version])

    @contextmanager
    def verify_negative(self, msg='expected a failure that did not take place'):
        failed = True
        try:
            yield
            failed = False
        except:
            pass
        assert failed, msg

    @attr('TestCli', jira='PD-21601', complexity=1, name='snmp_general')
    def test_snmp_general(self, setup_env):
        """
        Test - verifies the snmp functionality
        """
        with self.step('enable snmp'):
            ctx.cluster.cli.snmp_enable()
            self.verify_snmp_enabled()
        with self.step('enable snmpi again'):
            ctx.cluster.cli.snmp_enable()
            self.verify_snmp_enabled()
        with self.step('configure snmp - set a v1 manager'):
            ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,V1,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='1')
            self.verify_snmp_get(nodes=self.nodes, version='2c', negative=True)
            self.verify_snmp_get(nodes=self.nodes, version='1', community='bogus', negative=True)
        with self.step('configure snmp - set a v2 manager'):
            ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,V2C,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='2c')
            self.verify_snmp_get(nodes=self.nodes, version='1', negative=True)
            self.verify_snmp_get(nodes=self.nodes, version='2c', community='bogus', negative=True)
        with self.step('configure snmp - set an any manager'):
            ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,ANY,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='1')
            self.verify_snmp_get(nodes=self.nodes, version='2c')
            self.verify_snmp_get(nodes=self.nodes, version='2c', oid='bogus', negative=True)
        with self.step('configure snmp - repeat a command twice'):
            ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,ANY,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='1')
            self.verify_snmp_get(nodes=self.nodes, version='2c')
        with self.step('configure snmp - use a non authorized IP'):
            ctx.cluster.cli.snmp_config(manager='10.0.0.9/32,ANY,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='2c', negative=True)
        with self.step('configure snmp - input validation'):
            with self.verify_negative('should have failed due to incorrect address'):
                ctx.cluster.cli.snmp_config(manager='123.123.123.123.123/32,ANY,'+COMMUNITY)
            with self.verify_negative('should have failed due to incorrect address'):
                ctx.cluster.cli.snmp_config(manager='lala/32,ANY,'+COMMUNITY)
            with self.verify_negative('should have failed due to incorrect cidr'):
                ctx.cluster.cli.snmp_config(manager=self.client.address+'/33,ANY,'+COMMUNITY)
            with self.verify_negative('should have failed due to incorrect cidr'):
                ctx.cluster.cli.snmp_config(manager=self.client.address+'/ab,ANY,'+COMMUNITY)
        with self.step('disable snmp'):
            ctx.cluster.cli.snmp_disable()
            self.verify_snmp_disabled()
        with self.step('disable snmp - second time'):
            ctx.cluster.cli.snmp_disable()
            self.verify_snmp_disabled()
        with self.step('disable and enable snmp'):
            for _ in xrange(5):
                ctx.cluster.cli.snmp_enable()
                self.verify_snmp_enabled()
                ctx.cluster.cli.snmp_disable()
                self.verify_snmp_disabled()
            ctx.cluster.cli.snmp_enable()
            self.verify_snmp_enabled()
        with self.step('clear snmp'):
            ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,ANY,'+COMMUNITY)
            self.verify_snmp_get(nodes=self.nodes, version='2c')
            ctx.cluster.cli.snmp_clear()
            self.verify_snmp_disabled()
            ctx.cluster.cli.snmp_enable()
            self.verify_snmp_get(nodes=self.nodes, version='2c', negative=True)

    @attr('TestCli', jira='PD-21887', complexity=1, name='snmp_mib')
    def test_snmp_mib(self, setup_env):
        """
        Test - verifies the snmp mib queries
        """
        ctx.cluster.cli.snmp_config(manager=self.client.address+'/32,ANY,'+COMMUNITY)
        ctx.cluster.cli.snmp_enable()
        self.verify_snmp_get(version='2c', mib=MIBFILE, oid='pdDsSystemUuid.0')
        self.verify_snmp_get(version='1', mib=MIBFILE, oid='pdDsSystemUuid.0')
        for _ in xrange(10):
            self.client.execute(['snmpwalk', ctx.cluster.address, '-c'+COMMUNITY, '-v2c',
                                 '-m'+MIBFILE]) 
            self.client.execute(['snmpwalk', ctx.cluster.address, '-c'+COMMUNITY, '-v1',
                                 '-m'+MIBFILE]) 
