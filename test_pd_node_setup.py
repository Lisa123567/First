#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = 'fgelcer'

from random import choice
from time import sleep
from os.path import join

from tonat.libtests import attr
from tonat.context import ctx
from tonat.test_base import TestBase
from tonat.errors import  Failure
from tonat.product.objects.mount import Mount, NfsVersion
#from tonat.product.objects.label import RandLabel, Label
#from tonat.product.objects.obs import RandObs, DsType
#from tonat.product.objects.share.dd_share import RandDdShare
import pd_common.utils as utils


class TestDsMgmtCli(TestBase):

    @attr(name='basic_install')
    def test_basic_install(self):
        ctx.servers[0].conn.execute(['yum', 'install', '-y', 'pd-dd'])
        ctx.servers[0].conn.execute(['pd-node-setup', '-c', '172.31.13.119', '-i', '22', '-E',
                                     '-H', 'fabio-dd-21.lab.primarydata.com', '--configure-dd',
                                     '--skip-hw', '--standalone', '--force'], timeout=3200)
        ctx.cluster.cli.user_list()
