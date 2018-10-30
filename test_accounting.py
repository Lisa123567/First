#__author__ = 'mleopold'

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
import uuid
import random
import time
import os
from pytest import yield_fixture

class TestAccounting(TestBase):
    
    @yield_fixture
    def setup_env(self, get_pnfs4_2_mount_points): 
        self.mounts = get_pnfs4_2_mount_points       
        yield