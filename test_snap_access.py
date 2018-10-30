#__author__ = 'mleopold'


from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
import random
import string
from contextlib import contextmanager
from tonat.nfsctl import NfsVersion
from tonat.product.objects.mount import Mount
from client.models.ShareView import ShareView
from pytest import yield_fixture

class TestSnapAccess(TestBase):
    
    @yield_fixture
    def setup_env(self, get_pnfs_mount_points):
        self.mounts = get_pnfs_mount_points
        yield

    @attr(jira='next', complexity=1, name='snap_access_md_change')
    def test_snap_access_md_change(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_md_create_001')
    def test_snap_access_md_create_001(self, setup_env):
        pass
 
    @attr(jira='next', complexity=1, name='snap_access_md_create_002')
    def test_snap_access_md_create_002(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_data_write')
    def test_snap_access_data_write(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_data_read')
    def test_snap_access_data_read(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_hidden_dir')
    def test_snap_access_hidden_dir(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_permissions_001')
    def test_snap_access_permissions_001(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_permissions_002')
    def test_snap_access_permissions_002(self, setup_env):
        pass

    @attr(jira='next', complexity=1, name='snap_access_cascaded')
    def test_snap_access_cascaded(self, setup_env):
        pass
