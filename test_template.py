__author__ = 'rkissos'

from tonat.context import ctx
from tonat.test_base import TestBase, attr


class TestTemplate(TestBase):

    @attr(jira='next', name='test_template_1')
    def test_template_1(self):
    # Or use fixture: (check conftest.py for fixture list)
    # def test_template_1(self, get_mount_points):
        all_shares = ctx.cluster.shares.values()