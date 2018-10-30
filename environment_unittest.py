#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from tonat.product.mgmt_interfaces import manager

__author__ = 'Avi Tal <avi@primarydata.com>'
__date__ = 'Feb 8, 2014'

import unittest
from tonat.context import ctx, Environment
from tonat.objects import (data_director)

"""
Example for env.json:
{
    "id": "this-is-my-id",
    "clients": [{"address": ",
                 "user": "",
                 "password": ""
                }
               ],
    "manager": {"address": "",
                "user": "",
                "password": "",
                "auth": ["", ""]
                },
    "data_directors": [{"address": "",
                       "user": "",
                       "password": ""
                      }
                     ],
    "data_stores": [{"address": "",
                     "user": "",
                     "password": "",
                     "type": "performance"
                    }
                   ]
}
"""


class TestEnv(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        ctx.load('/home/avi/git/tonat2/tests/envs/env.json')

    def test_repr(self):
        o = ctx
        t = type(ctx)
        assert isinstance(o, Environment), '{0} {1}'.format(t, o)

    def test_id(self):
        o = ctx.id
        t = type(o)
        assert t is str, '{0} {1}'.format(t, o)

    def test_clients(self):
        o = ctx.clients
        t = type(o)
        assert t is list, '{0} {1}'.format(t, o)

    def test_data_directors(self):
        o = ctx.data_directors
        t = type(o)
        assert t is list, '{0} {1}'.format(t, o)
        for i in o:
            assert isinstance(i, data_director.DataDirector), \
            '{0} {1}'.format(type(i), i)

    def test_data_stores(self):
        o = ctx.data_stores
        t = type(o)
        assert t is list, '{0} {1}'.format(t, o)

    def test_manager(self):
        o = ctx.manager
        t = type(o)
        assert isinstance(o, manager.Manager), '{0} {1}'.format(t, o)

if __name__ == '__main__':
    unittest.main()
