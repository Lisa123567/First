#!/usr/bin/env python

__author__ = 'Avi Tal <avi@primarydata.com>'
__date__ = 'Feb 5, 2014'

import shutil

from functools import partial
from os import mkdir
from os.path import exists, join as path_join

from tonat.singleton import Singleton
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.product.nodes.cluster import Cluster
from tonat.product.nodes.ds import get_correct_ds
from tonat.product.nodes.client import Client
from tonat.product.nodes.server import Server
from tonat.product.nodes.data_portal import DataPortal
from tonat.product.nodes.da_ds import DaDs
from tonat.product.nodes.hypervisor import Hypervisor
from tonat.product.nodes.ds.pd import PdDs
from tonat.settings import TMP_PATH


LOADER_PREFIX = '_loader_'


class EnvContext(object):
    '''
    :summary: an object container which holds all the
    SUT objects under one Environment singleton class.
    the idea is to make it easier to explore SUT and make use of
    it within a test
    '''
    __metaclass__ = Singleton

    def __init__(self):
        self.__servers = dispatcher_tuple([])
        self.__clients = dispatcher_tuple([])
        self.__cluster = None
        self.__data_stores = dispatcher_tuple([])
        self.__data_portals = dispatcher_tuple([])
        self.__da_dss = dispatcher_tuple([])
        self.__variables = None
        self.__seed = None
        self.__working_dir = None
        self._results = []

    def __repr__(self):
        repr_template = ("<{0.__class__.__module__}.{0.__class__.__name__}"
                         " object at {1}>")
        return repr_template.format(self, hex(id(self)))

    # convenience way of getting the first item of a sequence if the attribute
    # name ends with an s, so you can do ctx.data_director instead of
    # ctx.data_directors[0]
    def __getattr__(self, name):
        plural = name + 's'
        if hasattr(self, plural):
            try:
                return getattr(self, plural)[0]
            except (IndexError, TypeError):
                pass
        raise AttributeError(
            '{!r} object has no attribute {!r}'.format(
                self.__class__.__name__, name))

    @property
    def servers(self):
        return self.__servers

    @property
    def clients(self):
        return self.__clients

    @clients.setter
    def clients(self, clients):
        self.__clients = clients

    @property
    def data_directors(self):
        return self.__cluster.data_directors \
            if self.__cluster else dispatcher_tuple([])

    @property
    def hypervisors(self):
        return self.__hypervisors

    @property
    def cluster(self):
        return self.__cluster

    @cluster.setter
    def cluster(self, cluster):
        self.__cluster = cluster

    @property
    def data_stores(self):
        return self.__data_stores

    @property
    def data_portals(self):
        return self.__data_portals

    @property
    def vars(self):
        return self.__variables

    @property
    def seed(self):
        return self.__seed

    @property
    def working_dir(self):
        return self.__working_dir

    @property
    def results(self):
        return self._results

    #############
    # loaders: these are private methods to load env json entities.
    # prefix by _loader_<entity name> The entity name MUST be identical to
    # the json entity name.

    def _loader_servers(self, attr):
        servers = [Server(**cl) for cl in attr]
        self.__servers = dispatcher_tuple(servers)

    def _loader_clients(self, attr):
        clients = [Client(cluster=self.cluster, **cl) for cl in attr]
        self.__clients = dispatcher_tuple(clients)

    def _loader_da_dss(self, attr):
        da_dss = [DaDs(**cl) for cl in attr]
        self.__da_dss = dispatcher_tuple(da_dss)

    def _loader_data_directors(self, attr, use_cache=True):
        if attr:
            self.__cluster = Cluster(attr, use_cache=use_cache)

    def _loader_data_stores(self, attr):
        data_stores = [get_correct_ds(
            node_update=partial(self.cluster.rest.node_list, use_cache=False),
            **cl) for cl in attr]
        self.__data_stores = dispatcher_tuple(data_stores)

    def _loader_data_portals(self, attr):
        data_portals = [DataPortal(
            node_update=partial(self.cluster.rest.node_list, use_cache=False),
            **cl) for cl in attr]
        self.__data_portals = dispatcher_tuple(data_portals)

    def _loader_hypervisors(self, attr):
        hypervisors = [Hypervisor(**cl) for cl in attr]
        self.__hypervisors = dispatcher_tuple(hypervisors)

    def load(self, environment, variables, seed, protocols,
             use_cluster_cache=True):
        '''
        :summary: Loading environment from a dict object.
        :param environment: A dict describing the environment.
        :param variables: environment variables
        :param seed: run random seed
        :param use_cluster_cache: whether to cache cluster objs
        '''
        # This order is important, Cluster needs to be loaded first
        load_list = ['data_directors', 'servers', 'data_stores', 'clients',
                     'data_portals', 'hypervisors', 'da_dss']

        for entity in load_list:
            func = getattr(self, '{prefix}{name}'.
                           format(prefix=LOADER_PREFIX, name=entity))
            if entity in environment:
                kwargs = (dict(use_cache=use_cluster_cache)
                          if entity == 'data_directors'
                          else dict())
                func(environment.get(entity), **kwargs)

        # create a class with all variables defined in the json file.
        self.__variables = type('variables', (object,), variables)
        if protocols:
            setattr(self.__variables, 'ask_proto', protocols)
        self.__seed = seed

    def _query_obj(self, arr, key, value):
        '''
        :summary: abstract method for query any object from list
        :param arr: objects list
        :param key: object attribute
        :param value: attribute value
        :return: first match object
        '''
        found = None
        for o in arr:
            if hasattr(o, key):
                if value == getattr(o, key):
                    found = o
                    break
        return found

    def get_client(self, attr, value):
        ''':summary: query client by any attribute and value'''
        return self._query_obj(self.clients, attr, value)

    def get_ds(self, attr, value):
        ''':summary: query data stores by any attribute and value'''
        return self._query_obj(self.data_stores, attr, value)

    def get_active_dd(self):
        '''
        :summary: get HA active DD checking heartbeat
        '''
        return self.cluster.get_active_dd()

    def pd_node_setup(self, server, **kwargs):
        ret = server.pd_node_setup(**kwargs)
        if kwargs.get('configure_dd'):
            if self.cluster:
                self.cluster.add_server_to_cluster(server)
            else:
                self.__cluster = Cluster.create_instance(server)
        elif kwargs.get('configure_ds'):
            self.__data_stores += PdDs(server.conn)
        elif kwargs.get('configure_dp'):
            self.__data_portals += DataPortal.create_instance(server)
        return ret

    def create_working_dir(self, exec_id):
        self.__working_dir = path_join(TMP_PATH, exec_id)
        if not exists(self.__working_dir):
            mkdir(self.__working_dir)
        return self.working_dir

    def archive_working_dir(self):
        ret = shutil.make_archive(self.__working_dir, 'gztar', self.__working_dir)
        shutil.rmtree(self.__working_dir)
        self.__working_dir = None
        return ret

ctx = EnvContext()
