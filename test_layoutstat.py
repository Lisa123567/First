#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from __future__ import division
from flufl.enum import IntEnum
import os
import random
from copy import copy
from time import sleep
from os.path import join

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from tonat.dispatcher_tuple import dispatcher_tuple
from pd_tools.fio_caps import FioCaps


class IoType(IntEnum):
    READ = 0
    WRITE = 1
    READ_WRITE = 2


class Test_Layoutstat(TestBase):
    DB_UPDATE_INTERVAL = 60

    _FIO_GLOBALS = {'group_reporting': None, 'thread': None,
                    'ioengine': 'libaio', 'time_based': None, 
                    'direct': 1, 'stonewall': None}

    _FIO_IOPS_OPT = {'blocksize': '4K',
                    'rw': 'randrw',
                    'size': '2G'}

    def fuzzy_compare(self, base_value, test_value, tolerance_pct=10):
        '''
        base_value: Known value to compare the test_value against
        test_value: Unknown value to be fuzzily compared to the base_value
        tolerance_pct: percent of tolerance (fuzziness)
        '''
        res = True
        if tolerance_pct == 0:
            tolerance_value = 0 # Prevent division by zero
        else:
            tolerance_value = base_value/100*tolerance_pct

        min = base_value - tolerance_value
        max = base_value + tolerance_value
        if test_value < min or test_value > max:
            res = False
        self._logger.debug("Fuzzy compare of {} to {} with tolerance +/- {} (min: {}, max: {}) - res: {}".format(base_value, test_value, tolerance_pct, min, max, res))
        return res

    def nano2sec(self, time_nano):
        int(time_nano)
        return time_nano / 1000000000

    def fio_init(self, fname, runtime, direct=1, teardown=True):
        fio_cap = FioCaps(conf_file='fio-conf')
        conf = copy(self._FIO_GLOBALS)
        conf.update({'filename': fname, 'runtime': runtime, 'direct': direct})
        fio_cap.edit_conf_file(global_sec=conf, job_a=self._FIO_IOPS_OPT)
        self.fio = fio_cap.generic(teardown=teardown)

    def fio_run(self, mounts, fname, runtime, teardown=True):
        self._logger.debug("Initializing fio settings")
        self.fio_init(fname, runtime, teardown=teardown)
        self._logger.info("Running fio on file {} for {}".format(fname, runtime))
        fio_res = ctx.clients[0].execute_tool(self.fio, mounts[0])
        return fio_res

    def fio_init_rate_iops(self, fname, runtime, teardown):
        fio_cap = FioCaps(conf_file='fio-conf')
        conf = copy(self._FIO_GLOBALS)
        conf.update({'filename': fname, 'runtime': runtime, 'rate_iops': 100})
        fio_cap.edit_conf_file(global_sec=conf, job_a=self._FIO_IOPS_OPT)
        self.fio = fio_cap.generic(teardown=teardown)

    def fio_run_rate_ops(self, mounts, fname, runtime, teardown=True):
        self._logger.debug("Initializing fio settings")
        self.fio_init(fname, runtime, teardown=teardown)
        self._logger.info("Running fio on file {} for {}".format(fname, runtime))
        fio_res = ctx.clients[0].execute_tool(self.fio, mounts[0])
        return fio_res

    @attr('layoutstat', jira='PD-16364', name='iops_fio_write')
    def test_iops_fio_write(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "iops_fio_write_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        last_write_iops = fio_res.last_result.values()[0]['write_iops']

        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawWriteOpsCompleted = stats_entry.rawWriteOpsCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_write_iops (from fio) = 
        # TODO: Catch potential division by zero
        DBiops = LastRawWriteOpsCompleted / LastRawDuration
        self._logger.debug("Last LastRawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last LastRawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawWriteOpsCompleted: {}".format(LastRawWriteOpsCompleted))
        self._logger.debug("DB write_iops: {} (LastRawWriteOpsCompleted / LastRawDuration)".format(DBiops))
        self._logger.debug("FIO TOOL write_iops: {} ".format(last_write_iops))

        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        DBiops = int(DBiops)
        last_write_iops = int(last_write_iops)
        assert self.fuzzy_compare(DBiops, last_write_iops, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DBiops, last_write_iops, tolerance)

    @attr('layoutstat', jira='PD-16365', name='iops_fio_write_rate_ops')
    def test_iops_fio_write_rate_ops(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "iops_fio_write_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run_rate_ops(mounts, fname, runtime='121s')
        last_write_iops = fio_res.last_result.values()[0]['write_iops']

        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawWriteOpsCompleted = stats_entry.rawWriteOpsCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_write_iops (from fio) = 
        # TODO: Catch potential division by zero
        DBiops = LastRawWriteOpsCompleted / LastRawDuration
        self._logger.debug("Last LastRawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last LastRawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawWriteOpsCompleted: {}".format(LastRawWriteOpsCompleted))
        self._logger.debug("DB write_iops: {} (LastRawWriteOpsCompleted / LastRawDuration)".format(DBiops))
        self._logger.debug("FIO TOOL write_iops: {} ".format(last_write_iops))

        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        DBiops = int(DBiops)
        last_write_iops = int(last_write_iops)
        assert self.fuzzy_compare(DBiops, last_write_iops, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DBiops, last_write_iops, tolerance)

    @attr('layoutstat', jira='PD-16366', name='iops_fio_read')
    def test_iops_fio_read(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "iops_fio_read_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        last_read_iops = fio_res.last_result.values()[0]['read_iops']

        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawReadOpsCompleted = stats_entry.rawReadOpsCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_read_iops (from fio) = 
        # TODO: Catch potential division by zero
        DBiops = LastRawReadOpsCompleted / LastRawDuration
        self._logger.debug("Last rawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last rawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawReadOpsCompleted: {}".format(LastRawReadOpsCompleted))
        self._logger.debug("DB read_iops: {} (LastRawReadOpsCompleted / LastRawDuration)".format(DBiops))
        self._logger.debug("FIO TOOL read_iops: {} ".format(last_read_iops))
        DBiops = int(DBiops)
        last_read_iops = int(last_read_iops)
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DBiops, last_read_iops, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DBiops, last_read_iops, tolerance)

    @attr('layoutstat', jira='PD-16367', name='iops_fio_read_rate_ops')
    def test_iops_fio_read_rate_ops(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "iops_fio_read_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run_rate_ops(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        last_read_iops = fio_res.last_result.values()[0]['read_iops']

        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawReadOpsCompleted = stats_entry.rawReadOpsCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_read_iops (from fio) = 
        # TODO: Catch potential division by zero
        DBiops = LastRawReadOpsCompleted / LastRawDuration
        self._logger.debug("Last rawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last rawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawReadOpsCompleted: {}".format(LastRawReadOpsCompleted))
        self._logger.debug("DB read_iops: {} (LastRawReadOpsCompleted / LastRawDuration)".format(DBiops))
        self._logger.debug("FIO TOOL read_iops: {} ".format(last_read_iops))
        DBiops = int(DBiops)
        last_read_iops = int(last_read_iops)
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DBiops, last_read_iops, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DBiops, last_read_iops, tolerance)

    @attr('layoutstat', jira='PD-16368', name='bw_fio_read')
    def test_bw_fio_read(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "bw_fio_read_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_read_bw_kbs_mean = fio_res.last_result.values()[0]['read_bw_kbs_mean']
        fio_read_bw_kbs_mean = float (fio_read_bw_kbs_mean) * 1024
        
        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawReadBytesCompleted = stats_entry.rawReadBytesCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_read_iops (from fio) = 
        # TODO: Catch potential division by zero
        DB_bw = LastRawReadBytesCompleted / LastRawDuration
        self._logger.debug("Last rawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last rawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawReadOpsCompleted: {}".format(LastRawReadBytesCompleted))
        self._logger.debug("DB bw_read: {} (LastRawWriteOpsCompleted / LastRawWriteAggrCompletionTime)".format(DB_bw))
        self._logger.debug("FIO TOOL bw_read: {} ".format(fio_read_bw_kbs_mean))

        DB_bw = int(DB_bw)
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DB_bw, fio_read_bw_kbs_mean, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DB_bw, fio_read_bw_kbs_mean, tolerance)

    @attr('layoutstat', jira='PD-16369', name='bw_fio_write')
    def test_bw_fio_write(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "bw_fio_write_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)
        # Create file with size=2G using fio tool

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_write_bw_kbs_mean = fio_res.last_result.values()[0]['write_bw_kbs_mean']
        fio_write_bw_kbs_mean = float (fio_write_bw_kbs_mean) * 1024
        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawWriteBytesCompleted = stats_entry.rawWriteBytesCompleted[-1]
        LastRawDuration = self.nano2sec(stats_entry.rawDuration[-1])

        # Calculation  second_write_iops (from fio) = 
        # TODO: Catch potential division by zero
        DB_bw = LastRawWriteBytesCompleted / LastRawDuration
        self._logger.debug("Last rawDuration: {} nanosec".format(stats_entry.rawDuration[-1]))
        self._logger.debug("Last rawDuration: {} sec".format(LastRawDuration))
        self._logger.debug("Last LastRawWriteOpsCompleted: {}".format(LastRawWriteBytesCompleted))
        self._logger.debug("DB bw_write: {} (LastRawWriteOpsCompleted / LastRawWriteAggrCompletionTime)".format(DB_bw))
        self._logger.debug("FIO TOOL bw_write: {} ".format(fio_write_bw_kbs_mean))
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DB_bw, fio_write_bw_kbs_mean, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DB_bw, fio_write_bw_kbs_mean, tolerance)

    @attr('layoutstat', jira='PD-16370', name='latency_fio_read')
    def test_latency_fio_read(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "latency_fio_read_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)
        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_latency_read = fio_res.last_result.values()[0]['completion_read_mean']
        fio_latency_read = float(fio_latency_read)*1000 # Converted to nano
        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawReadOpsCompleted = stats_entry.rawReadOpsCompleted[-1]
        LastRawReadAggrCompletionTime = stats_entry.rawReadAggrCompletionTime[-1] # Time in nano

        # Calculation  second_read_iops (from fio) = 
        # TODO: Catch potential division by zero
        DB_latency_read = LastRawReadAggrCompletionTime / LastRawReadOpsCompleted
        self._logger.debug("Last rawReadAggrCompletionTime: {} nanosec".format(stats_entry.rawReadAggrCompletionTime[-1]))
        self._logger.debug("Last rawReadAggrCompletionTime: {} sec".format(LastRawReadAggrCompletionTime))
        self._logger.debug("Last LastRawReadOpsCompleted: {}".format(LastRawReadOpsCompleted))
        self._logger.debug("DB latency_read: {} (LastRawReadAggrCompletionTime / LastRawReadOpsCompleted)".format(DB_latency_read))
        self._logger.debug("FIO TOOL latency_read: {} ".format(DB_latency_read))
        DB_latency_read = int(DB_latency_read)
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DB_latency_read, fio_latency_read, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DB_latency_read, fio_latency_read, tolerance)

    @attr('layoutstat', jira='PD-16371', name='latency_fio_write')
    def test_latency_fio_write(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "latency_fio_write_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)
        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_latency_write = fio_res.last_result.values()[0]['completion_write_mean']
        fio_latency_write = float(fio_latency_write) * 1000 # Converting to nano
        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawWriteOpsCompleted = stats_entry.rawWriteOpsCompleted[-1]
        LastRawWriteAggrCompletionTime = stats_entry.rawWriteAggrCompletionTime[-1] # Time in nano
        # Calculation  second_write_iops (from fio) = 
        # TODO: Catch potential division by zero
        DB_latency_write = LastRawWriteAggrCompletionTime / LastRawWriteOpsCompleted
        self._logger.debug("Last rawWriteAggrCompletionTime: {} nanosec".format(stats_entry.rawWriteAggrCompletionTime[-1]))
        self._logger.debug("Last rawWriteAggrCompletionTime: {} sec".format(LastRawWriteAggrCompletionTime))
        self._logger.debug("Last LastRawWriteOpsCompleted: {}".format(LastRawWriteOpsCompleted))
        self._logger.debug("DB latency_write: {} (LastRawWriteAggrCompletionTime / LastRawWriteOpsCompleted)".format(DB_latency_write))
        self._logger.debug("FIO TOOL latency_write: {} ".format(fio_latency_write))
        DB_latency_write = int(DB_latency_write)
        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        assert self.fuzzy_compare(DB_latency_write, fio_latency_write, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(DB_latency_write, fio_latency_write, tolerance)

    def _do_io(self, fds, io_type, size, delay=0, pattern=1):
        self._logger.debug('Going to perform I/O type {}, size {}, delay {}'.format(io_type, size, delay))
        if io_type == IoType.WRITE or io_type == IoType.READ_WRITE:
            ctx.clients.write_file(fds, 0, size, pattern)
            ctx.clients.drop_caches() # N/A for direct I/O
        elif io_type == IoType.READ or io_type == IoType.READ_WRITE:
            ctx.clients.read_file(fds, 0, size, pattern)
            ctx.clients.drop_caches() # N/A for direct I/O           
        sleep(delay)
 
    def io_spread(self, fds, io_type=IoType.WRITE, blocks_count=32, report_interval=15, interval_count=1, pattern=1):
        '''
        params:
        @fds: File descriptors
        @io_type: Type of I/O (see IoType enum)
        @blocks_count: Number of blocks (per I/O type) to spread over report_interval
        @report_interval: Interval at which the client reports LAYOUTSTATS
        @interval_count: Number of report intervals to spread I/O over
        '''

        res = {}
        intervals=[] # I/O is issued at these intervals
        intervals_sum=0
        max_time=interval_count*report_interval-1 # FIXME: Account for I/O delays so that we never go over report_interval
        while intervals_sum < max_time:
            interval=random.randint(1, max_time-intervals_sum)
            intervals.append(interval)
            intervals_sum += interval
        io_count = len(intervals)

        # Preallocate list
        blocks=blocks_count*[0] # I/O size to be used at its corresponding interval 
        blocks_sum = blocks_count-1 # Compensate for the final single-block I/O
        while blocks_sum > 0:
            pos = random.randint(0, io_count)
            blocks[pos] += 1
            blocks_sum -= 1
            # TODO: limit any blocks size to be less than r/w size (can be an arbitrary low number, e.g. 8 blocks)

        for i in xrange(io_count):
            self._do_io(fds, io_type, blocks[i], intervals[i], pattern=pattern)

        sleep(report_interval) # Triggers the final LAYOUTSTATS
        self._do_io(fds, io_type, 1, pattern=pattern)  # must be 1 
        # Last I/O will be accounted for in Requested fields of LAYOUTSTATS as this is what triggers the last LAYOUTSTATS
        res['io_count_requested'] = io_count+1
        # Last I/O will not be accounted for in Completed fields of LAYOUTSTATS as it hasn't been acknowledged by the server yet
        res['io_count_completed'] = io_count

        return res

    @attr('layoutstat', jira='PD-16372', name='write_requested_ops_dio')
    def test_write_requested_ops_dio(self, get_pnfs4_2_mount_points):
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "write-test1-" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)
        fds = ctx.clients.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        res = self.io_spread(fds, io_type=IoType.WRITE, blocks_count=1024) 
        self._logger.debug("io_count_requested: {}".format(res['io_count_requested']))
        ctx.clients.close_file(fds)
        ctx.clients.close_agent()

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))
        sleep(self.DB_UPDATE_INTERVAL) 
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
            stats_entry=stats[0]
            self._logger.debug('rawWriteOpsRequested LayoutStats:', stats_entry.rawWriteOpsRequested)
            assert stats_entry.rawWriteOpsRequested[-1] == res['io_count_requested'], 'inode {} write_requested_ops performed by test {} LAYOUTSTATS reported {}'.format(inode, res['io_count_requested'], stats_entry.rawWriteOpsRequested[-1])

    @attr('layoutstat', jira='PD-16373', name='read_requested_ops_dio')
    def test_read_requested_ops_dio(self, get_pnfs4_2_mount_points):
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "read-test1-" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)
        fds = ctx.clients.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        ctx.clients.write_file(fds, 0, 1024, 1)
        res = self.io_spread(fds, io_type=IoType.READ, blocks_count=1024)
        self._logger.debug("io_count_requested: {}".format(res['io_count_requested']))

        ctx.clients.close_file(fds)
        ctx.clients.close_agent()

        inode = client.file_inode(fname) # TODO: Complete support for multiple clients.
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))
        sleep(self.DB_UPDATE_INTERVAL)    
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
            stats_entry=stats[0]
            self._logger.debug('rawReadOpsRequested LayoutStats:', stats_entry.rawReadOpsRequested)
            assert stats_entry.rawReadOpsRequested[-1] == res['io_count_requested'], 'inode {} read_requested_ops performed by test {} LAYOUTSTATS reported {}'.format(inode, res['io_count_requested'], stats_entry.rawReadOpsRequested[-1])

    @attr('layoutstat', jira='PD-16374', name='write_completed_ops_dio')
    def test_write_completed_ops_dio(self, get_pnfs4_2_mount_points):
        mounts = get_pnfs4_2_mount_points
        fnames = []

        # TODO: cleanup existing files
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "write-test1-" + client.address)
            fnames.append(fname)

        fnames = dispatcher_tuple(fnames)
        fds = ctx.clients.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        # Loop for 4 WriteOpsCompleted and one WriteOpsRequested 
        res = self.io_spread(fds, io_type=IoType.WRITE, interval_count=4, blocks_count=1024)
        self._logger.debug("io_count_completed: {}".format(res['io_count_completed']))
        ctx.clients.close_file(fds)
        ctx.clients.close_agent()

        inode = client.file_inode(fname)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode)) 
        sleep(self.DB_UPDATE_INTERVAL)
        stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry=stats[0]
        self._logger.debug('rawWriteOpsCompleted LayoutStats:', stats_entry.rawWriteOpsCompleted)
        assert stats_entry.rawWriteOpsCompleted[-1] == res['io_count_completed'], 'inode {} (completed) write ops performed by test {} LAYOUTSTATS reported {}'.format(inode, res['io_count_completed'], stats_entry.rawWriteOpsCompleted[-1])

    @attr('layoutstat', jira='PD-16375', name='read_completed_ops_dio')
    def test_read_completed_ops_dio(self, get_pnfs4_2_mount_points):
        mounts = get_pnfs4_2_mount_points
        fnames = []

        # TODO - cleanup to existing file
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "write-test1-" + client.address)
            fnames.append(fname)

        fnames = dispatcher_tuple(fnames)
        fds = ctx.clients.open_file(fnames, os.O_CREAT | os.O_RDWR | os.O_SYNC | os.O_DIRECT, 0644)
        ctx.clients.write_file(fds, 0, 1024, 1)
        # Loop for 4 WriteOpsCompleted and one WriteOpsRequested 
        res = self.io_spread(fds, io_type=IoType.READ, interval_count=4, blocks_count=1024)
        self._logger.debug("io_count_completed: {}".format(res['io_count_completed']))
        ctx.clients.close_file(fds)
        ctx.clients.close_agent()

        inode = client.file_inode(fname)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))
        sleep(self.DB_UPDATE_INTERVAL)
        stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry=stats[0]
        self._logger.debug('rawReadOpsCompleted LayoutStats:', stats_entry.rawReadOpsCompleted)
      
        assert stats_entry.rawReadOpsCompleted[-1] == res['io_count_completed'], 'inode {} (completed) read ops performed by test {} LAYOUTSTATS reported {}'.format(inode, res['io_count_completed'], stats_entry.rawReadOpsCompleted[-1])

    @attr('layoutstat', jira='PD-16376', name='fio_write_byte_completed')
    def test_fio_write_byte_completed(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "fio_write_byte_completed_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_write_completed = fio_res.last_result.values()[0]['write_total_io_kb']
        fio_write_completed = float (fio_write_completed) * 1024
        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawWriteByteCompleted = stats_entry.rawWriteBytesCompleted[-1]

        # Calculation  second_write_iops (from fio) = 
        # TODO: Catch potential division by zero
        self._logger.debug("DB_RawWriteByteCompleted: {}".format(LastRawWriteByteCompleted))
        self._logger.debug("FIO TOOL write_byte_completed: {} ".format(fio_write_completed))

        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        fio_write_completed = float (fio_write_completed)
        assert self.fuzzy_compare(LastRawWriteByteCompleted, fio_write_completed, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(LastRawWriteByteCompleted, fio_write_completed, tolerance)

    @attr('layoutstat', jira='PD-16377', name='fio_read_byte_completed')
    def test_fio_read_byte_completed(self, get_pnfs4_2_mount_points):
        # Create mount and file name
        mounts = get_pnfs4_2_mount_points
        fnames = []
        inodes = []
        tolerance = 15 # %
        ctx.clients[0].mkdirs(mounts[0].path + '/test_layoutstat')

        # TODO: Add support for multiple clients
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = join(path + '/test_layoutstat', "fio_read_byte_completed_" + client.address)
            fnames.append(fname)
        fnames = dispatcher_tuple(fnames)

        # Create file with size=2G using fio tool
        self._logger.info("Creating and laying out file")
        self.fio_run(mounts, fname, runtime='1s', teardown=False)

        inode = client.file_inode(fname)
        inodes.append(inode)
        self._logger.debug('fname {}, inode: {}'.format(fname, inode))

        # TODO: Check if sleep is needed
        # TODO: Check if the sleep doesn't cover up a delta calculation bug
        sleep(self.DB_UPDATE_INTERVAL)

        # Running fio-with existing file again and take results from fio tools
        self._logger.info("Performing I/O")
        fio_res = self.fio_run(mounts, fname, runtime='121s')
        fio_read_completed = fio_res.last_result.values()[0]['read_total_io_kb']
        fio_read_completed = float (fio_read_completed) * 1024

        self._logger.debug("Waiting for {}".format(self.DB_UPDATE_INTERVAL))
        sleep(self.DB_UPDATE_INTERVAL)

        self._logger.debug("Fetching DB values")
        for inode in inodes:
            stats = ctx.cluster.influx_db.layout_stats(inode=inode)
        stats_entry = stats[0]
        LastRawReadByteCompleted = stats_entry.rawReadBytesCompleted[-1]

        # Calculation  second_read_iops (from fio) = 
        # TODO: Catch potential division by zero
        self._logger.debug("DB_RawReadByteCompleted: {}".format(LastRawReadByteCompleted))
        self._logger.debug("FIO TOOL read_byte_completed: {} ".format(fio_read_completed))

        self._logger.info("Comparing values returned by fio to LAYOUTSTATS sent to InfluxDB")
        fio_read_completed = float (fio_read_completed)
        assert self.fuzzy_compare(LastRawReadByteCompleted, fio_read_completed, tolerance), \
            "Comparing {} to {} with tolerance {}% failed".format(LastRawReadByteCompleted, fio_read_completed, tolerance)
