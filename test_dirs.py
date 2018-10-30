#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import os.path

from tonat.libtests import attr
from tonat.test_base import TestBase
from tonat.product.objects.mount import Mount, NfsVersion
from tonat.context import ctx
from tonat.dispatcher_tuple import dispatcher_tuple


class TestDirs(TestBase):

    @attr('dirs', name='create_remove_dir')
    def test_create_remove_dir(self, get_pd_share):
        share = get_pd_share
        mnt_tmpl = Mount(share, NfsVersion.pnfs)
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        dirnames = []
        for (client, path) in zip(ctx.clients, mounts.path):
            dirname = os.path.join(path, "dirA-" + client.address)
            dirnames.append(dirname)

        dirnames = dispatcher_tuple(dirnames)
        self._logger.debug('Creating directory')
        res = ctx.clients.makedir(dirnames, 0755)

        self._logger.debug('Removing directory')
        res = ctx.clients.rmdir(dirnames)

        self._logger.debug('Closing agent')
        ctx.clients.close_agent()

    @attr('dirs', name='list_dir')
    def test_list_dir(self, get_pd_share):
        share = get_pd_share
        mnt_tmpl = Mount(share, NfsVersion.pnfs)
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        dirnames = []
        for (client, path) in zip(ctx.clients, mounts.path):
            dirname = os.path.join(path, "dirA-" + client.address)
            dirnames.append(dirname)

        dirnames = dispatcher_tuple(dirnames)
        self._logger.info('Creating directory')
        res = ctx.clients.makedir(dirnames, 0755)

        self._logger.info('Listing directory')
        res = ctx.clients.readdir(dirnames)

        self._logger.info('Removing directory')
        res = ctx.clients.rmdir(dirnames)

        self._logger.debug('Closing agent')
        ctx.clients.close_agent()

    @attr('dirs', name='stat_dir')
    def test_stat_dir(self, get_pd_share):
        share = get_pd_share
        mnt_tmpl = Mount(share, NfsVersion.pnfs)
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        dirnames = []
        for (client, path) in zip(ctx.clients, mounts.path):
            dirname = os.path.join(path, "dirA-" + client.address)
            dirnames.append(dirname)

        dirnames = dispatcher_tuple(dirnames)
        self._logger.info('Creating directory')
        res = ctx.clients.makedir(dirnames, 0755)

        self._logger.info('Listing directory')
        res = ctx.clients.stat(dirnames)
        for expected_dir in dirnames:
            dir_found = False
            for actual_dir in res:
                actual_dir_basename = actual_dir['Name']
                expected_dir_basename = os.path.basename(expected_dir)
                if actual_dir_basename == expected_dir_basename:
                    self._logger.debug("Dir {0} found in actual results")
                    dir_found = True
                    break
            assert dir_found, "Directory {0} not found".format(expected_dir)
            assert actual_dir['IsDir'], "Entry {0} is not a directory".format(expected_dir)

        self._logger.info('Removing directory')
        res = ctx.clients.rmdir(dirnames)

        self._logger.debug('Closing agent')
        ctx.clients.close_agent()
