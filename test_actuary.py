__author__ = 'Sudhakar Sukumar'

from os.path import join
from time import sleep

from pytest import mark
from tonat.context import ctx
from tonat.test_base import TestBase, attr
from tonat.constants import fileflags

# File size in MB
FILE_SIZE = 10
IDLE_TIME = 600

class TestActuary(TestBase):

    @mark.skipif(len(ctx.data_stores) < 2, reason='Insufficient resources - Supply at least 2 DSes')
    @attr(jira='PD-17091', name='test_idle_immobility')
    def test_idle_immobility(self, get_mount_points):
        """
        Create few files on each DS and make sure they don't move after few minutes of idle time.
        SZ2.1 Test - Actuary doesn't make any changes on a idle system.
        """
        # Create few files on DS1 and DS2
        total_files = 0
        files_needed_on_each_ds = 5
        ds_count = 0
        filecount = {}

        mount = get_mount_points[0]

        for ds in ctx.cluster.data_stores.values():
            self._logger.info('DS ID: %s' % ds.internal_id)
            # Initialize the dictionary with key as internal_id
            filecount[ds.internal_id] = 0
            ds_count += 1

        # list to store file information
        file_info = [[0 for x in range(3)] for x in range(100)]
        created_enough_files = False

        while not created_enough_files:
            file_name1 = join(mount.path, 'idle_file_%d' % total_files)

            with self.step('Create an idle file on the DS'):
                ctx.clients[0].write_to_file(file_name1, bs='1M', count=FILE_SIZE,
                                             oflag='append', conv='notrunc')

            fd = ctx.clients[0].file_declare(file_name1, mount.share)
            ds_name1 = fd.instances[0].data_store.name
            ds_id = fd.instances[0].data_store.internal_id
            filecount[ds_id] += 1

            file_info[total_files] = [file_name1, ds_id, ds_name1]
            total_files += 1

            # Check if we got enough number of files created on each DS
            created_enough_files = True
            for key, value in filecount.iteritems():
                if filecount[key] < files_needed_on_each_ds:
                    created_enough_files = False
                    break

        self._logger.info("===============================")
        self._logger.info("Initial placement of files:")
        for loop in range(0, --total_files):
            self._logger.info("File %s is located on DS %d:" % (file_info[loop][0], file_info[loop][1]))
        self._logger.info("===============================")

        # sleep to keep the system idle for some time
        self._logger.info("Sleep for %d secs and then verify that there is NO file movement in an IDLE system"
                          % IDLE_TIME)
        sleep(IDLE_TIME)

        # Verify none of the files moved in an IDLE system
        for loop in range(0, total_files):
            fd = ctx.clients[0].file_declare(file_info[loop][0], mount.share)
            ds_id2 = fd.instances[0].data_store.internal_id
            assert file_info[loop][1] == ds_id2, 'FAILURE: File %s MOVED to a different DS %d in an IDLE system:' \
                                                 % (file_info[loop][0], ds_id2)

        self._logger.info("==================================================")
        self._logger.info('SUCCESS: None of the files moved in an IDLE system')
        self._logger.info("==================================================")
