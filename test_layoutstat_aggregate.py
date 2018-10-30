# __author__ = Elad Sofer

from __future__ import division
from os.path import join

from gevent import sleep

from tonat.context import ctx
from tonat.libtests import attr
from tonat.test_base import TestBase
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.greenlet_management import blockability
from tonat.utils import convert_data_size_generic, convert_time_units

from pd_tools.fio_caps import FioCaps


class TestAggregatePerformance(TestBase):
    RUNTIME = 183
    LAYOUTSTAT_DELAY = 15
    SIZE = "5M"
    FILE_NUMBER = 10
    DEVIATION_RATIO = 0.1
    LAYOUTSTATS_ROWS = int(RUNTIME / LAYOUTSTAT_DELAY)
    ERROR_MESSAGE = "The deviation on DS ID {0} between InfluxDB {1} and " \
                    "FIO_Result {2} is more than {3}"
    _FIO_GLOBALS = {'group_reporting': None, 'thread': None,
                    'ioengine': 'libaio', 'time_based': None,
                    'runtime': RUNTIME,
                    'direct': 1, 'stonewall': None,
                    'filesize': SIZE, 'rate_iops': 250, 'iodepth': 256}

    DB_UPDATE_INTERVAL = 120

    @staticmethod
    def truncate_function(client, paths, share, size):
        """
        :param client: the client object
        :param paths: the file paths which related to the client
        :param share: The share related to the client
        :param size: determines the file of each file
        :return: dictionary of ds id vs file paths which reside in that ds.
        """
        path_ds_dict = {}
        for file_path in paths:
            client.truncate(file_path, size)
            fd = client.file_declare(file_path, share)
            ds_id = fd.instances[0].data_store.internal_id
            if ds_id in path_ds_dict:
                path_ds_dict[ds_id].append(file_path)
            else:
                path_ds_dict[ds_id] = [file_path, ]
        return path_ds_dict

    @blockability
    def agg_multiple_ds_single_client(self, mount,
                                      test_type, fio_parameter,
                                      influx_parameter, files_per_client,
                                      client=None):
        """
        :param mount: the mount object related to the single client
        :param test_type:string, which kind of operations to perform -
        write/read ops
        :param fio_parameter: string, parameter to extract from FIO results
        :param influx_parameter: string, parameter to extract from Influx DB
        results
        :param files_per_client: int, how many files to create per client
        :return: dictionary contains FIO and InfluxDB aggregated results per DS.
        """
        client_results = {}
        client = client or ctx.clients[0]
        absolute_path = join(mount.path, "layoutstat", client.address)
        with self.step("Open directory, and run truncate."):
            client.mkdirs(absolute_path)
            file_names = [join(absolute_path, 'file' + str(file_index))
                          for file_index in range(files_per_client)]
            ds_id_path_dict = self.truncate_function(client, file_names,
                                                     mount.share, self.SIZE)

        with self.step("Run FIO & Gather statistics about the files."):
            fio_cap = FioCaps(conf_file='layoutstat-aggregate-fio-conf')
            for ds_id in ds_id_path_dict:
                fio_cap.edit_conf_file(global_sec=self._FIO_GLOBALS,
                                       job1={'rw': test_type, 'nrfiles': len(
                                           ds_id_path_dict[ds_id]),
                                             'filename': ':'.join
                                             (ds_id_path_dict[ds_id])})
                fio = fio_cap.generic()
                self._logger.info("client {0} is starting FIO for DS ID {1}".
                                  format(client.address, str(ds_id)))
                fio_res = client.execute_tool(fio, mount)
                ds_fio_res = float(fio_res.last_result.values()
                                   [0][fio_parameter])
                # Wait until DB is up to date.
                sleep(self.DB_UPDATE_INTERVAL)
                # Extracting the related rows from the Influx DB .
                ds_influx_res = getattr(
                    ctx.cluster.influx_db.aggregate_performance
                    (type='ds', id=str(ds_id), order_by='time desc',
                     limit=str(self.LAYOUTSTATS_ROWS))[0], influx_parameter)
                # Calculating average
                ds_influx_res = sum(ds_influx_res) / self.LAYOUTSTATS_ROWS
                # Units conversion from nanoSeconds to Seconds
                ds_influx_res = convert_time_units \
                    (ds_influx_res, source_units='N', dest_units='S')
                client_results[ds_id] = (ds_fio_res, ds_influx_res)
                self._logger.info("The results on client {0} for DS ID {1} "
                                  "is DS_Agg_Influx: {2} & DS_FIO: {3}".
                                  format(client.address, str(ds_id),
                                         ds_influx_res, ds_fio_res))

            return client_results

    def agg_multiple_ds_multiple_client(self, mounts, test_type, fio_parameter,
                                        influx_parameter, files_per_client):
        """
        :param mounts: the mounts related to the clients
        :param test_type:string, which kind of operations to perform -
        write/read ops
        :param fio_parameter: string, parameter to extract from FIO results
        :param influx_parameter: string, parameter to extract from Influx DB
        results
        :param files_per_client: int, how many files to create per client
        :return: dictionary contains FIO and InfluxDB aggregated results per DS.
        """
        agg_fio = {}
        agg_influx = {}
        clients_results = []
        clients = ctx.clients
        for client, mount in zip(clients, mounts):
            clients_results.append(self.agg_multiple_ds_single_client(
                mount, test_type, fio_parameter, influx_parameter,
                files_per_client, client, block=False))
        clients_results = dispatcher_tuple(clients_results).get()
        for client_result in clients_results:
            for (ds_id, (fio_res, influx_res)) in client_result.items():
                if ds_id in agg_fio:
                    agg_fio[ds_id] += fio_res
                else:
                    agg_fio[ds_id] = fio_res
                agg_influx[ds_id] = influx_res
        return agg_fio, agg_influx

    @attr('layoutstat', jira='PD-17568',
          name='agg_multiple_ds_single_client_wr_bw')
    def test_agg_multiple_ds_single_client_wr_bw(self, get_pnfs4_2_mount_points):

        """
        A test which execute random write ops on a single client & multiple DSX.
        The test checks if the write BW is equal between the Influx DB & FIO
        results.
        """

        mount = get_pnfs4_2_mount_points[0]
        ds_results = self.agg_multiple_ds_single_client(
            mount=mount, test_type="randwrite",
            fio_parameter="write_bandwidth_kb/sec",
            influx_parameter="totalWriteBwActual",
            files_per_client=self.FILE_NUMBER)

        for (ds_id, (fio_res, influx_res)) in ds_results.items():
            influx_res = convert_data_size_generic(influx_res, source_units='K',
                                                   dest_units='B')
            assert abs(fio_res - influx_res) < (self.DEVIATION_RATIO *
                                                fio_res), self.ERROR_MESSAGE.format(
                ds_id, influx_res,
                fio_res, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17569',
          name='agg_multiple_ds_single_client_re_bw')
    def test_agg_multiple_ds_single_client_re_bw(self, get_pnfs4_2_mount_points):
        """
        A test which execute random read ops on a single client & multiple DSX.
        The test checks if the read BW is equal between the Influx DB & FIO
        results.
        """

        mount = get_pnfs4_2_mount_points[0]
        ds_results = self.agg_multiple_ds_single_client(
            mount=mount, test_type="read",
            fio_parameter="read_bandwidth_kb/sec",
            influx_parameter="totalReadBw", files_per_client=self.FILE_NUMBER)

        for (ds_id, (fio_res, influx_res)) in ds_results.items():
            influx_res = convert_data_size_generic(influx_res, source_units='K',
                                                   dest_units='B')
            assert abs(fio_res - influx_res) < (self.DEVIATION_RATIO *
                                                fio_res), self.ERROR_MESSAGE.format(
                ds_id, influx_res,
                fio_res, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17570',
          name='agg_multiple_ds_single_client_wr_iops')
    def test_agg_multiple_ds_single_client_wr_iops(self, get_pnfs4_2_mount_points):
        """
        A test which execute random write ops on a single client & multiple DSX.
        The test checks if the write iops is equal between the Influx DB & FIO
        results.
        """

        mount = get_pnfs4_2_mount_points[0]
        ds_results = self.agg_multiple_ds_single_client(
            mount=mount, test_type="randwrite", fio_parameter="write_iops",
            influx_parameter="totalWriteOpRateActual",
            files_per_client=self.FILE_NUMBER)

        for (ds_id, (fio_res, influx_res)) in ds_results.items():
            assert abs(fio_res - influx_res) < (self.DEVIATION_RATIO *
                                                fio_res), self.ERROR_MESSAGE.format(
                ds_id, influx_res,
                fio_res, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17571',
          name='agg_multiple_ds_single_client_re_iops')
    def test_agg_multiple_ds_single_client_re_iops(self, get_pnfs4_2_mount_points):
        """
        A test which execute random read ops on a single client & multiple DSX.
        The test checks if the read iops is equal between the Influx DB & FIO
        results.
        """

        mount = get_pnfs4_2_mount_points[0]
        ds_results = self.agg_multiple_ds_single_client(
            mount=mount, test_type="read", fio_parameter="read_iops",
            influx_parameter="totalReadOpRate",
            files_per_client=self.FILE_NUMBER)

        for (ds_id, (fio_res, influx_res)) in ds_results.items():
            assert abs(fio_res - influx_res) < (self.DEVIATION_RATIO *
                                                fio_res), self.ERROR_MESSAGE.format(
                ds_id, influx_res,
                fio_res, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17572',
          name='agg_multiple_ds_multiple_client_wr_bw')
    def test_agg_multiple_ds_multiple_client_wr_bw(self, get_pnfs4_2_mount_points):
        """
        A test which execute random write ops on multiple clients & multiple
        DSX.
        The test checks if the write BW is equal between the Influx DB
        AggregatePerformance Table & FIO results.
        """
        mounts = get_pnfs4_2_mount_points
        fio_agg, influx_agg = self.agg_multiple_ds_multiple_client(
            mounts=mounts, test_type="randwrite",
            fio_parameter="write_bandwidth_kb/sec",
            influx_parameter="totalWriteBwActual",
            files_per_client=self.FILE_NUMBER)
        for ds_id, agg_fio in fio_agg.items():
            # Units conversion from nanoSeconds to Seconds & Bytes to kiloBytes
            influx_agg[ds_id] = convert_data_size_generic \
                (influx_agg[ds_id], source_units='K', dest_units='B')

            assert abs(agg_fio - influx_agg[ds_id]) < \
                   (self.DEVIATION_RATIO * agg_fio), self.ERROR_MESSAGE.format(
                ds_id, influx_agg[ds_id], agg_fio, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17573',
          name='agg_multiple_ds_multiple_client_re_bw')
    def test_agg_multiple_ds_multiple_client_re_bw(self, get_pnfs4_2_mount_points):
        """
        A test which execute random read ops on multiple clients & multiple DSX.
        The test checks if the read BW is equal between the Influx DB
        AggregatePerformance Table & FIO results.
        """
        mounts = get_pnfs4_2_mount_points
        fio_agg, influx_agg = self.agg_multiple_ds_multiple_client(
            mounts=mounts, test_type="read",
            fio_parameter="read_bandwidth_kb/sec",
            influx_parameter="totalReadBw",
            files_per_client=self.FILE_NUMBER)
        for ds_id, agg_fio in fio_agg.items():
            # Units conversion from nanoSeconds to Seconds & Bytes to kiloBytes
            influx_agg[ds_id] = convert_data_size_generic \
                (influx_agg[ds_id], source_units='K', dest_units='B')

            assert abs(agg_fio - influx_agg[ds_id]) < \
                   (self.DEVIATION_RATIO * agg_fio), self.ERROR_MESSAGE.format(
                ds_id, influx_agg[ds_id], agg_fio, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17574',
          name='agg_multiple_ds_multiple_client_wr_iops')
    def test_agg_multiple_ds_multiple_client_wr_iops(self, get_pnfs4_2_mount_points):
        """
        A test which execute random write ops on a multiple clients & multiple
        DSX.
        The test checks if the write iops is equal between the Influx DB & FIO
        results.
        """
        mounts = get_pnfs4_2_mount_points
        fio_agg, influx_agg = self.agg_multiple_ds_multiple_client(
            mounts=mounts, test_type="randwrite",
            fio_parameter="write_iops",
            influx_parameter="totalWriteOpRateActual",
            files_per_client=self.FILE_NUMBER)
        for ds_id, agg_fio in fio_agg.items():
            assert abs(agg_fio - influx_agg[ds_id]) < \
                   (self.DEVIATION_RATIO * agg_fio), self.ERROR_MESSAGE.format(
                ds_id, influx_agg[ds_id], agg_fio, self.DEVIATION_RATIO)
        ctx.clients.close_agent()

    @attr('layoutstat', jira='PD-17575',
          name='agg_multiple_ds_multiple_client_re_iops')
    def test_agg_multiple_ds_multiple_client_re_iops(self, get_pnfs4_2_mount_points):
        """
        A test which execute random read ops on a multiple clients & multiple
        DSX.
        The test checks if the write iops is equal between the Influx DB & FIO
        results.
        """
        mounts = get_pnfs4_2_mount_points
        fio_agg, influx_agg = self.agg_multiple_ds_multiple_client(
            mounts=mounts, test_type="randread",
            fio_parameter="read_iops",
            influx_parameter="totalReadOpRate",
            files_per_client=self.FILE_NUMBER)
        for ds_id, agg_fio in fio_agg.items():
            assert abs(agg_fio - influx_agg[ds_id]) < \
                   (self.DEVIATION_RATIO * agg_fio), self.ERROR_MESSAGE.format(
                ds_id, influx_agg[ds_id], agg_fio, self.DEVIATION_RATIO)
        ctx.clients.close_agent()
