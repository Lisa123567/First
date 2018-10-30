# __author__ = 'Elad Sofer'
from __future__ import division

from flufl import enum
import inspect

from os.path import join
from tonat.context import ctx
from tonat.errors import CliOperationFailed
from tonat.libtests import attr
from tonat.test_base import TestBase
from tonat.utils import get_rsd

from pytest import yield_fixture, mark


class Obj(enum.Enum):
    DONT = 'DONT'
    MEET = 'MEET'


class VolCaps(object):
    """
    A class which represent user friendly volume's capabilities
    """
    # A dictionary which converts the basic objective params between
    # pdcli's conventions to tonat's convention
    PARAMS_MAPPER = {'min_write_iops': 'write_iops',
                     'min_write_throughput': 'write_bw',
                     'max_write_response_time': 'write_latency',
                     'min_read_iops': 'read_iops',
                     'min_read_throughput': 'read_bw',
                     'max_read_response_time': 'read_latency'}

    def __init__(self, vol):
        self.vol_id = str(vol.internal_id)
        performance = vol.storage_capabilities.performance
        self.write_iops = performance.write_performance.iops
        self.write_latency = performance.write_performance.latency
        self.write_bw = performance.write_performance.bandwidth
        self.read_iops = performance.read_performance.iops
        self.read_latency = performance.read_performance.latency
        self.read_bw = performance.read_performance.bandwidth
        self.agg_perf = ctx.cluster.influx_db.aggregate_performance

    @staticmethod
    def basic_cap_by_method(vols, parameter, method, **kwargs):
        return method([(getattr(VolCaps(vol), parameter), vol.internal_id)
                       for vol in vols], **kwargs)

    def layout_cap_by_method(self, vols, parameter, method, **kwargs):
        return method([getattr(self.agg_perf(ds=ds.internal_id, **kwargs)[-1],
                               parameter) for ds in vols])


class TestInitialPlacement(TestBase):
    SIZE = "1M"
    FILES_PER_OBJECTIVE = 32
    FILE_NUMBER = 100
    DEVIATION_RATIO = 0.2
    MINIMUM_LAYOUTSTATS_ROWS = 15
    RANDOM_ERROR = ("the scatter wasn't fully random for the current "
                    "deviation {0}")
    ALL_FILES_IN_VOL_ERR = "not all files reside in DS ID {0}"
    BASIC_MODEL_ERR = ("The test should run on basic model only, there is "
                       "layoutstats in the DB")
    SEEM_LIKE_ERR = "All volumes aren't seemlike"
    TEST_PATH = 'Initial_Placement_Test'
    INSUFFICIENT_ERR = ("There are insufficient resources due to the "
                        "requirement of at least {0}")
    DISTRIBUTION_ERR = "Files should have reside only in volumes {0}"

    @yield_fixture
    def initial_placement_fixture(self, get_mount_points):
        """
        """
        self.client = ctx.clients[0]
        self.mount = get_mount_points[0]
        self.s_name = self.mount.share.name
        self.s_path = self.mount.share.path
        yield
        for cli_param, vol_attr in VolCaps.PARAMS_MAPPER.items():
            try:
                ctx.cluster.cli.share_objective_remove(
                    name=self.mount.share.name,
                    path=join(self.TEST_PATH, cli_param))
            except CliOperationFailed:
                pass
            try:
                ctx.cluster.cli.basic_objective_delete(name=cli_param)
            except CliOperationFailed:
                pass

    # The actions to perform in the generic function.
    ############################################################################

    def all_meet_actions(self, dimension, objective, assign_path,
                         above_objective_list):

        self.define_objective(Obj.MEET, dimension, objective, assign_path)
        results = yield
        self.random_verification(results, self.FILE_NUMBER)
        yield

    def none_meet_actions(self, dimension, objective, assign_path,
                          above_objective_list):

        self.define_objective(Obj.DONT, dimension, objective, assign_path)
        results = yield
        self.random_verification(results, self.FILE_NUMBER)
        yield

    def some_meet_actions(self, dimension, objective, assign_path,
                          above_objective_list):
        self.define_objective(Obj.MEET, dimension, objective, assign_path)
        results = yield
        keys = [vol_id for (cap_value, vol_id) in above_objective_list]
        self.dispersed_between_some_volumes(results, keys, self.FILE_NUMBER)
        yield

    #                     Initial Placement Utilities
    ############################################################################
    def define_objective(self, objective, capability_param, capability_value,
                         assignment_path):
        """
        Defining the objective by a set of parameters
        :param assignment_path: the target path to assign the objective
        :param objective: Obj Enum.
        :param capability_param: pdcli parameter
        :param capability_value: the value of the objective
        :return:
        """
        RATIO = 20
        # pdcli must receive a long number - cant receive float.
        if capability_value > RATIO:
            if objective == Obj.DONT:
                ctx.cluster.cli.basic_objective_create(**{
                    'name': capability_param, capability_param:
                        long(capability_value) * RATIO})

                self._logger.info('Objective {0} was set to {1}'.format(
                    capability_param, str(capability_value * RATIO)))

            else:
                ctx.cluster.cli.basic_objective_create(**{
                    'name': capability_param, capability_param:
                        long(capability_value)})

                self._logger.info('Objective {0} was set to {1}'.format(
                    capability_param, str(capability_value)))

            ctx.cluster.cli.share_objective_add(name=self.s_name,
                                                objective=capability_param,
                                                path=assignment_path)

    def dir_and_files_creator(self, cli_param):
        """
        :param cli_param: the target dimension for the files.
        :return: file list to truncate, path for objective assignment
        """
        path = join(self.mount.path, self.TEST_PATH, cli_param)
        assignment_path = join(self.TEST_PATH, cli_param)
        self.client.mkdirs(path)
        files = [join(path, cli_param + str(file_path))
                 for file_path in xrange(self.FILE_NUMBER)]
        return files, assignment_path

    def scatter(self, file_names):
        """
        :param file_names: the file names to truncate
        :return: dictionary of ds id vs file number which reside in that ds.
        which ds.
        """
        vol_list = [vol.internal_id for vol in ctx.cluster.data_stores.values()]
        files_per_ds_id = dict.fromkeys(vol_list, 0)
        client = ctx.clients[0]
        for file_name in file_names:
            client.truncate(file_name, self.SIZE)
            fd = client.file_declare(file_name, self.mount.share)
            ds_id = fd.instances[0].data_store.internal_id
            files_per_ds_id[ds_id] += 1
        return files_per_ds_id

    def random_verification(self, statistics, file_amount):
        """
        a function which checks if the distribution of the files is random.
        if the function receives a single volume parameter it checks that all
        files
        reside in that volume
        :param statistics: A dict of the files distribution between the volumes
        :param file_amount: how many files were scattered between the volumes
        contains all files.
        :return: if the scatter distribution was random or not
        """
        vol_amount = float(len(statistics))
        permitted_ds_deviation = self.DEVIATION_RATIO / vol_amount
        ideal_deviation = 1 / vol_amount
        for ds_id, ds_files in statistics.items():
            curr_ds_deviation = ds_files / file_amount
            self.dbg_assert(abs(curr_ds_deviation - ideal_deviation) <= \
                            permitted_ds_deviation, self.RANDOM_ERROR.format(
                            self.DEVIATION_RATIO), statistics=statistics)

    def dispersed_between_some_volumes(self, curr_distri, expected_distri,
                                       file_amount):

        results = {k: v for k, v in curr_distri.items() if v is not 0}
        self.dbg_assert(sorted(expected_distri) == sorted(results.keys()),
                        self.DISTRIBUTION_ERR.format(expected_distri),
                        curr_distri=curr_distri)
        if len(expected_distri) == 1:
            self.dbg_assert(results[expected_distri[0]] == self.FILE_NUMBER,
                            self.RANDOM_ERROR.format(self.DEVIATION_RATIO),
                            current_distri=curr_distri,
                            expected_distri=expected_distri)
        else:
            self.random_verification(results, file_amount), self.RANDOM_ERROR

    @staticmethod
    def seemlike_dimension(vols):
        """
        verify if the vols are seem like
        :param vols: the vols to compare
        :return: True if the vols are seem like. False if they doesn't
        """
        count = 0
        params = VolCaps.PARAMS_MAPPER.values()
        sorted_vols_caps = [VolCaps.basic_cap_by_method
                            (vols, param, sorted, reverse=True) for param in
                            params]
        for sorted_dimension in sorted_vols_caps:
            max_cap = sorted_dimension[0]
            min_cap = sorted_dimension[1]
            curr_rsd = get_rsd(max_cap[0], min_cap[0])
            if curr_rsd < 0.1:
                objective = sum([max_cap[0], min_cap[0]]) / 2
                yield (objective, max_cap[1], min_cap[1])
                count += 1
            else:
                yield False
        assert not count, "There aren't seem like dimensions at all"

    ############################################################################
    def generic_function(self, above_objective):
        """
        Generic function which designated to perform a set of tests such as:
        All Meet/Don't meet the Objective
        conditions.
        :param above_objective: how many volumes are above the objective
        """
        ACTIONS_MAPPER = {"test_all_meet": self.all_meet_actions,
                          "test_one_meet": self.some_meet_actions,
                          "test_some_meet": self.some_meet_actions,
                          "test_none_meet": self.none_meet_actions}
        vols = ctx.cluster.data_stores.values()
        for (dimension, v_attribute) in VolCaps.PARAMS_MAPPER.items():
            s_caps = VolCaps.basic_cap_by_method(vols, v_attribute, sorted,
                                                 reverse=True)
            objective = (s_caps[:above_objective][-1][0]
                         if above_objective else s_caps[above_objective][0])
            above_objective_list = s_caps[:above_objective]
            files, assign_path = self.dir_and_files_creator(dimension)
            # Finds out which test called the generic function.
            cal_fun = inspect.getouterframes(inspect.currentframe(), 2)[1][3]
            gen = ACTIONS_MAPPER[cal_fun](dimension, objective, assign_path,
                                          above_objective_list)
            gen.next()
            gen.send(self.scatter(files))

    # Tests
    ############################################################################
    @mark.skipif(len(ctx.data_stores) < 2,
                 reason=INSUFFICIENT_ERR.format("2 DS's"))
    @attr('initial_placement', jira='PD-19367', name='TC_DM_IP_001')
    def test_default_objective(self, initial_placement_fixture):

        """
        All Volumes meet the default objective
        Expected result: Files are being placed randomly across ALL volumes.
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        path = join(self.mount.path, 'initial_placement')
        self.client.mkdirs(path)
        file_names = [join(path, 'basic_objective' + str(file_index))
                      for file_index in range(self.FILE_NUMBER)]
        statistics = self.scatter(file_names)
        self.random_verification(statistics, self.FILE_NUMBER)

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason=INSUFFICIENT_ERR.format("2 DS's"))
    @attr('initial_placement', jira='PD-19368', name='TC_DM_IP_002')
    def test_all_meet(self, initial_placement_fixture):

        """
        All Volumes meet the objective
        Expected result: Files are being placed randomly across ALL volumes.
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        vols = len(ctx.cluster.data_stores.values())
        self.generic_function(above_objective=vols)

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason=INSUFFICIENT_ERR.format("2 DS's"))
    @attr('initial_placement', jira='PD-19369', name='TC_DM_IP_003')
    def test_one_meet(self, initial_placement_fixture):

        """
        One Volume meet the objective
        Expected result: Files are being place only in that volume
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        self.generic_function(above_objective=1)

    @mark.skipif(len(ctx.data_stores) < 3,
                 reason=INSUFFICIENT_ERR.format("3 DS's"))
    @attr('initial_placement', jira='PD-19370', name='TC_DM_IP_004')
    def test_some_meet(self, initial_placement_fixture):

        """
        More than one volume meet the objective, other doesn't
        Expected result: Files are being placed randomly across The Volumes
        which meet the objective.
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        self.generic_function(above_objective=2)

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason=INSUFFICIENT_ERR.format("2 DS's"))
    @attr('initial_placement', jira='PD-20277', name='TC_DM_IP_005')
    def test_none_meet(self, initial_placement_fixture):

        """
        None of the volumes meet the objective in a ratio that beyond 20
        Expected result: Files are being placed randomly across ALL volumes.
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        self.generic_function(above_objective=0)

    @mark.skipif(len(ctx.data_stores) < 2,
                 reason=INSUFFICIENT_ERR.format("2 DS's"))
    @attr('initial_placement', jira='PD-20278', name='TC_DM_IP_006')
    def test_meet_between_10p_range(self, initial_placement_fixture):

        """
        One Volume meet the objective one doesn't, volumes are seem like
        Expected result: Files are being placed randomly across these volumes.
        """
        assert len(ctx.cluster.influx_db.aggregate_performance()) == 0, \
            "Test should run on the basic model"
        vols = ctx.cluster.data_stores.values()
        dimension_gen = self.seemlike_dimension(vols)
        for cli_param, vol_attr in VolCaps.PARAMS_MAPPER.items():
            seem_like = dimension_gen.next()
            if not seem_like:
                continue
            objective, min_vol_id, max_vol_id = (seem_like[0], seem_like[1],
                                                 seem_like[2])
            files, assignment_path = self.dir_and_files_creator(cli_param)
            self.define_objective(Obj.MEET, cli_param, objective,
                                  assignment_path)
            results = self.scatter(files)
            self.dispersed_between_some_volumes(results, [min_vol_id,
                                                          max_vol_id],
                                                self.FILE_NUMBER)
        dimension_gen.next()
