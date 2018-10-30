# __author__ = 'Elad Sofer'
from __future__ import division

from flufl import enum
from os.path import join

from tonat.context import ctx
from tonat.libtests import attr
from tonat.test_base import TestBase
from tonat.dispatcher_tuple import dispatcher_tuple
from tonat.errors import CompoundException, WrongFileLocation

from gevent import sleep
from pytest import yield_fixture, mark


class PlaceOn(enum.Enum):
    VOLUME = 'STORAGE_VOLUME'
    VOLUME_GROUP = 'VOLUME_GROUP'
    NODE = 'NODE'


class TestMobilityPlayBook5(TestBase):
    SIZE = "1K"
    FILE_NUMBER = 100
    PLAYBOOK6_SET_A_FILE_NUMERS = 9000
    PLAYBOOK6_SET_B_FILE_NUMERS = 1000
    DEVIATION_RATIO = 0.2
    RANDOM_ERROR = ("the scatter wasn't fully random for the current "
                    "deviation {0}")
    ALL_FILES_IN_VOL_ERR = "not all files reside in DS ID {0}"
    TEST_PATH = 'PlayBook5'
    INSUFFICIENT_ERR = ("There are insufficient resources due to the "
                        "requirement of at least {0}")
    DISTRIBUTION_ERR = "Files should have reside only in volumes {0}"
    SET_A_SUFFIX = 'SetA'
    SET_B_SUFFIX = 'SetB'
    SET_B_ERROR = ('Set B which not related to the place on objective moved '
                   'also due to the objective update')
    OBJECTIVE_A = 'ObjectiveVolA'
    OBJECTIVE_B = 'ObjectiveVolB'
    OBJECTIVE_VOL_A_RETURN = 'ObjectiveVolAReturn'

    @yield_fixture
    def playbook5_fixture(self, get_mount_points):
        """
        """
        self.client = ctx.clients[0]
        self.mount = get_mount_points[0]
        self.s_name = self.mount.share.name
        self.s_path = self.mount.share.path
        self.assignment_path = []
        self.objective_name = []
        self.smart_obj = []
        self.group_name = None
        self.condition = None
        yield
        # Configuration cleanup.
        if self.smart_obj:
            for smart in self.smart_obj:
                ctx.cluster.cli.smart_objective_delete(smart.name)
        if self.condition:
            ctx.cluster.cli.condition_delete(self.condition.name)
        for name, assign_path in zip(self.objective_name, self.assignment_path):
            ctx.cluster.cli.share_objective_remove(name=self.s_name,
                                                   path=assign_path)
            ctx.cluster.cli.basic_objective_delete(name=name)
        if self.group_name:
            ctx.cluster.cli.volume_group_delete(name=self.group_name)

    # Playbook Utilities
    ############################################################################
    # TODO should decide which utilities to move to tonat.utills
    def define_objective(self, **kwargs):
        """
        Defining the objective by a set of parameters
        :param place_on_expression: place on expression for the objective
        :param objective_name: objective name
        :param assignment_path: the target path to assign the objective
        :return:
        """
        assignment_path = kwargs.pop('assignment_path')
        objective_name = kwargs.get('name')
        self.assignment_path.append(assignment_path)
        self.objective_name.append(objective_name)
        ctx.cluster.cli.basic_objective_create(**kwargs)
        ctx.cluster.cli.share_objective_add(name=self.s_name,
                                            objective=objective_name,
                                            path=assignment_path)

    def dir_and_files_creator(self, base_path, suffix, file_num,
                              extension=None):
        """
        :param extension: string for example .txt, .doc .pdf etc
        :param file_num: how many files to create
        :param base_path:  the base path for the file
        :param suffix: suffix for the file's name
        :return: file list to truncate, path for objective assignment
        """
        path = join(self.mount.path, base_path, suffix)
        assignment_path = join(base_path, suffix)
        self.client.mkdirs(path)
        files = [join(path, suffix + str(file_path) + extension)
                 if extension else join(path, suffix + str(file_path))
                 for file_path in xrange(file_num)]
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
        fds = []
        for file_name in file_names:
            client.truncate(file_name, self.SIZE)
            fd = client.file_declare(file_name, self.mount.share)
            ds_id = fd.instances[0].data_store.internal_id
            files_per_ds_id[ds_id] += 1
            fds.append(fd)
        return files_per_ds_id, fds

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
        '''

        :param curr_distri: list of vol's internal ids
        :param expected_distri: list of volumes
        :param file_amount:
        :return:
        '''
        results = {k: v for k, v in curr_distri.items() if v is not 0}
        self.dbg_assert(sorted(expected_distri) == sorted(results.keys()),
                        self.DISTRIBUTION_ERR.format(expected_distri),
                        curr_distri=curr_distri)
        if len(expected_distri) == 1:
            self.dbg_assert(results[expected_distri[0]] == file_amount,
                            self.RANDOM_ERROR.format(self.DEVIATION_RATIO),
                            current_distri=curr_distri,
                            expected_distri=expected_distri)
        else:
            self.random_verification(results, file_amount), self.RANDOM_ERROR

    def files_location_verification(self, fds, dest):
        fvs = []
        try:
            for fd in fds:
                fvs.append(fd.wait_for_mobility_process(dest, attempt=30,
                                                        interval=30, block=False))
            fvs = dispatcher_tuple(fvs)
            fvs.get()
        except CompoundException:
            self.dbg_assert(False, "set A Files should reside in destination "
                                   "Vol", dest_vol=dest, set_a_files=fds)

    # The actions to perform in the generic function.
    ############################################################################

    def objective_by_volume_group(self, source_vol, destination_vol,
                                  assign_path):
        group_a = ctx.cluster.cli.volume_group_create(source_vol.name,
                                                      expressions='volume:{0}'
                                                      .format(source_vol.name))
        self.group_name = group_a.name
        self.define_objective(name=group_a.name, assignment_path=assign_path,
                              place_on_list='volume-group:{0}'.format(group_a.name))
        results, fds = yield
        self.dispersed_between_some_volumes(results, [source_vol.internal_id],
                                            self.FILE_NUMBER)
        ctx.cluster.cli.volume_group_update(group_a.name,
                                            expressions='volume:{0}'.format(
                                            destination_vol.name))
        yield fds
        ctx.cluster.cli.volume_group_update(group_a.name,
                                            expressions='volume:{0}'.format(
                                            source_vol.name))
        yield
        ctx.cluster.cli.volume_group_update(group_a.name,
                                            expressions='volume:{0}'.format(
                                            destination_vol.name))
        yield

    def objective_by_volume(self, source_vol, destination_vol, assign_path):
        self.define_objective(name=source_vol.name, assignment_path=assign_path,
                              place_on_list='volume:{0}'.format(source_vol.name))
        results, fds = yield
        self.dispersed_between_some_volumes(results, [source_vol.internal_id],
                                            self.FILE_NUMBER)
        ctx.cluster.cli.basic_objective_update(source_vol.name,
                                               place_on_list='volume:{0}'.format
                                               (destination_vol.name))
        yield fds
        ctx.cluster.cli.basic_objective_update(source_vol.name,
                                               place_on_list='volume:{0}'.format
                                               (source_vol.name))
        yield
        ctx.cluster.cli.basic_objective_update(source_vol.name,
                                       place_on_list='volume:{0}'.format
                                       (destination_vol.name))
        yield

    def objective_by_node(self, src_vols, dest_vols, assign_path):
        src_ds = src_vols[0].node.name
        dest_ds = dest_vols[0].node.name
        self.define_objective(name=src_ds, place_on_list='node:{0}'.format(src_ds),
                              assignment_path=assign_path)
        results, fds = yield
        src_vols = [vol.internal_id for vol in src_vols]
        self.dispersed_between_some_volumes(results, src_vols, self.FILE_NUMBER)
        ctx.cluster.cli.basic_objective_update(name=src_ds,
                                               place_on_list='node:{0}'.format(dest_ds))
        yield fds
        ctx.cluster.cli.basic_objective_update(name=src_ds,
                                               place_on_list='node:{0}'.format(src_ds))
        yield
        ctx.cluster.cli.basic_objective_update(name=src_ds,
                                               place_on_list='node:{0}'.format(dest_ds))
        yield


    ############################################################################
    def generic_function(self, src, dest, based_on=None):
        """
        function which designated to trigger mobility by the based_on param
        from source volume to destination volume.
        :param based_on: PlaceOn Enum
        :param dest: Destination Volumes
        :param src: Source Volumes
        """

        FUNC_MAPPER = {PlaceOn.VOLUME: self.objective_by_volume,
                       PlaceOn.VOLUME_GROUP: self.objective_by_volume_group,
                       PlaceOn.NODE: self.objective_by_node}

        set_a_files, set_a_path = self.dir_and_files_creator(self.TEST_PATH,
                                                             self.SET_A_SUFFIX,
                                                             self.FILE_NUMBER)
        # Negative check, checking the files spread across all volumes

        set_b_files, set_b_path = self.dir_and_files_creator(self.TEST_PATH,
                                                             self.SET_B_SUFFIX,
                                                             self.FILE_NUMBER)
        set_b_res, set_b_fds = self.scatter(set_b_files)
        self.random_verification(set_b_res, self.FILE_NUMBER)

        gen = FUNC_MAPPER[based_on](src, dest, set_a_path)
        gen.next()
        # waiting for objective to take place.
        sleep(5)
        set_a_fds = gen.send(self.scatter(set_a_files))
        # waiting for mobility to complete.
        sleep(120)
        # Mobility check
        self.files_location_verification(set_a_fds, dest)
        # Negative check, expect set b won't move due to objective update.
        try:
            for fd_b in set_b_fds:
                fd_b.wait_for_mobility_process(dest, attempt=1)
        except WrongFileLocation:
            # if the Exception is caught there is no error.
            pass

        else:
            self.dbg_assert(False, self.SET_B_ERROR, set_b_path=join(
                self.TEST_PATH, self.SET_B_SUFFIX))
        gen.next()
        sleep(120)
        # Mobility check
        self.files_location_verification(set_a_fds, src)
        gen.next()
        # Mobility check
        sleep(120)
        self.files_location_verification(set_a_fds, dest)

    # Tests
    ############################################################################
    @mark.skipif(len(ctx.data_stores) < 1, reason='At least 1 DS is required')
    @attr('playbook5', jira='PD-20789', name='test_playbook5_vol_a_to_b')
    def test_playbook5_vol_a_to_b(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Volume
        """
        vols = ctx.cluster.data_stores.values()
        vol_a, vol_b = vols[0], vols[1]
        self.generic_function(src=vol_a, dest=vol_b, based_on=PlaceOn.VOLUME)

    @mark.skipif(len(ctx.data_stores) < 1, reason='At least 1 DS is required')
    @attr('playbook5', jira='PD-20790', name='test_playbook5_vol_b_to_a')
    def test_playbook5_vol_b_to_a(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Volume
        """
        vols = ctx.cluster.data_stores.values()
        vol_a, vol_b = vols[1], vols[0]
        self.generic_function(src=vol_a, dest=vol_b, based_on=PlaceOn.VOLUME)

    @mark.skipif(len(ctx.data_stores) < 1, reason='At least 1 DS is required')
    @attr('playbook5', jira='PD-20791', name='test_playbook5_vol_group_a_to_b')
    def test_playbook5_vol_group_a_to_b(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Volume Group
        """
        vols = ctx.cluster.data_stores.values()
        vol_a, vol_b = vols[0], vols[1]
        self.generic_function(src=vol_a, dest=vol_b, based_on=PlaceOn.VOLUME_GROUP)

    @mark.skipif(len(ctx.data_stores) < 1, reason='At least 1 DS is required')
    @attr('playbook5', jira='PD-20792', name='test_playbook5_vol_group_b_to_a')
    def test_playbook5_vol_group_b_to_a(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Volume Group
        """
        vols = ctx.cluster.data_stores.values()
        vol_a, vol_b = vols[0], vols[1]
        self.generic_function(src=vol_a, dest=vol_b, based_on=PlaceOn.VOLUME_GROUP)

    @mark.skipif(len(ctx.data_stores) < 2, reason='At least 2 DS is required')
    @attr('playbook5', jira='PD-20793', name='test_playbook5_node_a_to_b')
    def test_playbook5_node_a_to_b(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Node
        """
        ds_a, ds_b = ctx.data_stores[0].node.name, ctx.data_stores[1].node.name
        vols_a = [vol for vol in ctx.cluster.data_stores.values()
                  if ds_a in vol.name]
        vols_b = [vol for vol in ctx.cluster.data_stores.values()
                  if ds_b in vol.name]
        self.generic_function(src=vols_a, dest=vols_b, based_on=PlaceOn.NODE)

    @mark.skipif(len(ctx.data_stores) < 2, reason='At least 2 DS is required')
    @attr('playbook5', jira='PD-20794', name='test_playbook5_node_b_to_a')
    def test_playbook5_node_b_to_a(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Node
        """
        ds_a, ds_b = ctx.data_stores[1].node.name, ctx.data_stores[0].node.name
        vols_a = [vol for vol in ctx.cluster.data_stores.values() if ds_a in
                  vol.name]
        vols_b = [vol for vol in ctx.cluster.data_stores.values() if ds_b in
                  vol.name]

        self.generic_function(src=vols_a, dest=vols_b, based_on=PlaceOn.NODE)

    @mark.skipif(len(ctx.data_stores) < 1, reason='At least 1 DS is required')
    @attr('playbook6', jira='PD-21020', name='test_playbook6')
    def test_playbook6(self, playbook5_fixture):

        """
        function which creates mobility, based on Place On by Volume
        """
        vols = ctx.cluster.data_stores.values()
        vol_a, vol_b = vols[0], vols[1]
        obj_vol_a_files, vol_a_path = self.dir_and_files_creator(self.TEST_PATH,
                                                                 self.SET_A_SUFFIX,
                                                                 self.PLAYBOOK6_SET_A_FILE_NUMERS)
        obj_vol_b_files, vol_b_path = self.dir_and_files_creator(self.TEST_PATH,
                                                                 self.SET_B_SUFFIX,
                                                                 self.PLAYBOOK6_SET_B_FILE_NUMERS,
                                                                 extension='.txt')
        self.define_objective(name=self.OBJECTIVE_A, assignment_path='/',
                              place_on_list='volume:{0}'.format(vol_a.name))
        set_a_res, set_a_fds = self.scatter(obj_vol_a_files)
        set_b_res, set_b_fds = self.scatter(obj_vol_b_files)
        # Expecting result: every file should reside in VolA .
        # the objective was set to VolA under the global share.
        self.dispersed_between_some_volumes(set_a_res, [vol_a.internal_id],
                                            self.PLAYBOOK6_SET_A_FILE_NUMERS)
        self.dispersed_between_some_volumes(set_b_res, [vol_a.internal_id],
                                            self.PLAYBOOK6_SET_B_FILE_NUMERS)
        # Expect test files (suffix .txt - (SET_B) to move to VolB.
        self.define_objective(name=self.OBJECTIVE_B, assignment_path='/',
                              place_on_list='volume:{0}'.format(vol_b.name))
        self.condition = ctx.cluster.cli.condition_create(name='test_files',
                                                          pattern='*.txt')
        self.smart_obj.append(ctx.cluster.cli.smart_objective_create(
            name='test_files', rule='{0}:{1}'.format(self.condition.name,
                                                     self.OBJECTIVE_B)))

        sleep(120)
        fvs = []
        try:
            for fd_b in set_b_fds:
                fvs.append(fd_b.wait_for_mobility_process(vol_b, attempt=30,
                                                       interval=30,
                                                       block=False))
            fvs = dispatcher_tuple(fvs)
            fvs.get()
        except CompoundException:
            raise CompoundException

        # Negative check, expect set b won't move due to objective update.
        try:
            for fd_a in set_a_fds:
                fd_a.wait_for_mobility_process(vol_a, attempt=1)
        except Exception:
            # if the Exception is caught there is no error.
            self.dbg_assert(False,
                            "set A Files should reside in VolA",
                            files=set_a_fds, vol_a=vol_a)
        self.define_objective(name=self.OBJECTIVE_VOL_A_RETURN,
                              assignment_path='/',
                              place_on_list='volume:{0}'.format(vol_a.name))
        self.smart_obj.append(ctx.cluster.cli.smart_objective_create(
            name='vol_a_return', rule='{0}:{1}'.format(self.condition.name,
                                                      self.OBJECTIVE_VOL_A_RETURN)))
        sleep(120)
        fvs = []
        try:
            for fd_b in set_b_fds:
                fvs.append(fd_b.wait_for_mobility_process(vol_a, attempt=30,
                                                       interval=30, block=False))
            fvs = dispatcher_tuple(fvs)
            fvs.get()
        except CompoundException:
            self.dbg_assert(False,
                            "set b files should reside in vol_a due to "
                            "VolAReturnObjective", vol=vol_a, set_b=set_b_fds)