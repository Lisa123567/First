#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import signal
import time

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from tonat.product.nodes.cluster import ClusterState
from tonat.hw_ctl.hw_interface import PowerState
from tonat.errors import SshTimeout

from pd_tools.fsx_caps import FsxCaps
from pd_tools.git_caps import GitCaps


PACEMAKER = 'pacemakerd'
COROSYNC = 'corosync'
SYSTEM_MANAGER = 'pd-sys-mgr'
SYSTEM_AGENT = 'pd-sys-agent'
DATA_MANAGER = 'pd-data-mgr'
DATA_MOVER = 'pd-data-mover'


class Timeout:
    def __init__(self, seconds=30, error_message='Timeout occurred.'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        raise AssertionError(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        signal.alarm(0)


class TestHA(TestBase):

    def failover_should_happen(self):
        #TODO: Scan logs for failover verification
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        yield curr_active
        assert ctx.cluster.get_active_dd(attempt=15, interval=10) == curr_passive, 'Failover didnt occur'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_active, 'Something wrong'
        yield

    def failover_should_not_happen(self):
        #TODO: Scan logs for failover verification
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        yield curr_passive
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        yield

    @attr(jira='PD-3796', complexity=1, name='dd_reboot_active')
    def test_001_dd_reboot_active(self):
        failover_check = self.failover_should_happen()
        with self.step('Rebooting active DD'):
            failover_check.next().reboot()
        failover_check.next()

    @attr(jira='PD-3797', complexity=1, name='dd_force_reboot_active')
    def test_002_dd_force_reboot_active(self):
        failover_check = self.failover_should_happen()
        with self.step('Force rebooting active DD'):
            failover_check.next().force_reboot()
        failover_check.next()

    @attr(jira='PD-3798', complexity=1, name='dd_reboot_passive')
    def test_003_dd_reboot_passive(self):
        failover_check = self.failover_should_not_happen()
        with self.step('Rebooting passive DD'):
            failover_check.next().reboot()
        failover_check.next()

    @attr(jira='PD-3799', complexity=1, name='dd_force_reboot_passive')
    def test_004_dd_force_reboot_passive(self):
        failover_check = self.failover_should_not_happen()
        with self.step('Force rebooting passive DD'):
            failover_check.next().reboot()
        failover_check.next()

    @attr(jira='PD-3800', complexity=1, name='dd_reboot_both')
    def test_005_dd_reboot_both(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Rebooting both nodes'):
            curr_active.reboot()
            curr_passive.reboot()
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=30, interval=10)

    @attr(jira='PD-3801', complexity=1, name='dd_force_reboot_both')
    def test_006_dd_force_reboot_both(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Force rebooting both nodes'):
            curr_active.force_reboot()
            curr_passive.force_reboot()
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=30, interval=10)

    @attr(jira='PD-3802', complexity=1, name='dd_hw_reset_active')
    def test_007_dd_hw_reset_active(self):
        failover_check = self.failover_should_happen()
        with self.step('Resetting active DD'):
            failover_check.next().hw.reset()
        failover_check.next()

    @attr(jira='PD-3803', complexity=1, name='dd_hw_reset_passive')
    def test_008_dd_hw_reset_passive(self):
        failover_check = self.failover_should_not_happen()
        with self.step('Resetting passive DD'):
            failover_check.next().hw.reset()
        failover_check.next()

    @attr(jira='PD-3804', complexity=1, name='dd_hw_reset_both')
    def test_009_dd_hw_reset_both(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Resetting both nodes'):
            curr_active.hw.reset()
            curr_passive.hw.reset()
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=30, interval=10)

    @attr(jira='PD-3805', complexity=1, name='dd_hw_poweroff_active')
    def test_010_dd_hw_poweroff_active(self):
        failover_check = self.failover_should_happen()
        with self.step('Powering off active DD'):
            failover_check.next().hw.power_off()
        failover_check.next()

    @attr(jira='PD-3806', complexity=1, name='dd_hw_poweroff_passive')
    def test_012_dd_hw_poweroff_passive(self):
        failover_check = self.failover_should_not_happen()
        with self.step('Powering off passive DD'):
            failover_check.next().hw.power_off()
        failover_check.next()

    @attr(jira='PD-3807', complexity=1, name='dd_hw_poweroff_soft_active')
    def test_013_dd_hw_poweroff_soft_active(self):
        if ctx.cluster.get_active_dd().is_virtual():
            return
        failover_check = self.failover_should_happen()
        failover_check.next()
        with self.step('Powering off (soft) active DD'):
            ctx.cluster.get_active_dd().power_off_soft()
        failover_check.next()

    @attr(jira='PD-3808', complexity=1, name='dd_hw_poweroff_soft_passive')
    def test_014_dd_hw_poweroff_soft_passive(self):
        if ctx.cluster.get_active_dd().is_virtual():
            return
        failover_check = self.failover_should_not_happen()
        failover_check.next()
        with self.step('Powering off (soft) passive DD'):
            ctx.cluster.get_passive_dd().hw.power_off_soft()
        failover_check.next()
        with self.step('Powering on powered off DD'):
            ctx.cluster.get_passive_dd().hw.power_on()

    @attr(jira='PD-3857', complexity=1, name='dd_hb_disconnect_active')
    def test_015_dd_hb_disconnect_active(self):
        failover_check = self.failover_should_happen()
        failover_check.next()
        with self.step('Disconnecting HB interface on active DD'):
            if ctx.cluster.get_active_dd().is_virtual():
                try:
                    ctx.cluster.get_active_dd().execute(['ifdown', 'ens32'], timeout=1)
                except SshTimeout:
                    pass
            else:
                try:
                    ctx.cluster.get_active_dd().execute(['ifdown', 'p19p2'], timeout=1)
                except SshTimeout:
                    pass
        failover_check.next()

    @attr(jira='PD-3858', complexity=1, name='dd_hb_disconnect_passive')
    def test_016_dd_hb_disconnect_passive(self):
        failover_check = self.failover_should_not_happen()
        failover_check.next()
        with self.step('Disconnecting HB interface on active DD'):
            if ctx.cluster.get_active_dd().is_virtual():
                try:
                    ctx.cluster.get_passive_dd().execute(['ifdown', 'ens32'], timeout=1)
                except SshTimeout:
                    pass
            else:
                try:
                    ctx.cluster.get_passive_dd().execute(['ifdown', 'p19p2'], timeout=1)
                except SshTimeout:
                    pass
        failover_check.next()

    @attr(jira='PD-3859', complexity=1, name='dd_kill_processes_active')
    def test_017_dd_kill_processes_active(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Killing pacemaker on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(PACEMAKER))
        curr_active.get_pid_by_proc_name(PACEMAKER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing corosync on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(COROSYNC))
        curr_active.get_pid_by_proc_name(COROSYNC, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing system-manager on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(SYSTEM_MANAGER))
        curr_active.get_pid_by_proc_name(SYSTEM_MANAGER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing data-manager on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(DATA_MANAGER))
        curr_active.get_pid_by_proc_name(DATA_MANAGER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing data-mover on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(DATA_MOVER))
        curr_active.get_pid_by_proc_name(DATA_MOVER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing system-agent on active DD'):
            curr_active.kill(curr_active.get_pid_by_proc_name(SYSTEM_AGENT))
        curr_active.get_pid_by_proc_name(SYSTEM_AGENT, )
        assert ctx.cluster.get_active_dd(attempt=6, interval=10) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'

    @attr(jira='PD-3860', complexity=1, name='dd_kill_processes_passive')
    def test_018_dd_kill_processes_passive(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Killing pacemaker on passive DD'):
            curr_passive.kill(curr_passive.get_pid_by_proc_name(PACEMAKER))
        curr_passive.get_pid_by_proc_name(PACEMAKER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing corosync on passive DD'):
            curr_passive.kill(curr_passive.get_pid_by_proc_name(COROSYNC))
        curr_passive.get_pid_by_proc_name(COROSYNC, attempts=3, interval=5)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing data-mover on passive DD'):
            curr_passive.kill(curr_passive.get_pid_by_proc_name(DATA_MOVER))
        curr_passive.get_pid_by_proc_name(DATA_MOVER, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'
        with self.step('Killing system-agent on passive DD'):
            curr_passive.kill(curr_passive.get_pid_by_proc_name(SYSTEM_AGENT))
        curr_passive.get_pid_by_proc_name(SYSTEM_AGENT, attempts=6, interval=10)
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'

    @attr(jira='PD-3861', complexity=1, name='dd_shutdown_active')
    def test_019_dd_shutdown_active(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Powering off active DD'):
            curr_active.shutdown()
        # Failover - passive should become active
        assert ctx.cluster.get_active_dd(attempt=15, interval=10) == curr_passive, 'Failover didnt occur'
        with self.step('Powering on powered off DD'):
            state = curr_active.hw.get_power_state()
        with Timeout(seconds=60, error_message='Took to long to power off the active DD.'):
            while state != PowerState.OFF:
                time.sleep(2)
                state = curr_active.hw.get_power_state()
        curr_active.hw.power_on()
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_active, 'Something wrong'

    @attr(jira='PD-3862', complexity=1, name='dd_shutdown_passive')
    def test_020_dd_shutdown_passive(self):
        curr_active = ctx.cluster.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Powering off passive DD'):
            curr_passive.shutdown()
        assert ctx.cluster.get_active_dd(attempt=3, interval=5) == curr_active, 'Failover occurred but it should not'
        with self.step('Powering on powered off DD'):
            state = curr_passive.hw.get_power_state()
        with Timeout(seconds=60, error_message='Took to long to power off the passive DD.'):
            while state != PowerState.OFF:
                time.sleep(2)
                state = curr_passive.hw.get_power_state()
        curr_passive.hw.power_on()
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_passive, 'Something wrong'

    @attr(jira='PD-3863', complexity=1, name='dd_multiple_soft_reboot')
    def test_021_dd_multiple_soft_reboot(self):
        curr_active = ctx.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Force rebooting active DD'):
            curr_active.reboot()
        assert ctx.cluster.get_active_dd(attempt=15, interval=10) == curr_passive, 'Failover didnt occur'
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_active, 'Something wrong'
        curr_active.reboot()
        assert ctx.cluster.get_passive_dd(expected_state=ClusterState.HEALTHY, attempt=30,
                                          interval=10) == curr_active, 'Something wrong'

    @attr(jira='PD-4325', complexity=1, name='dd_system_shutdown_no_poweroff')
    def test_022_dd_system_shutdown_nopoweroff(self):
        with self.step('Invoking system shutdown via CLI'):
            ctx.cluster.cli.system_shutdown(force=True, no_poweroff=True, timeout=300)
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.FAILED, attempt=30, interval=10)

    @attr(jira='PD-4326', complexity=1, name='dd_system_shutdown')
    def test_023_dd_system_shutdown(self):
        curr_active = ctx.get_active_dd()
        curr_passive = ctx.cluster.get_passive_dd()
        with self.step('Invoking system shutdown via CLI'):
            ctx.cluster.cli.system_shutdown(force=True, no_poweroff=False, timeout=300)
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.FAILED, attempt=60, interval=5)
        state = curr_active.hw.get_power_state()
        while state != PowerState.OFF:
            time.sleep(2)
            state = curr_active.hw.get_power_state()
        curr_active.hw.power_on()
        state = curr_passive.hw.get_power_state()
        while state != PowerState.OFF:
            time.sleep(2)
            state = curr_passive.hw.get_power_state()
        curr_passive.hw.power_on()
        ctx.cluster.wait_for_cluster_state(expected_state=ClusterState.HEALTHY, attempt=30, interval=10)

    @attr(jira='PD-3041', complexity=2, tool='fsx', abuse='reboot', name='fsx_hard_reboot_active_dd_basic')
    def test_fsx_hard_reboot_active_dd_basic(self, get_mount_points):
        '''
        fsx run basic with reboot of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_002_dd_force_reboot_active()
        # get usually return the value or raise an exception
        # if an exception occurred during the greenlet execution
        # but since this get is called on tuple_dispatcher
        # it will return a tuple of values or a
        # compound exception in case of error
        fsx_tools = workers.get()
        # FIXME: bellow will probably fail as they are not suppose to be equal
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3869', complexity=2, tool='fsx', abuse='reboot', name='fsx_soft_reboot_dd_basic')
    def test_fsx_soft_reboot_active_dd_basic(self, get_mount_points):
        '''
        fsx run basic with reboot of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_001_dd_reboot_active()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='', complexity=2, tool='fsx', abuse='reboot', name='fsx_soft_reboot_passive_dd_basic')
    def test_fsx_soft_reboot_passive_dd_basic(self, get_mount_points):
        '''
        fsx run basic with reboot of passive dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        failover_check = self.failover_should_not_happen()
        failover_check.next()
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_003_dd_reboot_passive()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)
        failover_check.next()

    @attr(jira='PD-3870', complexity=2, tool='fsx', abuse='reboot', name='fsx_hard_shutdown_passive_dd_basic')
    def test_fsx_hard_shutdown_passive_dd_basic(self, get_mount_points):
        '''
        fsx run basic with reboot of passive dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_012_dd_hw_poweroff_passive()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3871', complexity=2, tool='fsx', abuse='reboot', name='fsx_hard_shutdown_active_dd_basic')
    def test_fsx_hard_shutdown_active_dd_basic(self, get_mount_points):
        '''
        fsx run basic with shutdown of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_010_dd_hw_poweroff_active()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3872', complexity=2, tool='fsx', abuse='reboot', name='fsx_soft_hw_shutdown_active_dd_basic')
    def test_fsx_soft_hw_shutdown_active_dd_basic(self, get_mount_points):
        '''
        fsx run basic with shutdown of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        curr_active = ctx.get_active_dd()
        if curr_active.is_virtual():
            return
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_013_dd_hw_poweroff_soft_active()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3873', complexity=2, tool='fsx', abuse='reboot', name='fsx_soft_shutdown_active_dd_basic')
    def test_fsx_soft_shutdown_active_dd_basic(self, get_mount_points):
        '''
        fsx run basic with shutdown of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''

        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_019_dd_shutdown_active()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3874', complexity=2, tool='fsx', abuse='reboot', name='fsx_multiple_soft_reboot_dd_basic')
    def test_fsx_multiple_soft_reboot_dd_basic(self, get_mount_points):
        '''
        fsx run basic with reboot of active dd in the middle of
        the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_021_dd_multiple_soft_reboot()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr(jira='PD-3044', complexity=2, tool='fsx', abuse='reboot', name='fsx_hard_reboot_dd_directio')
    def test_fsx_hard_reboot_dd_directio(self, get_mount_points):
        '''
        fsx run big_direct_io with reboot dd in the
        middle of the test and compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().big_direct_io(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        self.test_002_dd_force_reboot_active()
        fsx_tools = workers.get()
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good)

    @attr('sanity', jira='PD-3043', complexity=2, tool='fsx', abuse='reboot', name='fsx_reboot_dd_after_basic')
    def test_fsx_reboot_dd_after_basic(self, get_mount_points):
        '''
        fsx run basic with reboot dd after the test and
        compare fsx file with .fsxgood file.
        '''
        fsx = FsxCaps().basic(teardown=False)
        workers = ctx.clients.execute_tool(fsx, get_mount_points, block=False)
        fsx_tools = workers.get()
        self.test_002_dd_force_reboot_active()
        # timeout increased below to avoid catching 90s NFS server grace time
        ctx.clients.compare_files(fsx_tools.data, fsx_tools.data_good, timeout=120)

    @attr(jira='PD-4179', complexity=1, tool='git', abuse='reboot', name='git_test_force_reboot')
    def test_git_force_reboot(self, get_mount_points):
        git = GitCaps().test(1)
        workers = ctx.clients.execute_tool(git, get_mount_points, block=False)
        time.sleep(120)
        self.test_002_dd_force_reboot_active()
        time.sleep(240)
        self.test_002_dd_force_reboot_active()
        time.sleep(240)
        self.test_002_dd_force_reboot_active()
        git_tools = workers.get()

    @attr(jira='PD-4327', complexity=1, tool='git', abuse='reboot', name='git_test_soft_reboot')
    def test_git_soft_reboot(self, get_mount_points):
        git = GitCaps().test(1)
        workers = ctx.clients.execute_tool(git, get_mount_points, block=False)
        time.sleep(120)
        self.test_001_dd_reboot_active()
        time.sleep(240)
        self.test_001_dd_reboot_active()
        time.sleep(240)
        self.test_001_dd_reboot_active()
        git_tools = workers.get()

    @attr(jira='PD-4328', complexity=1, tool='git', abuse='reboot', name='git_test_kill_services')
    def test_git_kill_services(self, get_mount_points):
        git = GitCaps().test(1)
        workers = ctx.clients.execute_tool(git, get_mount_points, block=False)
        time.sleep(120)
        self.test_017_dd_kill_processes_active()
        time.sleep(240)
        self.test_017_dd_kill_processes_active()
        time.sleep(240)
        self.test_017_dd_kill_processes_active()
        git_tools = workers.get()
