__author__ = 'mleopold'

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from tonat.libtools.dft_tool import DftWorkload, WorkloadTypes

from pd_tools.dft_caps import DftCaps

#Additional to the test time (test time + TIMEOUT)
TIMEOUT = 300
THREADS_PER_CLIENT=4

class TestStressSnap(TestBase):
    
    num_of_clients = len(ctx.clients)

    @attr(tool='dft', complexity=1, name='snap_stress_basic')
    def test_snap_stress_basic(self, get_fixed_mount_points): #Example_by_Sasi
        run_time = 60 # 1 minute run
        dft_workload = DftWorkload()
        dft_workload.create = 20
        dft_workload.read = 20
        dft_workload.write = 20
        dft_workload.delete = 10
        dft_workload.lock = 0
        dft_workload.snap_create = 3
        dft_workload.snap_delete = 3
        dft_workload.snap_create_then_delete = 3 
        dft_workload.snap_delete_then_create = 3
        dft_workload.snap_create_from_delete = 3
        dft_workload.snap_multiple_delete = 3
        dft_workload.snap_multiple_create = 3
        dft_workload.files_dosage = 90
        dft_workload.folders_dosage = 10
        dft_workload.time = run_time
        dft_workload.workload_type = WorkloadTypes.RANDOM
        dft = DftCaps(ctx.clients).basic(dft_workload, self.num_of_clients * THREADS_PER_CLIENT,  run_time + TIMEOUT)
        ctx.clients[0].execute_tool(dft, get_fixed_mount_points[0])
