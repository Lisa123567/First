__author__ = 'sudhakar'

from os.path import join

from _pytest.python import yield_fixture

from tonat.context import ctx
from tonat.test_base import TestBase, attr
from time import sleep
import uuid
from tonat.rand_ex import RandStr


class TestMobilityCapacity(TestBase):
    
    @yield_fixture
    def create_dses(self):
        dses = ctx.cluster.data_stores.values()

        dses_in_use = [ds.logical_volume.ip_addresses[0].address for ds in ctx.cluster.data_stores.values()]
        self._logger.info(dses_in_use)
        for ds in ctx.data_stores:
            if ds.address in dses_in_use:
                continue
            node = ctx.cluster.cli._node_add(ds, async=True)
            export = ds.get_free_export()
            dses.append(ctx.cluster.cli.ds_add(node_name=node.name,
                                               logical_volume_name=export.name,
                                               async=True))
        yield dses
        #teardown

    @attr(jira='PD-11250', name='test_cap_trig_mob')
    def test_cap_trig_mob(self, create_dses, get_nfs4_2_mount_points):
        reset_value1=11
        reset_value2=22
        dses = create_dses

        self._logger.info('RESET the ds_capacity_metrics for %d and %d' % (dses[0].internal_id, dses[1].internal_id))
        ctx.cluster.kafka.send_ds_capacity_msg(dses[0], total=0, used=reset_value1, free=0)
        ctx.cluster.kafka.send_ds_capacity_msg(dses[1], total=0, used=reset_value2, free=0)
        sleep(2)

        mounts = get_nfs4_2_mount_points

        rand_name = str(RandStr())+'.txt'
        filename = join(mounts[0].path, rand_name)
        fd = ctx.clients[0].open_file(filename, 1052738, 0644)
        ctx.clients[0].write_file(fd, 0, 10, 1)
        ctx.clients[0].close_file(fd)

        inode1 = ctx.clients[0].file_inode(filename)
        fi1 = ctx.cluster.get_file_info(mounts[0].share, inode1)
        ds_name1 = fi1.instances[0].data_store.name
        ds_id1 = fi1.instances[0].data_store.internal_id
        obj_id1 = fi1.obj_id
        inst_id1 = fi1.instances[0].instance_id
        self._logger.info("====================================")
        self._logger.info("Before issuing the capacity trigger")
        self._logger.info("====================================")
        self._logger.info('Inode Number : %d' % inode1)
        self._logger.info('DS Name      : %s' % ds_name1)
        self._logger.info('DS ID        : %d' % ds_id1)
        self._logger.info('OBJ ID       : %d' % obj_id1)
        self._logger.info('Instance ID  : %d' % inst_id1)

        total_cap = fi1.instances[0].data_store.logical_volume.capacity.total
        ctx.cluster.kafka.send_ds_capacity_msg(fi1.instances[0].data_store, total=0, used=(total_cap-10), free=0)

        # sleep sometime for grading and mobility to happen
        sleep_time = 180
        self._logger.info('Sleeping %s secs for grading and mobility to happen' % sleep_time)
        sleep(sleep_time)

        fi2 = ctx.cluster.get_file_info(mounts[0].share, inode1)
        ds_name2 = fi2.instances[0].data_store.name
        ds_id2 = fi2.instances[0].data_store.internal_id
        obj_id2 = fi2.obj_id
        inst_id2 = fi2.instances[0].instance_id
        self._logger.info("====================================")
        self._logger.info("After issuing the capacity trigger")
        self._logger.info("====================================")
        self._logger.info('Inode Number : %d' % inode1)
        self._logger.info('DS Name      : %s' % ds_name2)
        self._logger.info('DS ID        : %d' % ds_id2)
        self._logger.info('OBJ ID       : %d' % obj_id2)
        self._logger.info('Instance ID  : %d' % inst_id2)

        # Verify Capacity based Mobility
        if ds_id1 != ds_id2:
            self._logger.info("=========================================================")
            self._logger.info('Capacity Triggered Mobility is SUCCESSFUL')
            self._logger.info('Initially file was placed on DS ID           : %d' % ds_id1)
            self._logger.info('After capacity trigger file moved to DS ID   : %d' % ds_id2)
            self._logger.info("=========================================================")
        else:
            self._logger.info("=========================================================")
            self._logger.info('Capacity Triggered Mobility is UNSUCCESSFUL')
            self._logger.info('Initially file was placed on DS ID                   : %d' % ds_id1)
            self._logger.info('After capacity trigger file STILL remained on DS ID  : %d' % ds_id2)
            self._logger.info("=========================================================")

        self._logger.info('RESET the ds_capacity_metrics for %d and %d' % (dses[0].internal_id, dses[1].internal_id))
        ctx.cluster.kafka.send_ds_capacity_msg(dses[0], total=0, used=reset_value1, free=0)
        ctx.cluster.kafka.send_ds_capacity_msg(dses[1], total=0, used=reset_value2, free=0)

        ctx.clients[0].close_agent()

        sleep(2)
