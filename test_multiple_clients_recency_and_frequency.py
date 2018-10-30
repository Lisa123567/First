from tonat.constants import fileflags
from tonat.errors import CliOperationFailed
from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
from pd_tools.fio_caps import FioCaps
from tonat.nfsctl import NfsVersion
import random
import time
import os
import uuid
import fnmatch
from pd_common.helper import Helper as hlp
from pytest import yield_fixture
from client.models.ObjectType import ObjectType

SLEEP = 45
MNTPATH = '/mnt/pdfs'
TIMELAG = 10


class TestObjective(TestBase):
    """
    An objectives related test suite.
    """
    @yield_fixture
    def setup_env(self, get_mount_points):
        """
        Acts a general fixture for all tests.
        """
        self.mounts = get_mount_points
        self.mount = self.mounts[0]
        self.client = ctx.clients[0]

        # list of mounted path with unique uuid
        uid = str(uuid.uuid4())
        self.fname = ['{}/{}'.format(path, uid) for path in self.mounts.path]
        self.create_resources()
        yield

    def clear_root_objectives(self):
        """
        Clears any objective from the root of the share (if set).
        """
        f = self.client.file_declare(self.mount.path, self.mount.share)
        if f.explicit_profile_id:
            ctx.cluster.cli.share_objective_remove(name=self.mount.share.name, path='/')

    def create_resources(self):
        """
        1. Creates the required set of basic objective, conditions and smart objectives.
        2. Creates a basic tree structure with files, folders and others.
        """
        self.ptrns = ['a*', '*s', '***', '*li*', 'f*s', 'l*k*', '*l*k', 'link', '/', '/fixed',
                      '/a*', '/*a', 'a*/', '*a/', '*/*', 'a*/a*', 'open/*', 'open/a*', '**/**']
        self.conditions = ['condition_001', 'condition_002', 'condition_003']
        self.basics = ['basic_obj_001', 'basic_obj_002', 'basic_obj_003']
        self.smarts = ['smart_obj_001', 'smart_obj_002', 'smart_obj_003']

        for smart in ctx.cluster.cli.smart_objective_list():
            try:
                ctx.cluster.cli.smart_objective_delete(name=smart['Name'])
            except CliOperationFailed:
                pass

        for cond in ctx.cluster.cli.condition_list():
            try:
                ctx.cluster.cli.condition_delete(name=cond['Name'])
            except CliOperationFailed:
                pass
        for basic in ctx.cluster.cli.basic_objective_list():
            try:
                ctx.cluster.cli.basic_objective_delete(name=basic['Name'])
            except CliOperationFailed:
                pass

        for c in self.conditions:
            if not dict(ctx.cluster.conditions).has_key(c):
                ctx.cluster.cli.condition_create(name=c, pattern='*')
        for b in self.basics:
            if not dict(ctx.cluster.basic_objectives).has_key(b):
                ctx.cluster.cli.basic_objective_create(name=b, min_write_iops=0)
        for s in self.smarts:
            if not dict(ctx.cluster.smart_objectives).has_key(s):
                ctx.cluster.cli.smart_objective_create(name=s, rule=['default-condition:default-objective'])

    def required_slo(self, frequency, rules):
            """
            Calculates the required SLO for a given frequency from a set of rules.
            :param frequency: a given frequency
            :param rules: a list of rules
            :return: the required SLO
            """
            default_obj = ctx.cluster.basic_objectives['default-objective'].internal_id
            required = default_obj
            rules.sort(key=lambda tup: tup[1])
            for i in xrange(len(rules)):
                if frequency < rules[i][2]:
                    if i != 0:
                        required = ctx.cluster.basic_objectives[rules[i - 1][1]].internal_id
                    break
                else:
                    if i == len(rules) - 1:
                        required = ctx.cluster.basic_objectives[rules[i][1]].internal_id
            return required

    def required_slo_by_inactivity(self, passed, rules, lag=TIMELAG):
            """
            Calculates the required SLO for a given passed time from a set of rules.
            :param passed: a given passed time in seconds
            :param rules: a list of rules
            :return: the required SLO
            """
            default_obj = ctx.cluster.basic_objectives['default-objective'].internal_id
            required = default_obj
            for i in xrange(len(rules) - 1, 0, -1):
                if passed < rules[i][3] - lag:
                     required = ctx.cluster.basic_objectives[rules[i][1]].internal_id
            return required


    def create_fio_conf_file(self, filename, iops=10, period=60):
        """
        Generates a FIO configuration file.
        :param filename: List of paths to the file which will endure IOs
        :param iops: IO per seconds rate
        :param period: A time limit for the FIO tool's execution
        :return: A path to the FIO configuration file
        """
        f = "/tmp/fio.conf"
        contents = ["\[global\]", "time_based", "runtime={}".format(period),
                    "rw=randrw", "direct=1", "ioengine=libaio", "bs=4K",
                    "rwmixread=50", "iodepth=64", "\[job1\]", "name=job1",
                    "size=10M", "rate_iops={}".format(iops)]

        for index, client in enumerate(ctx.clients):
            if client.exists(f) and client.isfile(f):
                client.remove(f)
            contents.append('filename={}'.format(filename[index]))
            for line in contents:
                client.execute(["echo", line, ">>", f])
            contents.pop()
        return f

    def fio_process(self, frequency, duration=420):
        """
        :param frequency: expected avg frequency
        :param duration: duration of execution
        :return: list of threads
        """
        f = self.create_fio_conf_file(self.fname,
                                      frequency / (len(ctx.clients) * 2),
                                      duration)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        return [ctx.clients[index].execute_tool(fio, mount, block=False)
                for index, mount in enumerate(self.mounts)]

    @attr('objectives', jira='PD-22315', complexity=1, name='test_smart_objective_frequency_basic')
    def test_smart_objective_frequency_basic(self, setup_env):

        timeout = 425
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]

        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*',
                                         activity=rules[0][2],
                                         inactivity_clear=True)

        ctx.cluster.cli.smart_objective_update(name=s,
                                               rule=['{}:{}'.format
                                                     (rules[0][0], rules[0][1])])

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, path="/"
                                            ,objective=s)
        try:
            frequency = 115
            threads = self.fio_process(frequency)
            time.sleep(15)
            flags = fileflags.O_CREAT | fileflags.O_RDWR | fileflags.O_SYNC
            fd = [ctx.clients[index].open_file(file_path, flags, 0644)
                  for index, file_path in enumerate(self.fname)]

            info = self.client.file_declare(self.fname[0], self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()

            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(self.fname[0], self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()

            [thread.get() for thread in threads]
            assert required == slo, 'expected slo {0} found {1} for {2}'.\
                format(str(required), str(slo), self.fname)
            required = ctx.cluster.basic_objectives['default-objective'].internal_id
            time.sleep(timeout)
            info = self.client.file_declare(self.fname[0], self.mount.share)
            slo = info.slo_ids[0]
            assert required == slo, 'expected slo {0} found {1} for {2}'.\
                format(str(required), str(slo), self.fname)
        finally:
            [client.close_file(fd[index]) and client.close_agent()
             for index, client in enumerate(ctx.clients)]


    @attr('objectives', jira='PD-22316', complexity=1, name='test_smart_objective_recency_basic')
    def test_smart_objective_recency_basic(self, setup_env):

        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180)]

        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*',
                                         inactivity=rules[0][2],
                                         activity_clear=True)

        ctx.cluster.cli.smart_objective_update(name=s,
                                               rule=["{}:{}".format
                                                     (rules[0][0], rules[0][1])])

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)

        for index, value in enumerate(self.fname):
           ctx.clients[index].execute(['dd', 'if=/dev/urandom',
                                       'of={}'.format(value), 'bs=1M',
                                       'oflag=direct', 'count=100'])

        info = self.client.file_declare(self.fname[0], self.mount.share)
        slo = info.slo_ids[0]
        inode = info.inode

        if self.mount.version == NfsVersion.nfs3_dp:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=240)
        else:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=120)

        t1 = t2 = time.time()
        while t2 - t1 < 240:
            info = ctx.cluster.get_file_info(self.mount.share, inode)
            if slo != info.slo_ids[0]:
                slo = info.slo_ids[0]
                expected = self.required_slo_by_inactivity(t2 - t1, rules)
                assert slo == expected, "slo does not match the recency defined"
            time.sleep(10)
            t2 = time.time()
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), \
            "slo does not match the recency defined"



    @attr('objectives', jira='PD-22317', complexity=1, name='test_smart_objective_frequency_multi')
    def test_smart_objective_frequency_multi(self, setup_env):
        #timeout = 425
        timeout = 500
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 30),
                 (self.conditions[1], self.basics[1], 60),
                 (self.conditions[2], self.basics[2], 90)]

        ctx.cluster.cli.condition_update(name=rules[0][0],
                                         pattern='*',
                                         activity=rules[0][2],
                                         inactivity_clear=True)

        ctx.cluster.cli.condition_update(name=rules[1][0],
                                         pattern='*',
                                         activity=rules[1][2],
                                         inactivity_clear=True)

        ctx.cluster.cli.condition_update(name=rules[2][0],
                                         pattern='*',
                                         activity=rules[2][2],
                                         inactivity_clear=True)

        ctx.cluster.cli.smart_objective_update(name=s,
                                               rule=['{}:{}'.
                                               format(rules[0][0], rules[0][1]),
                                               '{}:{}'.format(rules[1][0], rules[1][1]),
                                               '{}:{}'.format(rules[2][0], rules[2][1])])

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        opened = False
        fd = None
        for _ in xrange(3):
            frequency = random.choice([15, 45, 75, 105])
            threads = self.fio_process(frequency, duration=500)
            flags = fileflags.O_RDWR | fileflags.O_SYNC
            time.sleep(15)
            if not opened:
                fd = [ctx.clients[index].open_file(file_path, flags, 0644)
                      for index, file_path in enumerate(self.fname)]
                opened = True
            info = self.client.file_declare(self.fname[0], self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()
            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(self.fname[0], self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()
            [thread.get() for thread in threads]
            assert required == slo, 'expected slo {} found {} for {}, avg_iops {}'.\
                format(str(required), str(slo), self.fname[0], info.avg_iops)

        [client.write_file(fd[index], 0, 1, 2)
         for index, client in enumerate(ctx.clients)]
        required = ctx.cluster.basic_objectives['default-objective'].internal_id
        t1 = t2 = time.time()

        try:
            while t2 - t1 < timeout:
                info = ctx.clients[0].file_declare(self.fname[0], self.mount.share)
                slo = info.slo_ids[0]
                if slo == required:
                    break
                time.sleep(30)
                [client.write_file(fd[index], 0, 1, 2)
                 for index, client in enumerate(ctx.clients)]
                t2 = time.time()
            assert required == slo, 'expected slo {} found {} for {}, avg_iops {}'.\
                format(str(required), str(slo), self.fname[0], info.avg_iops)

        finally:
            [client.close_file(fd[index]) and client.close_agent()
             for index, client in enumerate(ctx.clients)]