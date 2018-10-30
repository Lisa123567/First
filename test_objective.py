#__author__ = 'mleopold'

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
from pytest import mark
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
        nodes = [dd.node for dd in ctx.cluster.get_nodes_by_type(ObjectType.DATA_SPHERE)]
        self.dd_node = nodes[0]
        self.clear_root_objectives()
        self.create_resources()       
        yield

    def clear_root_objectives(self):
        """
        Clears any objective from the root of the share (if set).
        """
        f =  self.client.file_declare(self.mount.path, self.mount.share)
        if f.explicit_profile_id:
            ctx.cluster.cli.share_objective_remove(name=self.mount.share.name, path='/')
        
    def create_resources(self):
        """
        1. Creates the required set of basic objective, conditions and smart objectives.
        2. Creates a basic tree structure with files, folders and others.
        """
        self.ptrns = ['a*', '*s', '***', '*li*', 'f*s', 'l*k*', '*l*k', 'link', '/fixed', 
                      '/a*', '/*a', '*/*', 'a*/a*', 'open/*', 'open/a*', '**/**']
        self.conditions = ['condition_001', 'condition_002', 'condition_003']
        self.basics = ['basic_obj_001', 'basic_obj_002', 'basic_obj_003']
        self.smarts = ['smart_obj_001', 'smart_obj_002', 'smart_obj_003']
        for c in self.conditions:
            if not dict(ctx.cluster.conditions).has_key(c):
                ctx.cluster.cli.condition_create(name=c, pattern='*')
        for b in self.basics:
            if not dict(ctx.cluster.basic_objectives).has_key(b):
                ctx.cluster.cli.basic_objective_create(name=b, min_write_iops=0)
        for s in self.smarts:
            if not dict(ctx.cluster.smart_objectives).has_key(s):
                ctx.cluster.cli.smart_objective_create(name=s, rule=['default-condition:default-objective'])
        
    def copy_dataset(self, path):
        """
        Copies a data set into the path specified.
        :param path: the path to which we copy the data set
        """
        self.client.execute(['cp', '-a', '/opt/tools/latest/fstest', path])
    
    def validate_assignment(self, path):
        """
        Tree walks and validates the objectives assignment.
        :param path: the path to run assignment validation on
        """
        if self.client.isdir(path):
            res = self.client.execute(['find', path, '-mindepth', '1', '-maxdepth', '1', '-type', 'f'])
            files = res.stdout.split('\n')[:-1]
            res = self.client.execute(['find', path, '-mindepth', '1', '-maxdepth', '1', '-type', 'd'])
            dirs = res.stdout.split('\n')[:-1]
            parent = self.client.file_declare(path, self.mounts[0].share)
            for file1 in files:
                self._logger.info("FILE: %s" % file1)
                self.validate_objectives(file1, parent)
            for dir1 in dirs:           
                self._logger.info("DIR: %s" % dir1)
                d = self.client.file_declare(dir1, self.mount.share)
                if (not d.explicit_profile_id):
                    assert parent.profile_id == d.profile_id, 'The child directory did not inherit the profile of the parent'
                self.validate_assignment(dir1)
        elif self.client.isfile(path):
            d = self.client.file_declare(os.path.dirname(path), self.mount.share)
            self.validate_objectives(file1, self.client.file_declare(os.path.dirname(path), d))
        else:
            raise AssertionError('The given path is neither a directory or a file')
    
    def validate_objectives(self, path, parent):
        """
        Validates the SLO assigned to a file according to its parent's profile.
        :param path: a file path
        :param parent: the parent directory
        """      
        slos = set() 
        basename = os.path.basename(path)
        dirname = os.path.basename(os.path.dirname(path))
        fullname = dirname + '/' + basename  
        f = self.client.file_declare(path, self.mount.share)
        if f.explicit_profile_id:
            parent = f
        p = [profile for profile in dict(ctx.cluster.smart_objectives).values() if profile.internal_id == parent.profile_id][0] 
        for r in p.rules:
            for c in r.conditions:
                pattern = c.pattern
                if '/' in pattern:
                    if fnmatch.fnmatch(fullname, pattern):
                        slos = set(o.internal_id for o in r.objectives)
                        break
                else:
                    if fnmatch.fnmatch(basename, pattern):
                        slos = set(o.internal_id for o in r.objectives)
                        break
            if not slos:
                break
        if slos:
            assert slos == set(f.slo_ids), 'The assigned objectives are different than the expected ones'
        else:
            assert set([1]) == set(f.slo_ids), 'The assigned objectives are different than the expected ones'
    
    def abs_to_rel(self, path):
        """
        Converts from an absolute path to a share relative path.
        :param path: the path to convert
        :return: the share relative path
        """
        return path[len(self.mount.path):]
    
    def get_dir(self, path):
        """
        Finds a random directory under a  given path.
        :param path: a search path 
        :return: a path of a directory
        """
        res = self.client.execute(['find', path, '-mindepth', '1', '-type', 'd'])
        return random.choice(res.stdout.split('\n')[:-1])

    def get_file(self, path):
        """
        Finds a random file under a  given path.
        :param path: a search path 
        :return: a path of a file
        """
        res = self.client.execute(['find', path, '-mindepth', '1', '-type', 'f'])
        return random.choice(res.stdout.split('\n')[:-1])
    
    @attr('objectives', jira='PD-12706', complexity=1, name='set_objective_existing_objects')
    def test_set_objective_existing_objects(self, setup_env):
        """
        Test - set objectives on existing objects.
        """
        ptrns = list(self.ptrns)
        s = self.smarts[0]
        c = self.conditions[0]
        b = self.basics[0]
        v = 1
        for _ in xrange(2):
            self.copy_dataset(self.mount.path)        
            for _ in xrange(3):
                d = self.get_dir(self.mount.path)
                p = random.choice(ptrns)
                ptrns.remove(p)
                ctx.cluster.cli.condition_update(name=c, pattern=p, inactivity_clear=True)
                ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (c, b)])
                ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                                    path=self.abs_to_rel(d), 
                                                    objective=s)
                time.sleep(SLEEP)
                self._logger.info('Starting validation %d' % v)
                v += 1
                self.validate_assignment(self.mount.path)
            self.client.clean_dir(self.mount.path, exclude='.snapshot', timeout=180)
            self.clear_root_objectives()

    @attr('objectives', jira='PD-12707', complexity=1, name='remove_objective_existing_objects')
    def test_remove_objective_existing_objects(self, setup_env):
        """
        Test - remove objectives from existing objects.
        """
        ptrns = list(self.ptrns)
        s = self.smarts[0]
        c = self.conditions[0]
        b = self.basics[0]
        v = 1
        dirs = set([])
        self.copy_dataset(self.mount.path)        
        for _ in xrange(2):
            for _ in xrange(3):
                d = self.get_dir(self.mount.path)
                dirs.add(d)
                p = random.choice(ptrns)
                ptrns.remove(p)
                ctx.cluster.cli.condition_update(name=c, pattern=p, inactivity_clear=True)
                ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (c, b)])
                ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                                    path=self.abs_to_rel(d), 
                                                    objective=s)
            while dirs:
                d = random.choice(list(dirs))
                dirs.remove(d)
                ctx.cluster.cli.share_objective_remove(name=self.mount.share.name, path=self.abs_to_rel(d))
                time.sleep(SLEEP)
                self._logger.info('Starting validation %d' % v)
                v += 1
                self.validate_assignment(self.mount.path)

    @attr('objectives', jira='next', complexity=1, name='set_objective_create_objects')
    def _test_set_objective_create_objects(self, setup_env):
        """
        Test - create objects after objectives are set.
        """
        s = self.smarts[1]
        c = self.conditions[1]
        b = self.basics[1]
        self.copy_dataset(self.mount.path)
        # set objective on directory, create file
        d = self.get_dir(self.mount.path)
        f = self.get_file(d)
        ctx.cluster.cli.condition_update(name=c, pattern=os.path.basename(f)[:1]+'*', inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (c, b)])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path=self.abs_to_rel(d), 
                                            objective=s)
        self.client.write_to_file('%s/%s_bogus' % (d,os.path.basename(f)[:1]))
        self.client.write_to_file('%s/_bogus' % d)
        time.sleep(SLEEP)
        self.validate_assignment(d)
        self.client.remove('%s/%s_bogus' % (d,os.path.basename(f)[:1]))
        self.client.remove('%s/_bogus' % d)
        # set objective on directory, create directory
        d = self.get_dir(self.mount.path)
        ctx.cluster.cli.condition_update(name=c, pattern='*', inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (c, b)])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path=self.abs_to_rel(d), 
                                            objective=s)
        self.client.mkdir('%s/bogus' % d)
        f = str(uuid.uuid4())
        self.client.write_to_file('%s/bogus/%s' % (d, f))
        time.sleep(SLEEP)
        self.validate_assignment(d)
        self.client.remove('%s/bogus/%s' % (d, f))
        self.client.rmdir('%s/bogus' % d)
        # set objective on file
        d = self.get_dir(self.mount.path)
        f = self.get_file(d)
        ctx.cluster.cli.condition_update(name=c, pattern=os.path.basename(f)[:1]+'*', inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (c, b)])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path=self.abs_to_rel(f), 
                                            objective=s)
        time.sleep(SLEEP)
        self.validate_assignment(f)
# 
#     @attr('objectives', jira='next', complexity=1, name='set_objective_general_move_objects')
#     def test_set_objective_move_objects(self, setup_env):
#         
#         pass
# 
#     @attr('objectives', jira='next', complexity=1, name='smart_objective_general')
#     def test_smart_objective_general(self, setup_env):
#         
#         pass

    def create_fio_conf_file(self, filename, iops=10, period=60):
        """
        Generates a FIO configuration file.
        :param filename: A path to the file which will endure IOs
        :param iops: IO per seconds rate
        :param period: A time limit for the FIO tool's execution
        :return: A path to the FIO configuration file
        """
        f = "/tmp/fio.conf"
        contents = ["\[global\]", "time_based", "runtime=%ss" % str(period), "rw=randrw", 
                    "direct=1", "ioengine=libaio", "bs=4K", "rwmixread=50", "iodepth=64",
                    "\[job1\]", "name=job1", "filename=%s" % filename, "size=10M", 
                    "rate_iops=%s" % str(iops)]
        if self.client.exists(f) and self.client.isfile(f):
            self.client.remove(f)
        for line in contents:
            self.client.execute(["echo", line, ">>", f])
        return f
            
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
                    required =  ctx.cluster.basic_objectives[rules[i - 1][1]].internal_id
                break
            else:
                if i == len(rules) - 1:
                    required =  ctx.cluster.basic_objectives[rules[i][1]].internal_id
        return required

    @attr('objectives', jira='PD-15319', complexity=1, name='smart_objective_frequency_basic')
    def test_smart_objective_frequency_basic(self, setup_env):
        """
        Test - verifies correct SLO for a single frequency rule.
        """
        timeout = 425
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])]) 
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path="/",
                                            objective=s)
        try:
            frequency = 115
            f = self.create_fio_conf_file(fname, frequency / 2, 420)
            fio = FioCaps(conf_file=f)
            fio.generic([])
            thread = self.client.execute_tool(fio, self.mount, block=False)
            time.sleep(15)
            fd = self.client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()
            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(fname, self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()
            thread.get()
            assert required == slo, "expected slo %s found %s for %s, avg_iops %s, frequency %s" % (str(required), str(slo), fname, info.avg_iops, frequency)
            required = ctx.cluster.basic_objectives['default-objective'].internal_id
            time.sleep(timeout)
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            assert required == slo, "expected slo %s found %s for %s" % (str(required), str(slo), fname)
        finally:
            self.client.close_file(fd)
            self.client.close_agent()

    @attr('objectives', jira='PD-15571', complexity=1, name='smart_objective_frequency_basic_closed')
    def test_smart_objective_frequency_basic_closed(self, setup_env):
        """
        Test - verifies correct SLO for a single frequency rule.
        """
        timeout = 425
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])]) 
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path="/",
                                            objective=s)
        try:
            frequency = 115
            f = self.create_fio_conf_file(fname, frequency / 2, 420)
            fio = FioCaps(conf_file=f)
            fio.generic([])
            thread = self.client.execute_tool(fio, self.mount, block=False)
            time.sleep(15)
            fd = self.client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()
            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(fname, self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()
            thread.get()
            assert required == slo, "expected slo %s found %s for %s, avg_iops %s" % (str(required), str(slo), fname, info.avg_iops)
            required = ctx.cluster.basic_objectives['default-objective'].internal_id
            self.client.close_file(fd)
            if self.mount.version == NfsVersion.nfs3_dp:
                hlp.wait_for_no_layouts_inode(info.inode, self.mount.share, timeout=240)
            else:
                hlp.wait_for_no_layouts_inode(info.inode, self.mount.share, timeout=100)
            time.sleep(10)
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            assert required == slo, "expected slo %s found %s for %s" % (str(required), str(slo), fname)
        finally:
            self.client.close_agent()

    @attr('objectives', jira='PD-15572', complexity=1, name='smart_objective_frequency_basic_retro')
    def test_smart_objective_frequency_basic_retro(self, setup_env):
        """
        Test - verifies correct SLO for a single frequency rule.
        """
        timeout = 425
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]
        frequency = 115
        f = self.create_fio_conf_file(fname, frequency / 2, 435)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        time.sleep(15)
        try:
            fd = self.client.open_file(fname, os.O_CREAT | os.O_RDWR | os.O_SYNC, 0644)
            time.sleep(timeout)
            required = self.required_slo(frequency, rules)
            ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
            ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])]) 
            ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                                path="/",
                                                objective=s)
            time.sleep(10)
            thread.get()
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            assert required == slo, "expected slo %s found %s for %s" % (str(required), str(slo), fname)
        finally:
            self.client.close_file(fd)
            self.client.close_agent()


    @attr('objectives', jira='PD-15573', complexity=1, name='smart_objective_frequency_basic_changes')
    def test_smart_objective_frequency_basic_changes(self, setup_env):
        """
        Test - verifies correct SLO for a single frequency rule, with multiple activity changes.
        """
        timeout = 425
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])]) 
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path="/",
                                            objective=s)
        for _ in xrange(3):
            frequency = random.choice([55, 115])
            f = self.create_fio_conf_file(fname, frequency / 2, 420)
            fio = FioCaps(conf_file=f)
            fio.generic([])
            thread = self.client.execute_tool(fio, self.mount, block=False)
            time.sleep(15)
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()
            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(fname, self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()
            thread.get()
            assert required == slo, "expected slo %s found %s for %s" % (str(required), str(slo), fname)

    @attr('objectives', jira='PD-15533', complexity=1, name='smart_objective_frequency_basic_negative')
    def test_smart_objective_frequency_basic_negative(self, setup_env):
        """
        Test - verifies correct SLO for a single frequency rule that doesn't match.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 100)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])]) 
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path="/",
                                            objective=s)
        frequency = 55
        f = self.create_fio_conf_file(fname, frequency / 2, 420)
        fio = FioCaps(conf_file=f)
        fio.generic([])
        thread = self.client.execute_tool(fio, self.mount, block=False)
        thread.get()
        info = self.client.file_declare(fname, self.mount.share)
        slo = info.slo_ids[0]
        required = self.required_slo(frequency, rules)
        assert required == slo, "expected slo %s found %s for %s" % (str(required), str(slo), fname)

    @attr('objectives', jira='PD-13450', complexity=1, name='smart_objective_frequency_multi')
    def test_smart_objective_frequency_multi(self, setup_env):
        """
        Test - verifies correct SLO for different frequencies.
        """
        timeout = 455
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], 30), (self.conditions[1], self.basics[1], 60), (self.conditions[2], self.basics[2], 90)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', activity=rules[0][2], inactivity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[1][0], pattern='*', activity=rules[1][2], inactivity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[2][0], pattern='*', activity=rules[2][2], inactivity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1]), 
                                               "%s:%s" % (rules[1][0], rules[1][1]), 
                                               "%s:%s" % (rules[2][0], rules[2][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name, 
                                            path="/",
                                            objective=s)
        opened = False
        fd = None
        for _ in xrange(3):
            frequency = random.choice([15, 45, 75, 105])
            f = self.create_fio_conf_file(fname, frequency / 2, 420)
            fio = FioCaps(conf_file=f)
            fio.generic([])
            thread = self.client.execute_tool(fio, self.mount, block=False)
            time.sleep(15)
            if not opened:
                fd = self.client.open_file(fname, os.O_RDWR | os.O_SYNC, 0644)
                opened = True
            info = self.client.file_declare(fname, self.mount.share)
            slo = info.slo_ids[0]
            required = None
            t1 = t2 = time.time()
            while t2 - t1 < timeout:
                required = self.required_slo(frequency, rules)
                if required == slo:
                    break
                time.sleep(30)
                info = self.client.file_declare(fname, self.mount.share)
                slo = info.slo_ids[0]
                t2 = time.time()
            thread.get()
            assert required == slo, "expected slo %s found %s for %s, avg_iops %s" % (str(required), str(slo), fname, info.avg_iops)
        self.client.write_file(fd, 0, 1, 2)
        required = ctx.cluster.basic_objectives['default-objective'].internal_id
        t1 = t2 = time.time()
        try:
            while t2 - t1 < timeout + 60:
                info = self.client.file_declare(fname, self.mount.share)
                slo = info.slo_ids[0]
                if slo == required:
                    break
                time.sleep(10)
                self.client.read_file(fd, 0, 1, 2)
                t2 = time.time()
            assert required == slo, "expected slo %s found %s for %s, avg_iops %s" % (str(required), str(slo), fname, info.avg_iops)
        finally:
            self.client.close_file(fd)
            self.client.close_agent()

    def required_slo_by_inactivity(self, passed, rules, lag=TIMELAG):
        """
        Calculates the required SLO for a given passed time from a set of rules.
        :param passed: a given passed time in seconds
        :param rules: a list of rules
        :return: the required SLO
        """
        default_obj = ctx.cluster.basic_objectives['default-objective'].internal_id
        required = default_obj
        for i in xrange(len(rules) - 1, -1, -1):
            if passed < rules[i][3] - lag:
                required =  ctx.cluster.basic_objectives[rules[i][1]].internal_id
        return required

    # TODO: that method of testing is too loose... I need to refactor this, but it'll do for the time being 
    @attr('objectives', jira='PD-15003', complexity=1, name='smart_objective_recency_multi')
    def test_smart_objective_recency_multi(self, setup_env):
        """
        Test - assigns multiple recency rules and waits to verify assignment.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180), 
                 (self.conditions[1], self.basics[1], "360s", 360), 
                 (self.conditions[2], self.basics[2], "9m", 450)]
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[1][0], pattern='*', inactivity=rules[1][2], activity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[2][0], pattern='*', inactivity=rules[2][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1]), 
                                               "%s:%s" % (rules[1][0], rules[1][1]), 
                                               "%s:%s" % (rules[2][0], rules[2][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        slo = info.slo_ids[0]
        inode = info.inode
        # TODO: need to find how to determine the point in time that the file was closed
        if self.mount.version == NfsVersion.nfs3_dp:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=240)
        else:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=120)
        t1 = t2 = time.time()
        while t2 - t1 < 600:
            info = ctx.cluster.get_file_info(self.mount.share, inode)
            if slo != info.slo_ids[0]:
                slo = info.slo_ids[0]
                expected = self.required_slo_by_inactivity(t2 - t1, rules)
                assert slo == expected, "slo does not match the recency defined - time passed:%d" % (t2 - t1)
            time.sleep(1)               
            t2 = time.time()
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"
        
    @attr('objectives', jira='PD-15116', complexity=1, name='smart_objective_recency_multi_retro')
    def test_smart_objective_recency_multi_retro(self, setup_env):
        """
        Test - waits till 10m passed since close and assigns multiple rules, the biggest should be met.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180), 
                 (self.conditions[1], self.basics[1], "360s", 360), 
                 (self.conditions[2], self.basics[2], "9m", 450)]
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        inode = info.inode
        if self.mount.version == NfsVersion.nfs3_dp:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=240)
        else:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=120)
        time.sleep(600)
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[1][0], pattern='*', inactivity=rules[1][2], activity_clear=True)
        ctx.cluster.cli.condition_update(name=rules[2][0], pattern='*', inactivity=rules[2][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1]), 
                                               "%s:%s" % (rules[1][0], rules[1][1]), 
                                               "%s:%s" % (rules[2][0], rules[2][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        time.sleep(10)
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(600, rules), "slo does not match the recency defined"

    @attr('objectives', jira='PD-15117', complexity=1, name='smart_objective_recency_basic')
    def test_smart_objective_recency_basic(self, setup_env):
        """
        Test - assigns a < 3m recency rule and verify assignment for a 4 minutes period.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180)] 
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        slo = info.slo_ids[0]
        inode = info.inode
        # TODO: need to find how to determine the point in time that the file was closed
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
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"        

    @attr('objectives', jira='PD-15118', complexity=1, name='smart_objective_recency_basic_retro')
    def test_smart_objective_recency_basic_retro(self, setup_env):
        """
        Test - waits till 1m passed since close and assigns a < 3m recency rule.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180)] 
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        inode = info.inode
        if self.mount.version == NfsVersion.nfs3_dp:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=240)
        else:
            hlp.wait_for_no_layouts_inode(inode, self.mount.share, timeout=120)
        time.sleep(60)
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        time.sleep(10)
        t1 = time.time()
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        required = self.required_slo_by_inactivity(70, rules)
        assert slo == required, "slo does not match the recency defined"        
        time.sleep(170)
        required = self.required_slo_by_inactivity(250, rules)

        # ugly workaround to invalidate the pdfs-cli caching issue - PD-21657
        #info = ctx.cluster.get_file_info(self.mount.share, inode)

        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == required, "slo does not match the recency defined"        

    #@mark.skipif(True, reason="PD-15136")
    @attr('objectives', jira='PD-15119', complexity=1, name='smart_objective_recency_basic_md')
    def test_smart_objective_recency_basic_md(self, setup_env):
        """
        Test - assigns a > 3m recency rule and waits 4m to verify assignment, then change md and verifies once more.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180)] 
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        slo = info.slo_ids[0]
        inode = info.inode
        # TODO: need to find how to determine the point in time that the file was closed
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
            time.sleep(1)               
            t2 = time.time()
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"        
        self.client.execute(['touch', '-c', fname])
        time.sleep(10)
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"        
        self.client.execute(['chmod', 'ogu+x', fname])
        time.sleep(10)
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"        
        self.client.execute(['mv', fname, fname + '_1'])
        time.sleep(10)
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"                

    @attr('objectives', jira='PD-15574', complexity=1, name='smart_objective_recency_basic_md_exempt')
    def test_smart_objective_recency_basic_md_exempt(self, setup_env):
        """
        Test - assigns a > 3m recency rule and waits 4m to verify assignment, then changes size and verifies once more.
        """
        fname = self.mount.path + "/" + str(uuid.uuid4())
        s = self.smarts[0]
        rules = [(self.conditions[0], self.basics[0], "3m", 180)] 
        ctx.cluster.cli.condition_update(name=rules[0][0], pattern='*', inactivity=rules[0][2], activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=s, rule=["%s:%s" % (rules[0][0], rules[0][1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path="/",
                                            objective=s)
        self.client.execute(['dd', 'if=/dev/urandom' ,'of=%s' % fname, 'bs=1M', 'oflag=direct', 'count=100'])
        info = self.client.file_declare(fname, self.mount.share)
        slo = info.slo_ids[0]
        inode = info.inode
        # TODO: need to find how to determine the point in time that the file was closed
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
            time.sleep(1)               
            t2 = time.time()
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(t2 - t1, rules), "slo does not match the recency defined"        
        self.client.execute(['truncate', '-s', '150m', fname])
        time.sleep(10)
        info = ctx.cluster.get_file_info(self.mount.share, inode)
        slo = info.slo_ids[0]
        assert slo == self.required_slo_by_inactivity(0, rules), "slo does not match the recency defined"                

    @attr('objectives', jira='PD-16999', complexity=1, name='smart_objective_clone')
    def test_smart_objective_clone(self, setup_env):
        """
        Test - check the slo behavior on a clone pair
        """
        self.copy_dataset(self.mount.path)
        d = self.get_dir(self.mount.path)
        f = self.get_file(d)
        d2 = self.get_dir(self.mount.path)
        while d2 == d:
            d2 = self.get_dir(self.mount.path)
        f2 = self.get_file(d2)
        ctx.cluster.cli.condition_update(name=self.conditions[0], pattern="*", inactivity_clear=True, activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=self.smarts[0],
                                               rule=["%s:%s" % (self.conditions[0],
                                                                self.basics[0])])
        ctx.cluster.cli.condition_update(name=self.conditions[1], pattern="*", inactivity_clear=True, activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=self.smarts[1],
                                               rule=["%s:%s" % (self.conditions[1],
                                                                self.basics[1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(d),
                                            objective=self.smarts[0])
        info1 = self.client.file_declare(f, self.mount.share)
        inode1 = info1.inode
        if info1.slo_ids[0] != ctx.cluster.basic_objectives[self.basics[0]].internal_id:
            for _ in xrange(20):
                time.sleep(1)
                info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
                if info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id:
                    break
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        
        # clone a file
        ctx.cluster.clone(self.mount.share.path + self.abs_to_rel(f),
                        self.mount.share.path + self.abs_to_rel(f2))

        # hopefully this part is not causing cow
        info2 = self.client.file_declare(f2, self.mount.share)
        inode2 = info2.inode

        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(d2),
                                            objective=self.smarts[1])
        time.sleep(20)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        
        ctx.cluster.cli.share_objective_remove(name=self.mount.share.name,
                                               path=self.abs_to_rel(d2))

        time.sleep(20)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(d2),
                                            objective=self.smarts[1])
        ctx.cluster.cli.share_objective_remove(name=self.mount.share.name,
                                               path=self.abs_to_rel(d))

        time.sleep(20)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo - which is not the default one
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives['default-objective'].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives['default-objective'].internal_id
        
        self.client.remove(f)

        # wait for the sweeper
        time.sleep(120)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that target file has its slo
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[1]].internal_id        

    @attr('objectives', jira='PD-17009', complexity=1, name='smart_objective_clone_file_slo')
    def test_smart_objective_clone_file_slo(self, setup_env):
        """
        Test - check the slo behavior on a clone pair, objective is set directly on the files
        """
        self.copy_dataset(self.mount.path)
        f1 = self.get_file(self.mount.path)
        f2 = self.get_file(self.mount.path)
        while f1 == f2:
            f2 = self.get_file(self.mount.path)
        ctx.cluster.cli.condition_update(name=self.conditions[0], pattern="*", inactivity_clear=True, activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=self.smarts[0],
                                               rule=["%s:%s" % (self.conditions[0],
                                                                self.basics[0])])
        ctx.cluster.cli.condition_update(name=self.conditions[1], pattern="*", inactivity_clear=True, activity_clear=True)
        ctx.cluster.cli.smart_objective_update(name=self.smarts[1],
                                               rule=["%s:%s" % (self.conditions[1],
                                                                self.basics[1])])
        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(f1),
                                            objective=self.smarts[0])
        info1 = self.client.file_declare(f1, self.mount.share)
        inode1 = info1.inode
        time.sleep(5)

        # clone a file
        ctx.cluster.clone(self.mount.share.path + self.abs_to_rel(f1),
                        self.mount.share.path + self.abs_to_rel(f2))

        # hopefully this part is not causing cow
        info2 = self.client.file_declare(f2, self.mount.share)
        inode2 = info2.inode

        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(f2),
                                            objective=self.smarts[1])
        time.sleep(5)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        
        ctx.cluster.cli.share_objective_remove(name=self.mount.share.name,
                                               path=self.abs_to_rel(f2))

        time.sleep(5)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[0]].internal_id

        ctx.cluster.cli.share_objective_add(name=self.mount.share.name,
                                            path=self.abs_to_rel(f2),
                                            objective=self.smarts[1])
        ctx.cluster.cli.share_objective_remove(name=self.mount.share.name,
                                               path=self.abs_to_rel(f1))

        time.sleep(5)
        info1 = ctx.cluster.get_file_info(self.mount.share, inode1)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that both files has the source's slo - which is not the default one
        assert info1.slo_ids[0] == ctx.cluster.basic_objectives['default-objective'].internal_id
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives['default-objective'].internal_id
        
        self.client.remove(f1)

        # wait for the sweeper
        time.sleep(120)
        info2 = ctx.cluster.get_file_info(self.mount.share, inode2)
        # verify that target file has its slo
        assert info2.slo_ids[0] == ctx.cluster.basic_objectives[self.basics[1]].internal_id
