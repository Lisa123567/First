#__author__ = 'mleopold'


from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
import random
import string
from contextlib import contextmanager
from tonat.nfsctl import NfsVersion
from tonat.product.objects.mount import Mount
from client.models.ShareView import ShareView
from pytest import yield_fixture
from time import sleep 


default_export_option = "*,RW,no-root-squash"
cusername = "admin"
cpassword = "admin"
pdfs = "/pd_fs"

class TestShareOps(TestBase):
    
    # TODO: add the node + DS dependency as a fixture
    @yield_fixture
    def setup_env(self):
        yield

    def fix_punct(self, char):
        if char == "/":
            return ""
        if char in string.punctuation:
            return "\\" + char
        return char

    def generate_name(self, length, illegal=False):
        if length <= 0:
            return ""
        if not illegal:
            return ''.join(random.choice(string.ascii_uppercase + 
                                         string.ascii_lowercase + 
                                         string.digits) for _ in range(length))
        else:
            return ''.join(self.fix_punct(random.choice(string.ascii_uppercase + 
                                                        string.ascii_lowercase + 
                                                        string.digits +
                                                        string.punctuation)) 
                           for _ in range(length))
        
    @contextmanager
    def verify_share_creation(self):
        before_shares = dict(ctx.cluster.shares)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1", 
                                   "-type", "d", "-name", "0x\*"])
        before_counter = len(res.stdout.split('\n'))
        yield
        after_shares = dict(ctx.cluster.shares)
        added = set(after_shares.values()) - set(before_shares.values())
        # Verify that the share was created at the SM level
        assert len(added) == 1, "share was not created or something weird happened"
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1", 
                                   "-type", "d", "-name", "0x\*"])
        after_counter = len(res.stdout.split('\n'))
        # Verify that the share was created at the PDFS level
        assert after_counter - before_counter == 1, "KVS was not created or something weird happened"
        # Verify that the share was created at the PROTOD level
        mnt_template = Mount(list(added)[0], NfsVersion.nfs4_1)
        mount = ctx.clients[0].nfs.mount(mnt_template)
        ctx.clients[0].nfs.umount(mount)        

    @contextmanager
    def verify_failed_share_creation(self, path, check_mount=True):
        before_shares = dict(ctx.cluster.shares)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1", 
                                   "-type", "d", "-name", "0x\*"])
        before_counter = len(res.stdout.split('\n'))       
        yield
        after_shares = dict(ctx.cluster.shares)
        # Verify that the share was not created at the SM level
        assert len(set(after_shares.values()) - set(before_shares.values())) == 0, "share should not have been created"
        #res = ctx.cluster.execute('find %s -maxdepth 1 -type d -name 0x\*' % pdfs)
        res = ctx.cluster.execute(["find", "%s" % pdfs, "-maxdepth", "1", 
                                   "-type", "d", "-name", "0x\*"])
        after_counter = len(res.stdout.split('\n'))
        # Verify that the share was not created at the PDFS level
        assert after_counter - before_counter == 0, "irrelevant KVS directory was found"
        share = ShareView(path=path)
        mnt_template = Mount(share, NfsVersion.nfs4_1)
        # Verify that the share was not created at the PROTOD level
        if check_mount:
            try:
                mount = ctx.clients[0].nfs.mount(mnt_template)
            except:           
                return        
            ctx.clients[0].nfs.umount(mount)
            raise AssertionError("mount should not have succeeded")

    def verify_share_deletion(self):
        pass

    def verify_failed_share_deletion(self):
        pass

    @attr(jira='', complexity=1, name='share_create')
    def test_share_create(self, setup_env):
        name = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = default_export_option
        with self.verify_share_creation():
            ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                         clish_username=cusername, clish_password=cpassword)

    
    @attr(jira='', complexity=1, name='share_create_too_long_name')
    def test_share_create_too_long_name(self, setup_env):
        name = self.generate_name(256)
        path = "/" + self.generate_name(10)
        export_option = default_export_option
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')
    
    @attr(jira='', complexity=1, name='share_create_empty_name')
    def test_share_create_empty_name(self, setup_env):
        name = ""
        path = "/" + self.generate_name(10)
        export_option = default_export_option
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')
        

    @attr(jira='', complexity=1, name='share_create_using_taken_name')
    def test_share_create_using_taken_name(self, setup_env):
        name = self.generate_name(10)
        path = "/" + self.generate_name(10)
        export_option = default_export_option
        with self.verify_share_creation():
            ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                         clish_username=cusername, clish_password=cpassword)
        with self.verify_failed_share_creation(path=path, check_mount=False):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')


    @attr(jira='', complexity=1, name='share_create_empty_path')
    def test_share_create_empty_path(self, setup_env):
        name = self.generate_name(10)
        path = ""
        export_option = default_export_option
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')

    
    @attr(jira='', complexity=1, name='share_create_invalid_path')
    def test_share_create_invalid_path(self, setup_env):
        name = self.generate_name(10)
        path = "chipopo"
        export_option = default_export_option
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')

        path = "\invalid"
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')
                
        path = " "
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')

        # The below tests are for weird names, we expect the shares to be created
        for _ in xrange(10):
            path = self.generate_name(10, illegal=True)
            with self.verify_share_creation():
                ctx.cluster.cli.share_create(name=name, path="/"+path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
                
    @attr(jira='', complexity=1, name='share_create_using_root_path')
    def test_share_create_using_root_path(self, setup_env):
        name = self.generate_name(10)
        path = "/"
        export_option = default_export_option
        with self.verify_failed_share_creation(path=path):
            try:
                ctx.cluster.cli.share_create(name=name, path=path, export_option=export_option, 
                                             clish_username=cusername, clish_password=cpassword)
            except:
                return
            raise AssertionError('share create should have failed')

    
    @attr(jira='', complexity=1, name='share_create_using_taken_path')
    def test_share_create_using_taken_path(self, setup_env):
        # TODO: needs to verify the expected behavior
        # afaik, a share can have only one path, it can get a path with different depths,
        # the patch may be partially non existent, so a sort of mkdir -p will run on the namespace
        # and the actual dir where the share to me mounted must not exist
        # should have 2 verifications here: using a path used by another share, and using an existing
        # path. the verification should check the the existing share did not become corrupted
        pass    
    
    # TODO: export options tests
    # export options were not implemented according to requirements, 
    # we need to wait before being able to write tests for them

    # TODO: think if share create has more advanced test needs (like share delete will have for example)

    # TODO: share delete & share update tests
