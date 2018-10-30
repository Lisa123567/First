#!/usr/bin/env python
# -*- coding: UTF-8 -*-
from random import choice


__author__ = 'elie'

from itertools import chain

from _pytest.python import yield_fixture

from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.context import ctx
#from tonat.product.objects.share.dd_share import DdShare
from tonat.product.objects.mount import Mount, NfsVersion
from pd_common import utils

bag_list = '/sys/fs/tfs2/mdfs/11111111-1111-1111-1111-111111111111/bag/'
obs_list = '/sys/fs/tfs2/mdfs/11111111-1111-1111-1111-111111111111/bag/*/shard/'
#label = 'l=`pdcli ds-list --id $(basename $i)|grep -A3 Labels:|awk "{print$2}"`'
#num_of_files = 'count=`cat $i/objects_used`'
#basename = '$(basename $i)'
mnt_point1 = '/mnt/test'

labels = ['create-dss', 'other1-dss', 'default-dss', 'other2-dss', 'other3-dss']

oos_simple_rules = ["Create,ANY\<=4/1h,%s,CREATE" % labels[0],
             "Other1,WRITE\<=10/1h,%s" % labels[1],                                         
             "Default,NOOP,%s,DEFAULT" % labels[2]]

oos1_rules = ["Create,ANY\<=4/1h,%s,CREATE" % labels[0],
             "Other1,WRITE\<=10/1h,%s" % labels[1],                                         
             "Default,NOOP,%s,DEFAULT" % labels[2]]

oos2_rules = ["Create,ANY\<=4/1h,%s,CREATE" % labels[0],
             "Other1,WRITE\<=10/1h,%s" % labels[1], 
             "Other2,WRITE\>=10/1h,%s" % labels[3],                                        
             "Default,NOOP,%s,DEFAULT" % labels[2]]

oos3_rules = ["Create,ANY\<=4/1h,%s,CREATE" % labels[0],
             "Other1,WRITE\<=10/1h,%s" % labels[1], 
             "Other2,READ\>10/1h,%s" % labels[3],    
             "Other3,ANY\=10/1h,%s" % labels[4],                                     
             "Default,NOOP,%s,DEFAULT" % labels[2]]


oos_share = 'oos'
oos_cls= 'oos_cls'
ds_type = 'PD'
ds_name = 'DS'

class TestOutOfSpace(TestBase):
    
    def main_setup(self, num_of_dses, stack_list):
        for (num, label) in zip (num_of_dses, labels):
            if label not in ctx.cluster.labels:
                existing_dses = 0
            else:
                existing_dses = len(ctx.cluster.labels[label].obses)
                self._logger.info('existing_dses is: %s\nNum is: %s\n' % (existing_dses, num))        
            self.create_ds(num - existing_dses, label)

        for (share ,cl, rules) in stack_list:
            if cl not in ctx.cluster.classifiers:
                for rule in rules:
                    label = rule.split(',')[2]
                    if label not in ctx.cluster.data_profiles:
                        self.create_profile(label)
                self.create_classifier(cl, rules)
            if share not in ctx.cluster.shares:
                self.create_share(share, cl)


    @yield_fixture()
    def setup3(self):
        num_of_dses = [1, 1, 1]
        stack_list = [(oos_share, oos_cls, oos_simple_rules)]       
        self.main_setup(num_of_dses, stack_list)
        yield

    @yield_fixture()
    def setup1(self):
        num_of_dses = [3, 4, 3]
        stack_list = [(oos_share, oos_cls, oos1_rules)]       
        self.main_setup(num_of_dses, stack_list)
        yield
        
    @yield_fixture()
    def setup2(self):
        num_of_dses = [3, 2, 3, 2]
        stack_list = [(oos_share, oos_cls, oos2_rules)]       
        self.main_setup(num_of_dses, stack_list)
        yield

    @yield_fixture()
    def setup4(self):
        num_of_dses = [2, 2, 2, 2, 2]
        stack_list = [(oos_share, oos_cls, oos3_rules)]       
        self.main_setup(num_of_dses, stack_list)
        yield

    def create_ds(self, num, label):
        DS_ID = len(ctx.cluster.obses)
        for _ in xrange(num):
            ds_ip = ctx.data_stores[DS_ID].address
#            num_of_obses = len([obs for label_ in ctx.cluster.labels.itervalues() for obs in label_.obses])
            export_path = choice(ctx.data_stores[DS_ID].shares.keys())
            ds_ip_and_export_path = ds_ip + ':' + export_path
            ctx.cluster.cli.ds_add(ds_name + str(DS_ID), ds_ip, ds_ip_and_export_path, ds_type, label)
            self._logger.info('num is: %s\nds_name is: %s\nds_ip_and_export_path is: %s\n    label is: %s\n'
                               % (num, ds_name + str(DS_ID), ds_ip_and_export_path, label))
            DS_ID += 1
  
        
    def create_classifier(self, cls_name, rule):
        ctx.cluster.cli.classifier_create(cls_name, rule)        
        
    def create_share(self, share_name, cls):
        ctx.cluster.cli.share_create(share_name, cls)
        
    def create_profile(self, label):
        ctx.cluster.cli.data_profile_create(label, [label])        
        
    def call_mount(self, share_name, mount_point):
        if not ctx.clients[0].exists(mount_point):
            ctx.clients[0].mkdirs(mount_point)
        share = ctx.cluster.shares[share_name]
        mnt = Mount(share, NfsVersion.pnfs,
                         path=str(mount_point))
        mnt_cl = ctx.clients[0].nfs.mount(mnt)
        self._logger.info('mnt_cl is: %s' % mnt_cl)
        return mnt_cl

    def call_umount(self, mnt_cl):
        ctx.clients[0].clean_dir(mnt_cl.path)
        ctx.clients[0].nfs.umount(mnt_cl)   

    def create_files_on_client(self, num_of_files,  name, mount_point, bs=1, count=1, block=True):
        
        num = 10
        if num_of_files == 0:
            num_of_files = num
            
        if not name:
            name = 'file'    
        i=0
        reslist = []
        for i in range(num_of_files):
            if block:
                #sleep(sleep_time)
                pass
            res = ctx.clients[0].execute(['dd', 'if=/dev/zero', 'of={0}/{1}{2}'.format(mount_point, name, i), 'bs=%s'% bs, 'count=%s'% count], block=block)
            reslist.append(res)
        return reslist

    @attr(jira='next', complexity=1, name='test_verify_file_location')
    def test_verify_file_location(self):
        bags = ctx.cluster.listdir(bag_list)
        obses = [ctx.cluster.listdir('{0}{1}/shard'.format(bag_list, bag)) for bag in bags]       
        obses = list(chain.from_iterable(obses))
        res = []
        obs_label = None
        obs_obj = None
        for i in xrange(len(obses)):
            for label in ctx.cluster.labels.itervalues():
                for obs in label.obses:
                    if obs.id == obses[i]:
                        obs_label = label
                        obs_obj = obs
            if obs_label is None:
                assert False, 'label wasnt found for this obs'
            
            num_of_files = ctx.cluster.execute(['cat',obs_list + obses[i] + '/objects_used'])
            res.append('obs: %s label: [%s] export: %s num_of_files: %s' % (obses[i], obs_label.name, 
                                                                            obs_obj.ds_share.name, 
                                                                            num_of_files.stdout))
        for i in xrange(len(res)):
            self._logger.info('%s' % res[i])
                    

    @attr(jira='next', complexity=1, name='test_print_exports')        
    def test_print_exports(self, setup):
        ds_ip = ctx.data_stores[0].address
        exports = len(ctx.data_stores[0].shares.keys())
        for i in range (exports):
            ds_ip_and_export_path = ds_ip + ':' + ctx.data_stores[0].shares.keys()[i]
            self._logger.info('test_print_exports--- export list is: %s\n' % ds_ip_and_export_path)

    @attr(jira='next', complexity=1, name='test_oos_3_labels_simple')
    def test_oos_3_labels_simple(self, setup3):
        
        file_name = 'oos_file_'
        mnt = self.call_mount(oos_share,mnt_point1)
        files = 100000
        for i in xrange(files):
            bs = utils.digit_range_generator(2)
            count = utils.digit_range_generator(5)
            self._logger.info('test_oos --- bs is: %s, count is: %s\n' % (bs, count))
            self.create_files_on_client(1, file_name + str(i), mnt_point1, bs, count)
        ctx.cluster.shares[oos_share].verify_location()
        self.mddb_coherency(oos_share)
        #self.call_umount(mnt)

    @attr(jira='next', complexity=1, name='test_oos_3_labels')
    def test_oos_3_labels(self, setup1):
        
        file_name = 'oos_file'
        mnt = self.call_mount(oos_share,mnt_point1)
        files = 10000
        for i in xrange(files):
            bs = utils.digit_range_generator(2)
            count = utils.digit_range_generator(5)
            self._logger.info('test_oos --- bs is: %s, count is: %s\n' % (bs, count))
            self.create_files_on_client(1, file_name + str(i), mnt_point1, bs, count)
        self.test_verify_file_location()
        #self.call_umount(mnt)


    @attr(jira='next', complexity=1, name='test_oos_4_labels')
    def test_oos_4_labels(self, setup2):
        
        file_name = 'oos_file'
        mnt = self.call_mount(oos_share,mnt_point1)
        files = 10000
        for i in xrange(files):
            bs = utils.digit_range_generator(2)
            count = utils.digit_range_generator(5)
            self._logger.info('test_oos --- bs is: %s, count is: %s\n' % (bs, count))
            self.create_files_on_client(1, file_name + str(i), mnt_point1, bs, count)
        self.test_verify_file_location()
        #self.call_umount(mnt)  
        
        
    @attr(jira='next', complexity=1, name='test_oos_5_labels')
    def test_oos_5_labels(self, setup4):
        
        file_name = 'oos_file'
        mnt = self.call_mount(oos_share,mnt_point1)
        files = 10000
        for i in xrange(files):
            bs = utils.digit_range_generator(2)
            count = utils.digit_range_generator(5)
            self._logger.info('test_oos --- bs is: %s, count is: %s\n' % (bs, count))
            self.create_files_on_client(1, file_name + str(i), mnt_point1, bs, count)
        self.test_verify_file_location()
        #self.call_umount(mnt)
