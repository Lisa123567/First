#!/usr/bin/env python
# -*- coding: UTF-8 -*-

__author__ = 'fgelcer'

from random import choice
from paramiko.ssh_exception import SSHException

from tonat.libtests import attr
from tonat.context import ctx
from tonat.test_base import TestBase
from tonat.errors import CliOperationFailed, Failure
import pd_common.utils as utils


list_to_create_map = {'Name': 'name',
                      'First-Name': 'first_name',
                      'Last-Name': 'last_name',
                      'ID': 'user_id',
                      'Email': 'email',
                      'Role': 'role_name',
                      'Enabled': 'enabled',
                      'Public Key': 'public_key'}

admin_clish_user = 'admin'
admin_clish_password = 'admin'

class TestUserMgmtCli(TestBase):
    def setup(self):
        self.verify_base()

    def verify_base(self):
        res = self.user_list(full=True, clish_username=admin_clish_user, clish_password=admin_clish_password)
        if len(res) > 1:
            self.clean_users(res)

    def clean_users(self, users, by_id=False):
        for user in users:
            if user['name'] != 'admin':
                if by_id:
                    print "DELETING User: %s" % user['user_id']
                    self.delete_user(_id=user['user_id'])
                    # ctx.cluster.cli.user_delete(user_id=user['user_id'], clish_username=admin_clish_user, clish_password=admin_clish_password)
                else:
                    print "DELETING User: %s" % user['name']
                    self.delete_user(_name=user['name'])
                    # ctx.cluster.cli.user_delete(name=user['name'], clish_username=admin_clish_user, clish_password=admin_clish_password)

    def delete_user(self, _name=None, _id=None, _clish_username=admin_clish_user, _clish_password=admin_clish_password):
        ctx.cluster.cli.user_delete(name=_name, user_id=_id, clish_username=_clish_username, clish_password=_clish_password)

    email = 'a@a.co'
    password = '1qazXSW@'

    def user_list(self, name=None, user_id=None, full=None, clish_username=admin_clish_user, clish_password=admin_clish_password):
        users = ctx.cluster.cli.user_list(name=name, user_id=user_id, full=full, clish_username=clish_username, clish_password=clish_password)
        users_list = list()
        for user in users:
            user = {list_to_create_map[k]: v for (k, v) in user.items()}
            users_list.append(user)
        return users_list

    def update_user_list(self, fields, users):
        for user in users:
            for field in fields.keys():
                user[field] = fields[field]
        return users

    def user_password_update(self, user, new_password, old_password, by_id=False, clish_username=admin_clish_user, clish_password=admin_clish_password):
        if by_id:
            # ctx.cluster.cli.user_password_update(name=user['user_id'], new_password=new_password, old_password=old_password,
            #                                  clish_username=admin_clish_user, clish_password=admin_clish_password)
            ctx.cluster.cli.user_password_update(user_id=user['user_id'], new_password=new_password, old_password=old_password)
        else:
            # ctx.cluster.cli.user_password_update(name=user['name'], new_password=new_password, old_password=old_password,
            #                                  clish_username=admin_clish_user, clish_password=admin_clish_password)
            ctx.cluster.cli.user_password_update(name=user['name'], new_password=new_password, old_password=old_password)

    def add_user_id(self, users):
        for user in users:
            user['user_id'] = self.user_list(name=user['name'])[0]['user_id']
        return users

    def comp_users_res(self, users, results, full=False):
        if len(users) == len(results):
            passed = False
            for i in xrange(len(results)):
                if full:
                    if not (any(str(results[i]['name']) in u['name'] for u in users) or
                            any(str(results[i]['email']) in u['email'] for u in users) or
                            any(str(results[i]['role_name']) in u['role_name'] for u in users) or
                            any(str(results[i]['first-name']) in u['first_name'] for u in users) or
                            any(str(results[i]['last-name']) in u['last_name'] for u in users) or
                            any(str(results[i]['public_key']) in u['public_key'] for u in users)):
                        raise Failure('Username did not fit the test\n'
                                      + ' number of users ' + str(len(results))
                                      + ' user: ' + str(results)
                                      + ' curr_user ' + str(results[i]['Name']))
                else:
                    if not (any(str(results[i]['name']) in u['name'] for u in users)):
                        raise Failure('Username did not fit the test\n'
                                      + ' number of users ' + str(len(results))
                                      + ' username: ' + str(results)
                                      + ' curr_user ' + str(results[i]['Name']))
        else:
            raise Failure('Number of users did not fit the users requested\nnumber of users received %d and number of users requested %d' %
                          (len(results), len(users)))

    def _create_user_list(self, num, full=False, fields=dict()):
        users = list()
        for i in xrange(num):
            username = utils.lower_generator(15) #range(3, 12, 1))
            
            role = choice(ctx.cluster.roles.keys())
            kwargs = dict(name=username,
                          role_name=role,
                          email=self.email,
                          password=self.password)
            if full:
                kwargs.update(dict(first_name=utils.uld_mix_generator(15),
                                   last_name=utils.uld_mix_generator(15),
                                   public_key=utils.upper_generator(6)))
            else:
                if len(fields) > 0:
                    for key, _ in fields.iteritems():
                        fields[key] = utils.uld_mix_generator(15)
                    kwargs.update(fields)
            users.append(kwargs)
        return users

    def negative_user_create(self, username, password):
        with self.raises(CliOperationFailed) as exc_info:
            # have to create a user here, but because it will not have permission for that, it will never succeed
            user = self._create_user_list(1)[0]
            ctx.cluster.cli.user_create(name=user['name'],
                                        email=user['email'],
                                        role_name=user['role_name'],
                                        password=user['password'],
                                        clish_username=username,
                                        clish_password=password)

    def negative_user_update(self, username, password):
        with self.raises(CliOperationFailed) as exc_info:
            # have to create a user here, but because it will not have permission for that, it will never succeed
            ctx.cluster.cli.user_update(name=username,
                                        email='update@email.test.com',
                                        clish_username=username,
                                        clish_password=password)

    def __create_users(self, num, full=False, clish_user=admin_clish_user, clish_password=admin_clish_password, fields=dict(),
                       users_list=None, update=False, verify=True, by_id=False):
        """
        :rtype : object
        """
        has_admin = False
        if users_list:
            users = users_list
        else:
            users = self._create_user_list(num, full, fields)
        for user in users:
            if 'name' in user.keys() and (not user['name'] == 'admin'):
                if (by_id) and (not 'id' in user.keys()):
                    user = self.add_user_id([user])[0]
                if update:
                    if 'password' in fields.keys():
                        self.user_password_update(user=user, new_password=fields['password'],
                                                  old_password=user['password'], by_id=by_id)
                        user['password'] = fields['password']
                    kwargs = dict(clish_username=admin_clish_user, clish_password=admin_clish_password)
                    kwargs.update(fields)
                    kwargs.update(user)
                    if 'role_name' in kwargs.keys():
                        kwargs['role_name'] = kwargs.pop('role_name')
                    kwargs.pop('password')
                    if by_id:
                        kwargs.pop('name')
                    else:
                        if 'user_id' in kwargs:
                            kwargs.pop('user_id')
                    ctx.cluster.cli.user_update(**kwargs)
                else:
                    if 'user_id' in user.keys():
                        user.pop('user_id')
                    for field in fields:
                        user[field] = fields[field]
                    created_user = ctx.cluster.cli.user_create(**user)
                if verify:
                    self.check_user(user)
            else:
                has_admin = True
        if not update and not has_admin:
            user_admin = ctx.cluster.users['admin']
            users.append({'email': user_admin.email, 'user_id': user_admin.uoid.uuid, 'name': user_admin.username,
                          'password': 'admin', 'role_name': user_admin.role})
            has_admin = False
        result = self.user_list(full=True, clish_username=admin_clish_user, clish_password=admin_clish_password)
        for r in result:
            for user in users:
                if r['name'] != 'admin' and r['name'] == user['name']:
                    r['password'] = user['password']
        return users, result

    def check_user(self, user):
        username = user['name']
        password = user['password']

        # check if the user appears on user-list command using ssh
        res = self.user_list(name=username, clish_username=username, clish_password=password)
        res2 = self.user_list(user_id=res[0]['user_id'], clish_username=username, clish_password=password)

        # if admin, the create/update should succeed
        role = res[0]['role_name']
        if role == 'admin':
            (users, res) = self.__create_users(1, clish_user=username, clish_password=password, verify=False)
            test_user = self.user_list(name=users[0]['name'])
            fields = {'email': 'zz@pd.com'}
            # users_update = self.update_user_list(fields, users)
            (users_updated, res_updated) = self.__create_users(0, users_list=users, update=True, fields=fields)

            if users[0]['email'] != users_updated[0]['email']:
                raise Failure('The update did not really updated the field email\nOriginal users list : {0}'
                              + '\nNew users list: {1}'.format(users, users_updated))
            self.clean_users(test_user)

        # if dataowner, the create should fail and the update should succeed
        elif role == 'dataowner':
            self.negative_user_create(username, password)

            #create must be with user admin, as dataowner cannot create a user
            (users, res) = self.__create_users(1, clish_user=admin_clish_user, clish_password=admin_clish_password, verify=False)
            test_user = self.user_list(name=users[0]['name'])
            fields = {'email': 'zz@pd.com'}
            # users_update = self.update_user_list(fields, users)
            (users_updated, res_updated) = self.__create_users(0, users_list=users, update=True, fields=fields)

            if users[0]['email'] != users_updated[0]['email']:
                raise Failure('The update did not really updated the field email\nOriginal users list : {0}'
                              + '\nNew users list: {1}'.format(users, users_updated))
            self.clean_users(test_user)

        # if viewer, the create/update should fail
        elif role == 'viewer':
            self.negative_user_create(username, password)
            self.negative_user_update(username, password)

    def check_user_delete(self, users):
        for i in xrange(len(users)):
            if users[i]['name'] != 'admin':
                with self.raises(CliOperationFailed) as exc_info:
                    self.user_list(name=users[i]['name'], clish_username=admin_clish_user, clish_password=admin_clish_password)
                with self.raises(SSHException) as exc_info:
                    self.user_list(clish_username=users[i]['name'], clish_password=users[i]['password'])

    @attr('cli', 'user_mgmt_cli', jira='PD-4472', tool='cli', complexity=1, name='test_1_list_user')
    def test_1_list_user(self):
        (users, result) = self.__create_users(1)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4473', tool='cli', complexity=1, name='test_2_list_ten_users')
    def test_2_list_ten_users(self):
        (users, result) = self.__create_users(10)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4474', complexity=1, tool='cli', name='test_3_list_300_users')
    def test_3_list_300_users(self):
        (users, result) = self.__create_users(300)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4475', tool='cli', complexity=1, name='test_4_list_3000_users')
    def test_4_list_3000_users(self):
        (users, result) = self.__create_users(3000, verify=False)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4476', tool='cli', complexity=1, name='test_5_list_full_user')
    def test_5_list_full_user(self):
        full = True
        (users, result) = self.__create_users(1, full=full)
        self.comp_users_res(users, result, full=full)
        self.clean_users(result)


    @attr('cli', 'user_mgmt_cli', jira='PD-4477', tool='cli', complexity=1, name='test_6_list_full_ten_users')
    def test_6_list_full_ten_users(self):
        full = True
        (users, result) = self.__create_users(10, full=full)
        self.comp_users_res(users, result, full=full)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4478', complexity=1, tool='cli', name='test_7_list_full_300_users')
    def test_7_list_full_300_users(self):
        full = True
        (users, result) = self.__create_users(300, full=full)
        self.comp_users_res(users, result, full=full)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4479', tool='cli', complexity=1, name='test_8_list_full_3000_users')
    def test_8_list_full_3000_users(self):
        full = True
        (users, result) = self.__create_users(3000, full=full)
        self.comp_users_res(users, result, full=full)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4480', tool='cli', complexity=1, name='test_9_list_users_first_name')
    def test_9_list_users_first_name(self):
        (users, result) = self.__create_users(1, fields={'first_name': utils.uld_mix_generator(15)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('user_mgmt_cli', 'cli', jira='PD-4481', complexity=1, tool='cli', name='test_10_list_users_last_name')
    def test_10_list_users_last_name(self):
        (users, result) = self.__create_users(1, fields={'last_name': utils.uld_mix_generator(15)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4482', tool='cli', complexity=1, name='test_11_list_users_public_key')
    def test_11_list_users_public_key(self):
        (users, result) = self.__create_users(1, fields={'public_key': utils.upper_generator(6)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4483', tool='cli', complexity=1, name='test_12_list_users_first_and_last_name')
    def test_12_list_users_first_and_last_name(self):
        (users, result) = self.__create_users(1, fields={'first_name': utils.uld_mix_generator(15), 'last_name': utils.uld_mix_generator(15)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4484', tool='cli', complexity=1, name='test_13_list_users_first_name_and_public_key')
    def test_13_list_users_first_name_and_public_key(self):
        (users, result) = self.__create_users(1, fields={'first_name': utils.uld_mix_generator(15), 'public_key': utils.upper_generator(6)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4485', tool='cli', complexity=1, name='test_14_list_users_last_name_and_public_key')
    def test_14_list_users_last_name_and_public_key(self):
        (users, result) = self.__create_users(1, fields={'last_name': utils.uld_mix_generator(15), 'public_key': utils.upper_generator(6)})
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4486', complexity=1, tool='cli', name='test_15_create_31_char_username')
    def test_15_create_31_char_username(self):
        user_list = self._create_user_list(1)
        user_list[0]['name'] = utils.lower_generator(31)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4487', tool='cli', complexity=1, name='test_16_create_user_255_char_first_name')
    def test_16_create_user_255_char_first_name(self):
        user_list = self._create_user_list(1)
        user_list[0]['first_name'] = utils.uld_mix_generator(255)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('user_mgmt_cli', 'cli', jira='PD-4488', complexity=1, tool='cli', name='test_17_create_user_255_char_last_name')
    def test_17_create_user_255_char_last_name(self):
        user_list = self._create_user_list(1)
        user_list[0]['last_name'] = utils.uld_mix_generator(255)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4489', tool='cli', complexity=1, name='test_18_create_user_1_char_first_name')
    def test_18_create_user_1_char_first_name(self):
        user_list = self._create_user_list(1)
        user_list[0]['first_name'] = utils.uld_mix_generator(1)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4490', tool='cli', complexity=1, name='test_19_create_user_1_char_last_name')
    def test_19_create_user_1_char_last_name(self):
        user_list = self._create_user_list(1)
        user_list[0]['last_name'] = utils.uld_mix_generator(1)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4491', tool='cli', complexity=1, name='test_20_create_user_1_char_username')
    def test_20_create_user_1_char_username(self):
        user_list = self._create_user_list(1)
        user_list[0]['name'] = utils.lower_generator(1)
        (users, result) = self.__create_users(1, users_list=user_list)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4589', tool='cli', complexity=1, name='test_21_create_user_has_failed_before')
    def test_21_create_user_has_failed_before(self):
        users = self._create_user_list(1, fields={'email': utils.lower_generator(5)})
        with self.raises(CliOperationFailed) as exc_info:
            (users, result) = self.__create_users(1, users_list=users)
        users[0]['email'] = 'valid@email.com'
        (users, result) = self.__create_users(1, users_list=users)
        self.comp_users_res(users, result)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4492', tool='cli', complexity=1, name='test_22_update_users_email')
    def test_22_update_users_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'email': 'moshe@aaaa.aaaa.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4493', tool='cli', complexity=1, name='test_23_update_users_first_name')
    def test_23_update_users_first_name(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'first_name': 'first_name_test'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4494', tool='cli', complexity=1, name='test_24_update_users_last_name')
    def test_24_update_users_last_name(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'last_name': 'last_name_test'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4495', tool='cli', complexity=1, name='test_25_update_users_role')
    def test_25_update_users_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4496', complexity=1, tool='cli', name='test_26_update_users_first_name_email')
    def test_26_update_users_first_name_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'email': 'moshe@aaaa.aaaa.com', 'first_name': 'new first name'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4497', tool='cli', complexity=1, name='test_27_update_users_first_name_role')
    def test_27_update_users_first_name_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'role_name': new_role, 'first_name': 'another name'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4498', tool='cli', complexity=1, name='test_28_update_users_email_role')
    def test_28_update_users_email_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'role_name': new_role, 'email': 'email@new.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4499', complexity=1, tool='cli', name='test_29_update_users_first_name_last_name')
    def test_29_update_users_first_name_last_name(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'first_name': 'new first name', 'last_name': 'new last_name'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4514', tool='cli', complexity=1, name='test_30_update_users_first_name_last_name_role')
    def test_30_update_users_first_name_last_name_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first name', 'last_name': 'new last_name', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4515', tool='cli', complexity=1, name='test_31_update_users_first_name_email_role')
    def test_31_update_users_first_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first name', 'email': 'new@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4516', tool='cli', complexity=1, name='test_32_update_users_last_name_email')
    def test_32_update_users_last_name_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'last_name': 'new last_name', 'email': 'new@email.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4517', tool='cli', complexity=1, name='test_33_update_users_last_name_role')
    def test_33_update_users_last_name_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'last_name': 'new last_name', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4518', tool='cli', complexity=1, name='test_34_update_users_first_name_last_name_email')
    def test_34_update_users_first_name_last_name_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'first_name': 'new first_name', 'last_name': 'new last_name', 'email': 'new@email.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4519', tool='cli', complexity=1, name='test_35_update_users_last_name_email_role')
    def test_35_update_users_last_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'last_name': 'new last_name', 'email': 'my@newemail.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4520', tool='cli', complexity=1, name='test_36_update_users_first_name_last_name_email_role')
    def test_36_update_users_first_name_last_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first_name', 'last_name': 'new last_name', 'email': 'new@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4521', tool='cli', complexity=1, name='test_37_update_by_id_users_first_name')
    def test_37_update_by_id_users_first_name(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'first_name': 'first_name_test'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4522', tool='cli', complexity=1, name='test_38_update_by_id_users_last_name')
    def test_38_update_by_id_users_last_name(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'last_name': 'last_name_test'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4523', tool='cli', complexity=1, name='test_39_update_by_id_users_email')
    def test_39_update_by_id_users_email(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'email': 'new@email.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4524', tool='cli', complexity=1, name='test_40_update_by_id_users_role')
    def test_40_update_by_id_users_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4525', tool='cli', complexity=1, name='test_41_update_by_id_users_first_name_last_name')
    def test_41_update_by_id_users_first_name_last_name(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'first_name': 'new first_name', 'last_name': 'new last_name'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4526', tool='cli', complexity=1, name='test_42_update_by_id_users_first_name_email')
    def test_42_update_by_id_users_first_name_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'first_name': 'new first_name', 'email': 'email@new.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4527', tool='cli', complexity=1, name='test_43_update_by_id_users_first_name_role')
    def test_43_update_by_id_users_first_name_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new_name', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4528', tool='cli', complexity=1, name='test_44_update_by_id_users_last_name_email')
    def test_44_update_by_id_users_last_name_email(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'last_name': 'new last_name', 'email': 'email@new.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4529', tool='cli', complexity=1, name='test_45_update_by_id_users_last_name_role')
    def test_45_update_by_id_users_last_name_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'last_name': 'new_last_name', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4530', tool='cli', complexity=1, name='test_46_update_by_id_users_email_role')
    def test_46_update_by_id_users_email_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'email': 'new@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4531', tool='cli', complexity=1, name='test_47_update_by_id_users_first_name_last_name_email')
    def test_47_update_by_id_users_first_name_last_name_email(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'first_name': 'new first_name', 'last_name': 'new last_name', 'email': 'aaaa@new.com'}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4532', tool='cli', complexity=1, name='test_48_update_by_id_users_first_name_last_name_role')
    def test_48_update_by_id_users_first_name_last_name_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first_name', 'last_name': 'new last_name', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4533', tool='cli', complexity=1, name='test_49_update_by_id_users_first_name_email_role')
    def test_49_update_by_id_users_first_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first_name', 'email': 'my@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4534', tool='cli', complexity=1, name='test_50_update_by_id_users_last_name_email_role')
    def test_50_update_by_id_users_last_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'last_name': 'new last name', 'email': 'my@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4535', tool='cli', complexity=1, name='test_51_update_by_id_users_first_name_last_name_email_role')
    def test_51_update_by_id_users_first_name_last_name_email_role(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': 'new first name', 'last_name': 'new last name', 'email': 'my@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4595', tool='cli', complexity=1, name='test_52_update_by_id_users_public_key')
    def test_52_update_by_id_users_public_key(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'public_key': utils.upper_generator(6)}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4596', tool='cli', complexity=1, name='test_53_update_by_id_users_password')
    def test_53_update_by_id_users_password(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        fields = {'password': utils.passwd_generator(14)}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4597', tool='cli', complexity=1, name='test_54_update_users_public_key_password')
    def test_54_update_users_public_key_password(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        fields = {'public_key': utils.upper_generator(6), 'password': utils.passwd_generator(14)}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4598', tool='cli', complexity=1, name='test_55_update_users_all_fields')
    def test_55_update_users_all_fields(self):
        (users, result) = self.__create_users(1, verify=False, full=True)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': utils.lower_generator(8), 'last_name': utils.lower_generator(8),
                  'public_key': utils.upper_generator(6), 'password': utils.passwd_generator(14),
                  'email': 'new@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4599', tool='cli', complexity=1, name='test_56_update_by_id_users_all_fields')
    def test_56_update_by_id_users_all_fields(self):
        (users, result) = self.__create_users(1, verify=False)
        self.comp_users_res(users, result)
        new_role = 'viewer' if users[0]['role_name'] == 'admin' else 'admin'
        fields = {'first_name': utils.lower_generator(8), 'last_name': utils.lower_generator(8),
                  'public_key': utils.upper_generator(6), 'password': utils.passwd_generator(14),
                  'email': 'new@email.com', 'role_name': new_role}
        (users_updated, result2) = self.__create_users(0, users_list=result, update=True, by_id=True, fields=fields)
        self.comp_users_res(users_updated, result2)
        self.clean_users(result)

    @attr('cli', 'user_mgmt_cli', jira='PD-4536', tool='cli', complexity=1, name='test_57_del_a_user_by_name')
    def test_57_del_a_user_by_name(self):
        (users, result) = self.__create_users(1, verify=False)
        self.clean_users(result)
        self.check_user_delete(users)

    @attr('cli', 'user_mgmt_cli', jira='PD-4537', tool='cli', complexity=1, name='test_58_del_a_user_by_id')
    def test_58_del_a_user_by_id(self):
        (users, result) = self.__create_users(1, full=True, verify=False)
        self.clean_users(result, by_id=True)
        self.check_user_delete(users)

    @attr('cli', 'user_mgmt_cli', jira='PD-4652', tool='cli', complexity=1, name='test_59_create_user_as_deleted_one')
    def test_59_create_user_as_deleted_one(self):
        (users, result) = self.__create_users(1, full=True, verify=False)
        self.clean_users(result)
        self.check_user_delete(users)
        (users2, result2) = self.__create_users(1, full=True, users_list=result)
        self.comp_users_res(users, result2, full=True)
        self.comp_users_res(users2, result2, full=True)
        self.clean_users(result2)

# there is a need to add a few more tests for the command user-password-update, as trying to update password
# with wrong credentials, from a different user, from a user with different permission levels...