from nfs4_const import *
from nfs4_type import *
from environment import check, fail, use_obj, open_file, create_file, checklist, close_file
import nfs4_ops as op
from nfs4lib import get_nfstime
from pnfs_obj_v2_const import *
from pnfs_obj_v2_type import *
from obj_v2 import Packer as ObjV2Packer, Unpacker as ObjV2Unpacker
from layoutcheck import check_layout, check_devid, check_devid_flex, check_devid_flex

import socket
import math
import threading

def createWriteReadCloseClient1(sess1, fileOwner, filePath):

#    print "***** createWriteReadCloseClient1: Creating file '%s', with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = create_file(sess1, fileOwner, filePath, attrs={FATTR4_MODE: 0777}, want_deleg=True, access=OPEN4_SHARE_ACCESS_WANT_ANY_DELEG | OPEN4_SHARE_ACCESS_BOTH)
    check(res)
    fh = res.resarray[-1].object
    stateid = res.resarray[-2].stateid
    data = "write test data"
    res = sess1.compound([op.putfh(fh),
                          op.write(stateid, 5, FILE_SYNC4, data)])
    check(res)
    res = sess1.compound([op.putfh(fh),
                          op.read(stateid, 0, 1000)])
    check(res)
    if not res.resarray[-1].eof:
        fail("EOF not set on read")
    desired = "\0" * 5 + data
    if res.resarray[-1].data != desired:
        fail("Expected %r, got %r" % (desired, res.resarray[-1].data))
    else:
        print "Written file data read back OK"
    res = close_file(sess1, fh, stateid=stateid)
    check(res)


def test_FLEXFILE1(t, env):
    """Check basic multiple clients

    FLAGS: nfs-ff
    CODE: FLEXFILE1
    """
    sess1 = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid1 = res.resarray[-1].logr_stateid
    data = "write test data"
    res = sess1.compound([op.putfh(fh1),
                          op.write(open_stateid1, 5, FILE_SYNC4, data)])
    check(res)

    res = open_file(sess1, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid2 = res.resarray[-1].logr_stateid

    res = open_file(sess1, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh3 = res.resarray[-1].object
    open_stateid3 = res.resarray[-2].stateid

    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid3, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid3 = res.resarray[-1].logr_stateid

    res = open_file(sess1, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh4 = res.resarray[-1].object
    open_stateid4 = res.resarray[-2].stateid

    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid4, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid4 = res.resarray[-1].logr_stateid
    env.sleep(2)

    res4 = close_file(sess1, fh4, open_stateid4)
    check(res4)

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    env.sleep(2)

    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)


def testFLEXFILE2(t, env):
    """Check basic multiple clients

    FLAGS: nfs-ff
    CODE: FLEXFILE2
    """
    sess1 = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid1 = res.resarray[-1].logr_stateid
    data = "write test data"
    res = sess1.compound([op.putfh(fh1),
                          op.write(open_stateid1, 5, FILE_SYNC4, data)])
    check(res)
    sess2 = env.c2.new_client_session("2_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess2, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layout_stateid2 = res.resarray[-1].logr_stateid

    sess3 = env.c3.new_client_session("3_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess3, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)

    check(res)

    fh3 = res.resarray[-1].object
    open_stateid3 = res.resarray[-2].stateid

    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    layout_stateid3 = res.resarray[-1].logr_stateid

    sess4 = env.c4.new_client_session("4_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess4, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    check(res)

    fh4 = res.resarray[-1].object
    open_stateid4 = res.resarray[-2].stateid

    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid4, 0xffff)]
    res = sess4.compound(ops)

    check(res)

    layout_stateid4 = res.resarray[-1].logr_stateid

    res = close_file(sess1, fh1, open_stateid1)

    check(res)

    res = close_file(sess2, fh2, open_stateid2)
    check(res)

    res = close_file(sess3, fh3, open_stateid3)
    check(res)

    res = close_file(sess4, fh4, open_stateid4)
    check(res)

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    env.sleep(2)
    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid3,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)



def testFLEXFILE3 (t, env):
    """Multiopen for read, write, close from same client.
       Open_r&layoutget-->Open_w-->Delegreturn-->layoutget_rw-->close_r-->write-->close-->layoutreturn

    FLAGS: nfs-ff
    CODE: FLEXFILE3
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid  # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set  # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout from openClient2Read *********"
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open same fail for Write Client2Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess2.compound_async(env.home + [open_op])

    #print "*************res from openClient2Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"
    env.sleep(.1)
    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall.stateid)])
    check(res)

    #print "************* Now get OPEN reply*"
    res = sess2.listen(slot)
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

    #print "close the file1 from client2Read"
    res = close_file(sess2, fh2, stateid=stateid2)
    check(res)

    #print "Open the file from client2Write"
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return layout from client2Write *********"
    layouts2 = res.resarray[-1].logr_layout
    layout_stateid2 = res.resarray[-1].logr_stateid

    #print "write data to the file1 from client2Write"
    data = "sess2 with data for write....write test data from client2"
    res = sess2.compound([op.putfh(fh2),
                          op.write(stateid2, 5, FILE_SYNC4, data)])
    check(res)

    res = sess2.compound([op.putfh(fh2),
                          op.read(stateid2, 0, 1000)])
    check(res)

    res = close_file(sess2, fh2, stateid2)
    check(res)

    # layoutreturn after closed file
    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh2),
           op.layoutreturn(False,
                           LAYOUT4_FLEX_FILES,
                           LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)




def testFLEXFILE4(t, env):
    """Multiopen for read, write from one client and open for read from others client simultaneously.
      (OPen_w&layouget-->write-->Open_r&layoutget)C2--> (Open_r&layoutget_r)C3-->closeC2-->closeC3-->layoutupdate,commit,return C2

    FLAGS: nfs-ff
    CODE: FLEXFILE4
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid  # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set  # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)

    #print "***** Create - Client2 *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "write data to the file1 from client2Write"
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE,
                    deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return write layout  *********"
    layouts = res.resarray[-1].logr_layout
    lo_stateid = res.resarray[-1].logr_stateid

    #print "write data to the file1 from client2Write"
    data = "sess2 with data for write....write test data from client2"
    res = sess2.compound([op.putfh(fh2),
                          op.write(stateid2, 5, FILE_SYNC4, data)])
    check(res)

    #print "***** sess2 ***", res
    sessionName = env.testname(t) + "3"
    sess3 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2 Read=", res
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close
    res = sess3.compound([op.putfh(fh3),
                          op.read(stateid3, 0, 1000)])
   # print "***** sess3 ***", res

    #print "***** Create - Client3 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "***** Open same fail for Read client3*************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
    deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_NONE, want_deleg=False)
    #print "***********res from openClient4Read=", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    res = sess4.compound([op.putfh(fh4),
                          op.read(stateid4, 0, 1000)])

    #print "***** sess4 ***", res
    #print "*****  layout update / commit after closed file ***********"
    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh2),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    res = sess2.compound(ops)
    check(res)

    #print "close the file1 from client2Read"
    res = close_file(sess4, fh4, stateid4)
    check(res)

    res = close_file(sess2, fh2, stateid2)
    check(res)

    env.sleep(2)
    #print "***** Return Layot "

    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid, p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


def testFLEXFILE5(t, env):
    """Multiopen for read, write, close from same client.
       Open_r&layoutget-->Open_w-->Delegreturn-->layoutget_rw-->close_r-->write-->close-->layoutupdate,commit,return

    FLAGS: nfs-ff
    CODE: FLEXFILE5
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid  # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set  # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open some fail for Write Client2Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess2.compound_async(env.home + [open_op])

    #print "*************res from openClient3Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"
    env.sleep(.1)
    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall.stateid)])
    check(res)

    #print "************* Now get OPEN reply*"
    res = sess2.listen(slot)
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

#print "close the file1 from client2Read"
#  res = close_file(sess2, fh2, stateid=stateid2)
    res = sess2.compound([op.putfh(fh2),
                          op.read(stateid2, 0, 1000)])
    check(res)

    #print "write data to the file1 from client3Write"
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE,
                    deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return write layout  *********"
    layouts2 = res.resarray[-1].logr_layout
    lo_stateid2 = res.resarray[-1].logr_stateid

    #print "***** lo_stateid2==", lo_stateid2
    #print "write data to the file1 from client2Write"

    data = "sess2 with data for write....write test data from client2"
    res = sess2.compound([op.putfh(fh3),
                          op.write(stateid3, 5, FILE_SYNC4, data)])
    check(res)

    #print "***** res==", res
    #print "close file client2Write"


    res = sess2.compound([op.putfh(fh3),
                          op.read(stateid3, 0, 1000)])

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), True))

    ops = [op.putfh(fh3),
           op.layoutcommit(0, 0xffffffffffffffff, False, lo_stateid2,
                           newoffset4(True, 0), newtime4(True, get_nfstime()),
                           layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    res = sess2.compound(ops)
    check(res)

##    if roc:
##        state = NFS4_OK
##    else:
##        state = NFS4ERR_BADLAYOUT
##
#    res = close_file(sess2, fh3, stateid3)
#
##    print "***** Return Layot -  negative test"
    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0, 0xffffffffffffffff,
                                                            stateid3,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

def testFLEXFILE6(t, env):
    """Create a file1, open_r it from client2, open_w from client2, expected result is nfs4err_delay until file closed from  .

    FLAGS: nfs-ff
    CODE: FLEXFILE6
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid  # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set  # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

   #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout from openClient2Read *********"
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open same fail for Write Client2Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess2.compound_async(env.home + [open_op])

    #print "************* Wait for recall, and return delegation "
    recall.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"
    env.sleep(.1)
    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall.stateid)])
    check(res)

#print "************* Now get OPEN reply*"
    res = sess2.listen(slot)
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])
#    print "close the file1 from client2Read"

    res = sess2.compound([op.putfh(fh2),
                          op.read(stateid2, 0, 1000)])
    check(res)

    #print "Open the file from client2Write"
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE,
                    deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return layout from client2Write *********"
    layouts2 = res.resarray[-1].logr_layout
    layout_stateid2 = res.resarray[-1].logr_stateid

    #print "write data to the file1 from client2Write"
    data = "sess2 with data for write....write test data from client2"
    res = sess2.compound([op.putfh(fh3),
                          op.write(stateid3, 5, FILE_SYNC4, data)])
    check(res)

    res = sess2.compound([op.putfh(fh3),
                          op.read(stateid3, 0, 1000)])
    check(res)

 #   return
    #print "***** Create - Client3Wrie*************************** "
    sessionName = env.testname(t) + "3"
    sess3 = env.c3.new_client_session(sessionName)
    sess3.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess3.client.cb_post_hook(OP_CB_RECALL, post_hook)

    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE,
                    deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return layout from client2Write *********"
    layouts4 = res.resarray[-1].logr_layout
    layout_stateid4 = res.resarray[-1].logr_stateid

    data = "write test data"
    res = sess3.compound([op.putfh(fh4),
                          op.write(stateid4, 5, FILE_SYNC4, data)])

    res = sess3.compound([op.putfh(fh4),
                          op.read(stateid4, 0, 1000)])
    check(res)

    #print "*****  layout update / commit after closed file ***********"
    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh4),
           op.layoutcommit(0, 2 * 8192, False, layout_stateid4, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    res = sess3.compound(ops)
    check(res)

    res = close_file(sess2, fh3, stateid=stateid3)
    check(res)

    res = close_file(sess3, fh4, stateid=stateid4)
    check(res)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

def testFLEXFILE7(t, env):

    """Multiopen to read four clients, open_write and write from one.
              Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_w.

    FLAGS: nfs-ff
    CODE: FLEXFILE7
    """
    recall1 = threading.Event()
    recall2 = threading.Event()
    recall3 = threading.Event()
    recall4 = threading.Event()


    def pre_hook1(arg, env):
        recall1.stateid1 = arg.stateid  # NOTE this must be done before set()
        recall1.happened = True
        env.notify = recall1.set  # This is called after compound sent to queue

    def post_hook1(arg, env, res):
        return res

    def pre_hook2(arg, env):
        recall2.stateid2 = arg.stateid  # NOTE this must be done before set()
        recall2.happened = True
        env.notify = recall2.set  # This is called after compound sent to queue

    def post_hook2(arg, env, res):
        return res

    def pre_hook3(arg, env):
        recall3.stateid3 = arg.stateid  # NOTE this must be done before set()
        recall3.happened = True
        env.notify = recall3.set  # This is called after compound sent to queue

    def post_hook3(arg, env, res):
        return res

    def pre_hook4(arg, env):
        recall4.stateid4 = arg.stateid  # NOTE this must be done before set()
        recall4.happened = True
        env.notify = recall4.set  # This is called after compound sent to queue

    def post_hook4(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess1.client.cb_pre_hook(OP_CB_RECALL, pre_hook1)
    sess1.client.cb_post_hook(OP_CB_RECALL, post_hook1)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)
    env.sleep(1)
    res = open_file(sess1, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh1 = res.resarray[-1].object
    stateid1 = res.resarray[-2].stateid
    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid1 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook2)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook2)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid2 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client3Read *************************** "
    sessionName = env.testname(t) + "3"
    sess3 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess3.client.cb_pre_hook(OP_CB_RECALL, pre_hook3)
    sess3.client.cb_post_hook(OP_CB_RECALL, post_hook3)

    #print "***** openClient3Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid3 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client4 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c4.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook4)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook4)

    #print "***** openClient4 READ   file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient4Read", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid4 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open some fail for Write Client4Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess4.compound_async(env.home + [open_op])

    #print "*************res from openClient4Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall1.wait()  # STUB - deal with timeout
    recall2.wait()  # STUB - deal with timeout
    recall3.wait()  # STUB - deal with timeout
    recall4.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"

    res = sess1.compound([op.putfh(fh1), op.delegreturn(recall1.stateid1)])
    check(res)

    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall2.stateid2)])
    check(res)

    res = sess3.compound([op.putfh(fh3), op.delegreturn(recall3.stateid3)])
    check(res)

    res = sess4.compound([op.putfh(fh4), op.delegreturn(recall4.stateid4)])
    check(res)

    res = sess4.listen(slot)
    #print "***************deleg_res4***************", res
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

    #print "write data to the file1 from client4Write"
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)
    fh5 = res.resarray[-1].object
    stateid5 = res.resarray[-2].stateid
    ops = [op.putfh(fh5),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid5, 0xffff)]
    res = sess4.compound(ops)
    check(res)
    #print "***** Return write layout  *********"

    layouts5 = res.resarray[-1].logr_layout
    lo_stateid5 = res.resarray[-1].logr_stateid

    #print "***** lo_stateid4==", lo_stateid5
    #print "write data to the file1 from client4 Write"

    data = "sess2 with data for write....write test data from client4"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    #print "*****  layout update / commit after closed file ***********"

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh5),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid5, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    res = sess4.compound(ops)

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid5,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    #print "close file from session1,2,3 "
    res = close_file(sess1, fh1, stateid1)
    res = close_file(sess2, fh2, stateid2)
    res = close_file(sess3, fh3, stateid3)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid3,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4_OK
    else:
        state = NFS4ERR_BAD_STATEID

    check(res, state)


def testFLEXFILE8(t, env):

    """Multiopen to read 4 clients, open_write and write from one+ close_w session.
        Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_w.

    FLAGS: nfs-ff
    CODE: FLEXFILE8
    """
    recall1 = threading.Event()
    recall2 = threading.Event()
    recall3 = threading.Event()
    recall4 = threading.Event()


    def pre_hook1(arg, env):
        recall1.stateid1 = arg.stateid  # NOTE this must be done before set()
        recall1.happened = True
        env.notify = recall1.set  # This is called after compound sent to queue

    def post_hook1(arg, env, res):
        return res

    def pre_hook2(arg, env):
        recall2.stateid2 = arg.stateid  # NOTE this must be done before set()
        recall2.happened = True
        env.notify = recall2.set  # This is called after compound sent to queue

    def post_hook2(arg, env, res):
        return res

    def pre_hook3(arg, env):
        recall3.stateid3 = arg.stateid  # NOTE this must be done before set()
        recall3.happened = True
        env.notify = recall3.set  # This is called after compound sent to queue

    def post_hook3(arg, env, res):
        return res

    def pre_hook4(arg, env):
        recall4.stateid4 = arg.stateid  # NOTE this must be done before set()
        recall4.happened = True
        env.notify = recall4.set  # This is called after compound sent to queue

    def post_hook4(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess1.client.cb_pre_hook(OP_CB_RECALL, pre_hook1)
    sess1.client.cb_post_hook(OP_CB_RECALL, post_hook1)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)
    env.sleep(1)
    res = open_file(sess1, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh1 = res.resarray[-1].object
    stateid1 = res.resarray[-2].stateid
    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid1 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook2)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook2)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid2 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client3Read *************************** "
    sessionName = env.testname(t) + "3"
    sess3 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess3.client.cb_pre_hook(OP_CB_RECALL, pre_hook3)
    sess3.client.cb_post_hook(OP_CB_RECALL, post_hook3)

    #print "***** openClient3Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid3 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client4 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c4.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook4)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook4)

    #print "***** openClient4 READ   file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient4Read", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid4 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open some fail for Write Client4Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess4.compound_async(env.home + [open_op])

    #print "*************res from openClient4Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall1.wait()  # STUB - deal with timeout
    recall2.wait()  # STUB - deal with timeout
    recall3.wait()  # STUB - deal with timeout
    recall4.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"

    res = sess1.compound([op.putfh(fh1), op.delegreturn(recall1.stateid1)])
    check(res)

    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall2.stateid2)])
    check(res)

    res = sess3.compound([op.putfh(fh3), op.delegreturn(recall3.stateid3)])
    check(res)

    res = sess4.compound([op.putfh(fh4), op.delegreturn(recall4.stateid4)])
    check(res)

    res = sess4.listen(slot)
    #print "***************deleg_res4***************", res
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

    #print "write data to the file1 from client4Write"
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)
    fh5 = res.resarray[-1].object
    stateid5 = res.resarray[-2].stateid
    ops = [op.putfh(fh5),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid5, 0xffff)]
    res = sess4.compound(ops)
    check(res)
    #print "***** Return write layout  *********"

    layouts5 = res.resarray[-1].logr_layout
    lo_stateid5 = res.resarray[-1].logr_stateid

    #print "***** lo_stateid4==", lo_stateid5
    #print "write data to the file1 from client4 Write"

    data = "sess4 with data for write....write test data from client4"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    #print "*****  layout update / commit after closed file ***********"

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh5),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid5, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid5,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    #print "close file from session1,2,3 "
    res = close_file(sess1, fh1, stateid1)
    res = close_file(sess2, fh2, stateid2)
    res = close_file(sess3, fh3, stateid3)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid3,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    #print "layoutreturn to read layout - exepcted results is ok "
    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4_OK
    else:
        state = NFS4ERR_BAD_STATEID

    check(res, state)
    #print "close for read layout - exepcted results is ok stateid4", stateid4
    res = close_file(sess4, fh4, stateid4)

    #print "layoutreturn to read layout - exepcted results is NFS4ERR_BAD_STATEID "
    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


def testFLEXFILE9(t, env):

    """Multiopen to read four clients, open_write and write from one + close _r session.
        Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_r
                                                                --> write_C4_w, commit, layoureturn_OK, close,layoureturn_BAD_SESSION.

    FLAGS: nfs-ff
    CODE: FLEXFILE9
    """
    recall1 = threading.Event()
    recall2 = threading.Event()
    recall3 = threading.Event()
    recall4 = threading.Event()


    def pre_hook1(arg, env):
        recall1.stateid1 = arg.stateid  # NOTE this must be done before set()
        recall1.happened = True
        env.notify = recall1.set  # This is called after compound sent to queue

    def post_hook1(arg, env, res):
        return res

    def pre_hook2(arg, env):
        recall2.stateid2 = arg.stateid  # NOTE this must be done before set()
        recall2.happened = True
        env.notify = recall2.set  # This is called after compound sent to queue

    def post_hook2(arg, env, res):
        return res

    def pre_hook3(arg, env):
        recall3.stateid3 = arg.stateid  # NOTE this must be done before set()
        recall3.happened = True
        env.notify = recall3.set  # This is called after compound sent to queue

    def post_hook3(arg, env, res):
        return res

    def pre_hook4(arg, env):
        recall4.stateid4 = arg.stateid  # NOTE this must be done before set()
        recall4.happened = True
        env.notify = recall4.set  # This is called after compound sent to queue

    def post_hook4(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess1.client.cb_pre_hook(OP_CB_RECALL, pre_hook1)
    sess1.client.cb_post_hook(OP_CB_RECALL, post_hook1)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)
    env.sleep(1)
    res = open_file(sess1, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh1 = res.resarray[-1].object
    stateid1 = res.resarray[-2].stateid
    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid1 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook2)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook2)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid2 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client3Read *************************** "
    sessionName = env.testname(t) + "3"
    sess3 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess3.client.cb_pre_hook(OP_CB_RECALL, pre_hook3)
    sess3.client.cb_post_hook(OP_CB_RECALL, post_hook3)

    #print "***** openClient3Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid3 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client4 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c4.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook4)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook4)

    #print "***** openClient4 READ   file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient4Read", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid4 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close
    #print "***** read_state4  *********"
    res = sess4.compound([op.putfh(fh4),
                          op.read(stateid4, 0, 1000)])
    check(res)
    #print "***** Open some fail for Write Client4Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess4.compound_async(env.home + [open_op])

    #print "*************res from openClient4Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall1.wait()  # STUB - deal with timeout
    recall2.wait()  # STUB - deal with timeout
    recall3.wait()  # STUB - deal with timeout
    recall4.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"

    res = sess1.compound([op.putfh(fh1), op.delegreturn(recall1.stateid1)])
    check(res)

    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall2.stateid2)])
    check(res)

    res = sess3.compound([op.putfh(fh3), op.delegreturn(recall3.stateid3)])
    check(res)

    res = sess4.compound([op.putfh(fh4), op.delegreturn(recall4.stateid4)])
    check(res)

    res = sess4.listen(slot)
    #print "***************deleg_res4***************", res
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

    #print "write data to the file1 from client4Write"
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)
    fh5 = res.resarray[-1].object
    stateid5 = res.resarray[-2].stateid
    ops = [op.putfh(fh5),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid5, 0xffff)]
    res = sess4.compound(ops)
    check(res)
    #print "***** Return write layout  *********"

    layouts5 = res.resarray[-1].logr_layout
    lo_stateid5 = res.resarray[-1].logr_stateid

    #print "***** lo_stateid4==", lo_stateid5
    #print "write data to the file1 from client4 Write"

    data = "sess4 with data for write....write test data from client4"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    #print "close file from session1,2,3 "
    res = close_file(sess1, fh1, stateid1)
    res = close_file(sess2, fh2, stateid2)
    res = close_file(sess3, fh3, stateid3)

    print "*****  layout after closed file ***********"
    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid3,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    data = "sess4 with data for write....write test data from client4  secound loop"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    print "*****  layout update / commit after closed file ***********"
    ops = [op.putfh(fh5),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid5, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess4.compound(ops)

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot *****************"

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid5,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)



def testFLEXFILE10(t, env):

    """Multiopen to read four clients, open_write and write from two.
                   Clients C1,2,3 --> opens_r, clientC4 open_r, C4(open_w, delegreturn C4--> C1,2,3,4_r),C3(open_w, delegreturn C3--> C1,2,3,4_r). close all, layoutreturn_bad_session.

    FLAGS: nfs-ff
    CODE: FLEXFILE10
    """
    recall1 = threading.Event()
    recall2 = threading.Event()
    recall3 = threading.Event()
    recall4 = threading.Event()

    def pre_hook1(arg, env):
        recall1.stateid1 = arg.stateid  # NOTE this must be done before set()
        recall1.happened = True
        env.notify = recall1.set  # This is called after compound sent to queue

    def post_hook1(arg, env, res):
        return res

    def pre_hook2(arg, env):
        recall2.stateid2 = arg.stateid  # NOTE this must be done before set()
        recall2.happened = True
        env.notify = recall2.set  # This is called after compound sent to queue

    def post_hook2(arg, env, res):
        return res

    def pre_hook3(arg, env):
        recall3.stateid3 = arg.stateid  # NOTE this must be done before set()
        recall3.happened = True
        env.notify = recall3.set  # This is called after compound sent to queue

    def post_hook3(arg, env, res):
        return res

    def pre_hook4(arg, env):
        recall4.stateid4 = arg.stateid  # NOTE this must be done before set()
        recall4.happened = True
        env.notify = recall4.set  # This is called after compound sent to queue

    def post_hook4(arg, env, res):
        return res

    #print "Create sess1 and client1"
    sessionName = env.testname(t) + "1"
    sess1 = env.c1.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess1.client.cb_pre_hook(OP_CB_RECALL, pre_hook1)
    sess1.client.cb_post_hook(OP_CB_RECALL, post_hook1)

    fileOwner = "owner_%s" % sessionName
    fileName = env.testname(t)
    filePath = sess1.c.homedir + [fileName]

    #print "********************* Creating a file with NO deleg"
    createWriteReadCloseClient1(sess1, fileOwner, filePath)
    env.sleep(1)
    res = open_file(sess1, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh1 = res.resarray[-1].object
    stateid1 = res.resarray[-2].stateid
    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid1 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client2Read *************************** "
    sessionName = env.testname(t) + "2"
    sess2 = env.c2.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess2.client.cb_pre_hook(OP_CB_RECALL, pre_hook2)
    sess2.client.cb_post_hook(OP_CB_RECALL, post_hook2)

    #print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid2 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client3Read *************************** "
    sessionName = env.testname(t) + "3"
    sess3 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess3.client.cb_pre_hook(OP_CB_RECALL, pre_hook3)
    sess3.client.cb_post_hook(OP_CB_RECALL, post_hook3)

    #print "***** openClient3Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient2Read=", res
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid3 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Create - Client4 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c4.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook4)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook4)

    #print "***** openClient4 READ   file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    #print "***********res from openClient4Read", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid4 = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    #print "***** Open some fail for Write Client4Write *************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    open_op = op.open(0, OPEN4_SHARE_ACCESS_WRITE, OPEN4_SHARE_DENY_NONE, owner, how, claim)
    slot = sess4.compound_async(env.home + [open_op])

    #print "*************res from openClient4Write ===========", res
    #print "************* Wait for recall, and return delegation "
    recall1.wait()  # STUB - deal with timeout
    recall2.wait()  # STUB - deal with timeout
    recall3.wait()  # STUB - deal with timeout
    recall4.wait()  # STUB - deal with timeout
    #print "************* Getting here means CB_RECALL reply is in the send queue."
    #print "************* Give it a moment to actually be sent"

    res = sess1.compound([op.putfh(fh1), op.delegreturn(recall1.stateid1)])
    check(res)

    res = sess2.compound([op.putfh(fh2), op.delegreturn(recall2.stateid2)])
    check(res)

    res = sess3.compound([op.putfh(fh3), op.delegreturn(recall3.stateid3)])
    check(res)

    res = sess4.compound([op.putfh(fh4), op.delegreturn(recall4.stateid4)])
    check(res)

    res = sess4.listen(slot)
    #print "***************deleg_res4***************", res
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

 #   #print "write data to the file1 from client4Write"
    res = open_file(sess4, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)
    fh5 = res.resarray[-1].object
    stateid5 = res.resarray[-2].stateid
    ops = [op.putfh(fh5),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid5, 0xffff)]
    res = sess4.compound(ops)
    check(res)
    #print "***** Return write layout  *********"

    layouts5 = res.resarray[-1].logr_layout
    lo_stateid5 = res.resarray[-1].logr_stateid

#   #print "***** lo_stateid4==", lo_stateid5
#    #print "write data to the file1 from client4 Write"

    data = "sess4 with data for write....write test data from client4"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    #print "***** Open some fail for Write Client3 *************************** "
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)
    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh5),
           op.layoutget(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

##    #print "***** Return write layout  *********"

    layouts6 = res.resarray[-1].logr_layout
    lo_stateid6 = res.resarray[-1].logr_stateid

    #print "***** lo_stateid6==", lo_stateid6
    #print "write data to the file1 from client3 Write"

    data = "sess3 with data for write....write test data from client3"
    res = sess3.compound([op.putfh(fh3),
                          op.write(stateid3, 5, FILE_SYNC4, data)])
    check(res)
    res = sess3.compound([op.putfh(fh3),
                          op.read(stateid3, 0, 1000)])
    check(res)

 #   print "*****  layout update / commit after closed file ***********"

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh5),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid5, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_FLEX_FILES, p.get_buffer()))]

    res = sess4.compound(ops)

 #   print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

  #  print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid5,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh3),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid6, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess3.compound(ops)

    #print "close file client3Write"
    res = close_file(sess3, fh3, stateid3)
    check(res)

    #print "***** Return Layot "

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid6,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

#    print "close file from session1,2,3 "
    res = close_file(sess1, fh1, stateid1)
    check(res)
    res = close_file(sess2, fh2, stateid2)
    check(res)
    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
 #   print " sess2 stateid", res
    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_FLEX_FILES, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)
 #   print " sess1 stateid", res
    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

