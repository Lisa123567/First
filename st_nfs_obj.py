from nfs4_const import *
from nfs4_type import *
from environment import check, fail, use_obj, open_file, create_file, checklist, close_file
import nfs4_ops as op
from nfs4lib import get_nfstime
from pnfs_obj_v2_const import *
from pnfs_obj_v2_type import *
from obj_v2 import Packer as ObjV2Packer, Unpacker as ObjV2Unpacker
from layoutcheck import check_layout, check_devid

import socket
import math
import threading


def testNfsObj(t, env):
    """Test for NFS-Obj support

    (RFC 5661 12.6: When a pNFS client encounters a new FSID,
    it sends a GETATTR to the NFSv4.1 server for the fs_layout_type)

    AUTHOR: Nadav

    FLAGS: nfs-obj nfs-obj2 nfs-obj-recall
    CODE: NFSOBJ
    """
    c1 = env.c1.new_client(env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    if not c1.flags & EXCHGID4_FLAG_USE_PNFS_MDS:
        fail("Server did not set EXCHGID4_FLAG_USE_PNFS_MDS")

    sess = c1.create_session()

    ops = use_obj(env.opts.path) + [op.getattr(1 << FATTR4_FS_LAYOUT_TYPES)]
    res = sess.compound(ops)
    check(res)

    if FATTR4_FS_LAYOUT_TYPES not in res.resarray[-1].obj_attributes:
        fail("fs_layout_type not available")

    if LAYOUT4_OBJECTS_V2 not in \
        res.resarray[-1].obj_attributes[FATTR4_FS_LAYOUT_TYPES]:
        fail("layout_type does not contain NFS-OBJ")


def testGetNfsObjDevList(t, env):
    """Check devlist

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: GETNFSOBJDLIST1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    # Send GETDEVICELIST
    ops = use_obj(env.opts.path) + \
        [op.getdevicelist(LAYOUT4_OBJECTS_V2,  # gdla_layout_type
                          0xffffffff,  # gdla_maxdevices
                          0,  # gdla_cookie
                          "")]  # gdla_cookieverf
    res = sess.compound(ops)

    if res.status == NFS4ERR_NOTSUPP:
        t.pass_warn("GETDEVICELIST not supported")

    check(res)

    dev_list = res.resarray[-1].gdlr_deviceid_list
    cookie = res.resarray[-1].gdlr_cookie
    cookieverf = res.resarray[-1].gdlr_cookieverf
    eof = res.resarray[-1].gdlr_eof

    while not eof:
        ops = [op.getdevicelist(LAYOUT4_OBJECTS_V2,
                                0xffffffff,
                                cookie,
                                cookieverf)]
        res = sess.compound(ops)
        check(res)

        dev_list = dev_list + res.resarray[-1].gdlr_deviceid_list
        cookie = res.resarray[-1].gdlr_cookie
        cookieverf = res.resarray[-1].gdlr_cookieverf
        eof = res.resarray[-1].gdlr_eof

    for dev_id in dev_list:
        check_devid(sess, dev_id)


def testGetNfsObjLayout(t, env):
    """Verify layout handling
       Also tests GETDEVICEINFO for the device-ids in the layout

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: GETNFSOBJLAYOUT1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid
    open_stateid.seqid = 0

    ops = [op.putfh(fh),
           op.layoutget(False,  # loga_signal_layout_avail
                        LAYOUT4_OBJECTS_V2,  # loga_layout_type
                        LAYOUTIOMODE4_READ,  # loga_iomode
                        0,  # loga_offset
                        0xffffffffffffffff,  # loga_length
                        0,  # loga_minlength
                        open_stateid,  # loga_stateid
                        0xffff)]  # loga_maxcount
    res = sess.compound(ops)
    check(res)

    # Parse opaque
    for layout in res.resarray[-1].logr_layout:
        check_layout(sess, layout)


def testGetNfsObjWriteLayout(t, env):
    """Verify layout handling

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: GETNFSOBJLAYOUT2
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Parse opaque
    for layout in  res.resarray[-1].logr_layout:
        check_layout(sess, layout)


def testGetNFSOBJLayout3(t, env):
    '''Test Duplicate layoutget with open stateid instead of
    layout stateid for the second request.

    (RFC5661 12.5.2: In order to get layout the client MUST first open a file
    and present an open stateid, Thereafter, the client MUST use a layout stateid.)

    AUTHOR: Avi

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJOPENSTATEID
    '''
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Parse opaque
    for layout in  res.resarray[-1].logr_layout:
        check_layout(sess, layout)

    # TODO: catch the error should be raise due to duplicate open stateid
    # and treat as PASS

    # another kayoutget with open stateid
    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testGetNFSOBJLayout4(t, env):
    '''Test layoutget with delegation stateid instead of open stateid
    (RFC5661 12.5.2: when a client has no layout on a file it MUST present
    an open stateid, a delegation stateid, or a byte-range lock stateid in the
    loga_stateid arguments.)

    AUTHOR: Avi

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJDELEGSTATEID
    '''
    # c1 - create a file and open for RW
    sess = env.c1.new_client_session(env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = create_file(sess, env.testname(t),
                      access=OPEN4_SHARE_ACCESS_READ |
                      OPEN4_SHARE_ACCESS_WANT_READ_DELEG)
    check(res)

    fh = res.resarray[-1].object
    deleg = res.resarray[-2].delegation.read

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, deleg.stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)
#
    # Parse opaque
    for layout in res.resarray[-1].logr_layout:
        check_layout(sess, layout)

    # return delegation
    res = sess.compound([op.putfh(fh), op.delegreturn(deleg.stateid)])
    check(res)


def testNfsObjROC1(t, env):
    """
    Check layoutreturn after close depends on return_on_close flag

    (RFC5661 Errata ID: 3226: When the metadata server sets this return value to TRUE,
    the client MUST NOT use the layout and the respective layout stateid after
    sending the last CLOSE of that file)

    AUTHOR: Avi

    FLAGS: nfs-obj
    DEPEND: GETNFSOBJLAYOUT1
    CODE: NFSOBJROC
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    # close file before layout return
    res = close_file(sess=sess,
                     fh=fh,
                     stateid=open_stateid)

    # layoutreturn after closed file
    ops = [op.putfh(fh),
           op.layoutreturn(False,  # lora_reclaim
                           LAYOUT4_OBJECTS_V2,  # lora_layout_type
                           LAYOUTIOMODE4_ANY,  # lora_iomode
                           layoutreturn4(LAYOUTRETURN4_FILE,  # lora_layoutreturn
                                         layoutreturn_file4(0,  # lrf_offset
                                                            0xffffffffffffffff,  # lrf_length
                                                            layout_stateid,  # lrf_stateid
                                                            "")))]  # lrf_body
    res = sess.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


def testNfsObjROC2(t, env):
    """
    Check layoutcommit after close depends on return_on_close flag

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: GETNFSOBJLAYOUT1
    CODE: NFSOBJROC2
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close

    # close file before layout commit
    res = close_file(sess=sess,
                     fh=fh,
                     stateid=open_stateid)

    # layoutcommit after closed file
    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    # layoutcommit after closed file
    ops = [op.putfh(fh),
           op.layoutcommit(0, 2 * 8192, False, layout_stateid,
                           newoffset4(True, (2 * 8192) - 1), time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]
    res = sess.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)

    if not roc:
        # Return layout
        ops = [op.putfh(fh),
               op.layoutreturn(False,  # lora_reclaim
                               LAYOUT4_OBJECTS_V2,  # lora_layout_type
                               LAYOUTIOMODE4_ANY,  # lora_iomode
                               layoutreturn4(LAYOUTRETURN4_FILE,  # lora_layoutreturn
                                             layoutreturn_file4(0,  # lrf_offset
                                                                0xffffffffffffffff,  # lrf_length
                                                                layout_stateid,  # lrf_stateid
                                                                "")))]  # lrf_body
        res = sess.compound(ops)
	check(res)


def testNfsObjLayoutReturnFile(t, env):
    """
    Return a file's layout

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: GETNFSOBJLAYOUT1
    CODE: NFSOBJLAYOUTRET1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutreturn(False,  # lora_reclaim
                           LAYOUT4_OBJECTS_V2,  # lora_layout_type
                           LAYOUTIOMODE4_ANY,  # lora_iomode
                           layoutreturn4(LAYOUTRETURN4_FILE,  # lora_layoutreturn
                                         layoutreturn_file4(0,  # lrf_offset
                                                            0xffffffffffffffff,  # lrf_length
                                                            layout_stateid,  # lrf_stateid
                                                            "")))]  # lrf_body
    res = sess.compound(ops)
    check(res)


def testNfsObjLayoutReturnFsid(t, env):
    """
    Return all of a filesystem's layouts

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: GETNFSOBJLAYOUT1
    CODE: NFSOBJLAYOUTRET2
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    ops = use_obj(env.opts.path) + [op.layoutreturn(False,
                                                    LAYOUT4_OBJECTS_V2,
                                                    LAYOUTIOMODE4_ANY,
                                                    layoutreturn4(LAYOUTRETURN4_FSID))]
    res = sess.compound(ops)
    check(res)


def testNfsObjLayoutReturnAll(t, env):
    """
    Return all of a client's layouts

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: GETNFSOBJLAYOUT1
    CODE: NFSOBJLAYOUTRET3
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    ops = use_obj(env.opts.path) + [op.layoutreturn(False,
                                                    LAYOUT4_OBJECTS_V2,
                                                    LAYOUTIOMODE4_ANY,
                                                    layoutreturn4(LAYOUTRETURN4_ALL))]
    res = sess.compound(ops)
    check(res)


def testNfsObjLayoutReturnFile2(t, env):
    """
    Return a file's layout with return data

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRET1a
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False,
                           LAYOUT4_OBJECTS_V2,
                           LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res)


def testNFSOBJLayoutReturnSEQID(t, env):
    '''Validate that server increment seqid also for layoutreturn

    (RFC 5661 12.5.3: After the layout stateid is established,
    the server increments by one the value if the "seqid" in each
    subsequent LAYOUTGET and LAYOUTRETURN)

    AUTHOR: Avi

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRET4
    '''
    sess = env.c1.new_client_session(env.testname(t),
                                    flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Get layout 2
    lo_stateid1 = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        8192, 8192, 8192, lo_stateid1, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid2 = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
      pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False,
                           LAYOUT4_OBJECTS_V2,
                           LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(8192,
                                                            8192,
                                                            lo_stateid2,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res)

    ops = [op.putfh(fh),
           op.layoutreturn(False,
                           LAYOUT4_OBJECTS_V2,
                           LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            lo_stateid1,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res)

    # validate that server incremented seqid by one
    # (two for two layoutreturns) for layout return
    if res.resarray[-1].lrs_stateid.seqid != lo_stateid2.seqid + 2:
        fail('server does not increment seqid with layoutreturn')


def testNfsObjLayoutDoubleReturn(t, env):
    """Verify layout return - double-return

    AUTHOR: Nadav

    FLAGS: nfs-obj2
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRETD1
    """
    sess = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res)

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]

    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testNfsObjLayoutFalseReturn(t, env):
    """Verify layout return - false return

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRETF1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            open_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testNfsObjLayoutCommitAfterReturn(t, env):
    """Verify layout return - commit-after-return

    AUTHOR: Nadav

    FLAGS: nfs-obj2
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRETD2
    """
    sess = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    layout_stateid = res.resarray[-1].logr_stateid

    # Return layout
    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res)

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh),
           op.layoutcommit(0, 2 * 8192, False, layout_stateid,
                           newoffset4(True, (2 * 8192) - 1), time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]
    res = sess.compound(ops)
    check(res, NFS4ERR_BADLAYOUT)

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testNfsObjLayoutReturnOtherClient(t, env):
    """Verify layout return - return the layout from a different client

    AUTHOR: Nadav

    FLAGS: nfs-obj2
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRETD3
    """
    sess = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    layout_stateid = res.resarray[-1].logr_stateid

    # Return layout
    c2 = env.c1.new_client("2_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess2 = c2.create_session()
    res = sess2.compound([op.reclaim_complete(FALSE)])
    check(res)

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testLayoutStateid1(t, env):
    """Check for proper sequence handling in layout stateids.

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFS-OBJ1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                        flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid = res.resarray[-1].logr_stateid

    if lo_stateid.seqid != 1:
        # From draft23 12.5.2 "The first successful LAYOUTGET processed by
        # the server using a non-layout stateid as an argument MUST have the
        # "seqid" field of the layout stateid in the response set to one."
        fail("Expected stateid.seqid==1, got %i" % lo_stateid.seqid)

    for i in range(6):
        # Get subsequent layouts
        ops = [op.putfh(fh),
               op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                            (i + 1) * 8192, 8192, 8192, lo_stateid, 0xffff)]
        res = sess.compound(ops)
        check(res)

        lo_stateid = res.resarray[-1].logr_stateid

        if lo_stateid.seqid != i + 2:
            # From draft23 12.5.3 "After the layout stateid is established,
            # the server increments by one the value of the "seqid" in each
            # subsequent LAYOUTGET and LAYOUTRETURN response,
            fail("Expected stateid.seqid==%i, got %i" % (i + 2,
                 lo_stateid.seqid))


def testLayoutStateid2(t, env):
    """Check for proper sequence handling in layout stateids.

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFS-OBJ2
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Get layout 2
    lo_stateid1 = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        8192, 8192, 8192, lo_stateid1, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Get layout 3 (merge of prior two)
    lo_stateid2 = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 2 * 8192, 2 * 8192, lo_stateid2, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid3 = res.resarray[-1].logr_stateid
    layout = res.resarray[-1].logr_layout[-1]

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid3,
                           newoffset4(True, 2 * 8192 - 1), time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]
    res = sess.compound(ops)
    check(res)


def testLayoutStateid3(t, env):
    '''Validate illegal stateid after layoutreturn

    (RFC5661 12.5.3: Once a client has no more layouts on a file, the
    lauot stateid is no longer valid and MUST NOT be used. Any attempt
    to use such a layout atateid will result in NFS4ERR_BAD_STATEID)

    AUTHOR: Avi

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJSTATEID3
    '''
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            "")))]
    res = sess.compound(ops)
    check(res)

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, layout_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

def testNfsObjLayoutCommit(t, env):
    """
    Do some commits

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTCOMMIT1
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Test that fs handles nfs-obj layouts
    ops = use_obj(env.opts.path) + [op.getattr(1 << FATTR4_LAYOUT_BLKSIZE)]
    res = sess.compound(ops)
    check(res)

    try:
        blocksize = res.resarray[-1].obj_attributes[FATTR4_LAYOUT_BLKSIZE]
    except KeyError:
        fail("layout_blksize not available")

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 4 * blocksize, 4 * blocksize, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    layout_stateid = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    notime = newtime4(False)

    ops = [op.putfh(fh),
           op.layoutcommit(0,  # loca_offset
                           0,  # loca_length
                           False,  # loca_reclaim
                           layout_stateid,  # loca_stateid
                           newoffset4(True, 0),  # loca_last_write_offset
                           notime,  # loca_time_modify
                           layoutupdate4(# loca_layoutupdate
                                         LAYOUT4_OBJECTS_V2,  # lou_type
                                         p.get_buffer()))]  # lou_body
    res = sess.compound(ops)
    check(res)


def testEmptyCommit(t, env):
    """Check for proper handling of empty LAYOUTCOMMIT.

    AUTHOR: Nadav

    FLAGS: nfs-obj-broken
    DEPEND: NFSOBJ
    CODE: NFS-OBJ3
    """
    sess = env.c1.new_client_session(env.testname(t),
                                    flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Get layout 2
    lo_stateid1 = res.resarray[-1].logr_stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        8192, 8192, 8192, lo_stateid1, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid2 = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid2,
                           newoffset4(True, 2 * 8192 - 1), time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]
    res = sess.compound(ops)
    check(res)

    # Send another LAYOUTCOMMIT, with an empty opaque
    time = newtime4(True, get_nfstime())
    ops = [op.putfh(fh),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid2,
                           newoffset4(True, 2 * 8192 - 1), time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2, ""))]
    res = sess.compound(ops)
    check(res)


def testSplitCommit(t, env):
    """Check for proper handling of disjoint LAYOUTCOMMIT.opaque

    AUTHOR: Nadav

    FLAGS: nfs-obj-broken
    DEPEND: NFSOBJ
    CODE: NFS-OBJ4
    """
    sess = env.c1.new_client_session(env.testname(t),
                                        flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 2 * 8192, 2 * 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid1 = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh),
           op.layoutcommit(8192,
                           8192,
                           False, lo_stateid1,
                           newoffset4(True, 2 * 8192 - 1),
                           time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2,
                                         p.get_buffer()))]
    res = sess.compound(ops)
    check(res)

    ops = [op.putfh(fh),
           op.layoutcommit(0,
                           8192,
                           False, lo_stateid1,
                           newoffset4(True, 2 * 8192 - 1),
                           time,
                           layoutupdate4(LAYOUT4_OBJECTS_V2,
                                         p.get_buffer()))]
    res = sess.compound(ops)
    check(res)


def testNfsObjLayoutRecall1(t, env):
    """Verify layout recall
       Server should recall layouts for a file upon receiving an IO error

    AUTHOR: Nadav

    FLAGS: nfs-obj-recall
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRECALL1
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    # c1 - create a file and open for RW
    c1 = env.c1.new_client("%s_1" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess1 = c1.create_session()
    res = sess1.compound([op.reclaim_complete(FALSE)])
    check(res)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layouts1 = res.resarray[-1].logr_layout
    layout_stateid1 = res.resarray[-1].logr_stateid

    # New client also opens the file and gets a layout
    c2 = env.c1.new_client("%s_2" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess2 = c2.create_session()
    res = sess2.compound([op.reclaim_complete(FALSE)])
    check(res)

    owner2 = "Open Owner 2"
    res = open_file(sess2, env.testname(t))
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layouts2 = res.resarray[-1].logr_layout
    layout_stateid2 = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # Client 1 returns layout with IO error
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, PNFS_OBJ_ERR_EIO)))

    ops = [op.putfh(fh1),
           op.layoutreturn(False,
                           LAYOUT4_OBJECTS_V2,
                           LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid1,
                                                            p.get_buffer())))]

    c2.cb_pre_hook(OP_CB_LAYOUTRECALL, pre_hook)
    c2.cb_post_hook(OP_CB_LAYOUTRECALL, post_hook)

    recall.happened = False
    slot1 = sess1.compound_async(ops)
    recall.wait(1)

    if not recall.happened:
        res = sess1.listen(slot1)
        check(res)

        env.sleep(1)
        if recall.happened:
            fail("Recall happened late")

        ops = [op.putfh(fh2),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4(0,
                                                                0xffffffffffffffff,
                                                                layout_stateid2,
                                                                "")))]
        res = sess2.compound(ops)
        check(res)

        fail("Did not get recall callback")

    # Getting here means CB_LAYOUTRECALL reply is in the send queue.
    # Give it a moment to actually be sent
    env.sleep(1)
    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid2,
                                                            "")))]
    res = sess2.compound(ops)
    check(res)


def testNfsObjLayoutRecall2(t, env):
    """Verify layout recall
       Server should recall layouts for a file upon recieving an IO error (layoutcommit)

    AUTHOR: Nadav

    FLAGS: nfs-obj-recall
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRECALL2
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    # c1 - create a file and open for RW
    c1 = env.c1.new_client("%s_1" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess1 = c1.create_session()
    res = sess1.compound([op.reclaim_complete(FALSE)])
    check(res)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layouts1 = res.resarray[-1].logr_layout
    layout_stateid1 = res.resarray[-1].logr_stateid

    # New client also opens the file and gets a layout
    c2 = env.c1.new_client("%s_2" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess2 = c2.create_session()
    res = sess2.compound([op.reclaim_complete(FALSE)])
    check(res)

    owner2 = "Open Owner 2"
    res = open_file(sess2, env.testname(t))
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layouts2 = res.resarray[-1].logr_layout
    layout_stateid2 = res.resarray[-1].logr_stateid

    # Client 1 updates the layout with IO error
    p = ObjV2Packer()
    # error flag set to true
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), True))

    ops = [op.putfh(fh1),
           op.layoutcommit(0, 0xffffffffffffffff, False, layout_stateid1,
                           newoffset4(True, 0), newtime4(True, get_nfstime()),
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    c2.cb_pre_hook(OP_CB_LAYOUTRECALL, pre_hook)
    c2.cb_post_hook(OP_CB_LAYOUTRECALL, post_hook)

    recall.happened = False
    slot1 = sess1.compound_async(ops)
    recall.wait(1)

    if not recall.happened:
        res = sess1.listen(slot1)
        check(res)

        env.sleep(1)
        if recall.happened:
            fail("Recall happened late")

        ops = [op.putfh(fh2),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4(0,
                                                                0xffffffffffffffff,
                                                                layout_stateid2,
                                                                "")))]
        res = sess2.compound(ops)
        check(res)

        fail("Did not get recall callback")

    # Getting here means CB_LAYOUTRECALL reply is in the send queue.
    # Give it a moment to actually be sent
    env.sleep(1)
    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid2,
                                                            "")))]
    res = sess2.compound(ops)
    check(res)


def testNfsObjLayoutRecallBadClient(t, env):
    """Verify layout recall of bad clients
       Server should recall layouts for a client that returns a layout subset

   If none are found and the byte range is a subset of an outstanding
   layout segment with for the same clientid and iomode,
   then the client can be considered malfunctioning and the server SHOULD
   recall all layouts from this client to reset its state.
   If this behavior repeats, the server SHOULD deny all LAYOUTGETs from this client.

    AUTHOR: Nadav

    FLAGS: nfs-obj-recall
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRECALL3
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    c1 = env.c1.new_client(env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess = c1.create_session()
    res = sess.compound([op.reclaim_complete(FALSE)])
    check(res)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    # Return layout
    layout_stateid = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 4096, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(4096,
                                                            8192,
                                                            layout_stateid,
                                                            p.get_buffer())))]

    c1.cb_pre_hook(OP_CB_LAYOUTRECALL, pre_hook)
    c1.cb_post_hook(OP_CB_LAYOUTRECALL, post_hook)

    recall.happened = False
    slot1 = sess.compound_async(ops)
    recall.wait(1)

    if not recall.happened:
        res = sess.listen(slot1)
        check(res)

        env.sleep(1)
        if recall.happened:
            fail("Recall happened late")

        ops = [op.putfh(fh),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4(0,
                                                                0xffffffffffffffff,
                                                                layout_stateid,
                                                                "")))]
        res = sess.compound(ops)
        check(res)

        fail("Did not get recall callback")

    # Getting here means CB_LAYOUTRECALL reply is in the send queue.
    # Give it a moment to actually be sent
    env.sleep(1)
    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid,
                                                            "")))]
    res = sess.compound(ops)
    check(res)


def testNfsObjLayoutRecallChmod(t, env):
    """Verify layout recall of outstanding access privileges
       Server should recall layouts for an outstanding IO access (when file IO privileges change)
       (i.e. a read-write layout for a file that's being turned read-only)

    AUTHOR: Nadav

    FLAGS: nfs-obj-recall
    DEPEND: NFSOBJ
    CODE: NFSOBJLAYOUTRECALL4
    """
    recall = threading.Event()

    def pre_hook(arg, env):
        recall.stateid = arg.stateid # NOTE this must be done before set()
        recall.happened = True
        env.notify = recall.set # This is called after compound sent to queue

    def post_hook(arg, env, res):
        return res

    c1 = env.c1.new_client("1_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    c2 = env.c1.new_client("2_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    sess1 = c1.create_session()
    res = sess1.compound([op.reclaim_complete(FALSE)])
    check(res)

    sess2 = c2.create_session()
    res = sess2.compound([op.reclaim_complete(FALSE)])
    check(res)

    # Create the file
    res = create_file(sess1, env.testname(t))
    check(res)

    fh = res.resarray[-1].object
    stateid = res.resarray[-2].stateid

    # Open the file
    res = open_file(sess2, env.testname(t), access=OPEN4_SHARE_ACCESS_BOTH)
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid

    # Get layout
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 0xffffffffffffffff, 0, stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layout_stateid2 = res.resarray[-1].logr_stateid

    # Write data
    stateid.seqid = 0
    res = sess1.compound([op.putfh(fh),
                          op.write(stateid, 5, FILE_SYNC4, "write test data")])
    check(res)

    # Turn file read-only
    c2.cb_pre_hook(OP_CB_LAYOUTRECALL, pre_hook)
    c2.cb_post_hook(OP_CB_LAYOUTRECALL, post_hook)

    recall.happened = False
    slot1 = sess1.compound_async([op.putfh(fh),
                                  op.setattr(stateid, {FATTR4_MODE:MODE4_RUSR})])

    recall.wait(1)

    if not recall.happened:
        res = sess1.listen(slot1)
        check(res)

        stateid2.seqid = 0
        res = sess2.compound([op.putfh(fh2),
                              op.write(stateid2, 5, FILE_SYNC4, "write invalid data")])
        check(res)

        #check(res, NFS4ERR_ACCESS)
        res = sess1.compound([op.putfh(fh),
                              op.read(stateid2, 0, 1000)])
        check(res)

        if not res.resarray[-1].eof:
            fail("EOF not set on read")

        desired = "\0" * 5 + "write test data"
        invalid = "\0" * 5 + "write invalid data"

        if res.resarray[-1].data == invalid:
            fail("Wrong data - data written past SETATTR")
        else:
            if res.resarray[-1].data != desired:
                fail("Expected %r, got %r" % (desired, res.resarray[-1].data))

        env.sleep(5)

        if recall.happened:
            fail("Recall happened late")

        ops = [op.putfh(fh2),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4(0,
                                                                0xffffffffffffffff,
                                                                layout_stateid2,
                                                                "")))]
        res = sess2.compound(ops)
        check(res)

        fail("Did not get recall callback")

    # Getting here means CB_LAYOUTRECALL reply is in the send queue.
    # Give it a moment to actually be sent
    env.sleep(1)
    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            0xffffffffffffffff,
                                                            layout_stateid2,
                                                            "")))]
    res = sess2.compound(ops)
    check(res)

    # Now get SETATTR reply
    res = sess1.listen(slot1)
    checklist(res, [NFS4_OK, NFS4ERR_DELAY])

    if res.status == NFS4ERR_DELAY:
        env.sleep(1)
        res = sess1.compound([op.putfh(fh),
                              op.setattr(stateid, {FATTR4_MODE:MODE4_RUSR})])
        check(res)


def testReturnStateid1(t, env):
    """Check for proper sequence handling in layout stateids return.

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFS-OBJ5
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid = res.resarray[-1].logr_stateid

    for i in range(6):
        # Get subsequent layouts
        ops = [op.putfh(fh),
               op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                            (i + 1) * 8192, 8192, 8192, lo_stateid, 0xffff)]
        res = sess.compound(ops)
        check(res)

        lo_stateid = res.resarray[-1].logr_stateid

    for i in range(7):
        lo_stateid.seqid = i

        p = ObjV2Packer()
        # component, offset, length, is_write, errno
        p.pack_pnfs_obj_layoutreturn4(
            pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

        ops = [op.putfh(fh),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4(i * 8192,
                                                                8192,
                                                                lo_stateid,
                                                                p.get_buffer())))]
        res = sess.compound(ops)
        check(res)


def testReturnStateid2(t, env):
    """Check for proper sequence handling in layout stateids return.

    AUTHOR: Nadav

    FLAGS: nfs-obj
    DEPEND: NFSOBJ
    CODE: NFS-OBJ6
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid = res.resarray[-1].logr_stateid
    # invalid increasing layout seqid
    lo_stateid.seqid += 1

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    ops = [op.putfh(fh),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            lo_stateid,
                                                            p.get_buffer())))]
    res = sess.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)


def testReturnStateid3(t, env):
    """Check for proper sequence handling in layout stateids return.

    AUTHOR: Nadav

    FLAGS: nfs-obj2
    DEPEND: NFSOBJ
    CODE: NFS-OBJ7
    """
    sess = env.c1.new_client_session(env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    # Create the file
    res = create_file(sess, env.testname(t))
    check(res)

    # Get layout 1
    fh = res.resarray[-1].object
    open_stateid = res.resarray[-2].stateid

    ops = [op.putfh(fh),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                        0, 8192, 8192, open_stateid, 0xffff)]
    res = sess.compound(ops)
    check(res)

    lo_stateid = res.resarray[-1].logr_stateid
    start_stateid = lo_stateid

    for i in range(6):
        # Get subsequent layouts
        ops = [op.putfh(fh),
               op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
                            (i + 1) * 8192, 8192, 8192, lo_stateid, 0xffff)]
        res = sess.compound(ops)
        check(res)

        lo_stateid = res.resarray[-1].logr_stateid

    p = ObjV2Packer()
    # component, offset, length, is_write, errno
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    for i in range(7):
        ops = [op.putfh(fh),
               op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                               layoutreturn4(LAYOUTRETURN4_FILE,
                                             layoutreturn_file4((7 - i) * 8192,
                                                                8192,
                                                                start_stateid,
                                                                p.get_buffer())))]
        res = sess.compound(ops)
        check(res, NFS4ERR_BAD_STATEID)

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

 
def testROC1(t, env):
    """Check basic multiple clients

    FLAGS: nfs-obj
    CODE: ROC1
    """
    #print "Client 1, sess1 - create file"
    sess1 = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid1 = res.resarray[-1].logr_stateid

    res = open_file(sess1, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)

    #print "Client 1, sess2 - only open created file"
    sess2 = env.c1.new_client_session("2_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess2, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layout_stateid2 = res.resarray[-1].logr_stateid

    #print "Client 1, session3 - only open created file"

    sess3 = env.c1.new_client_session("3_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess3, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh3 = res.resarray[-1].object
    open_stateid3 = res.resarray[-2].stateid

    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    layout_stateid3 = res.resarray[-1].logr_stateid

    #print "Client 4 - only open created file"

    sess4 = env.c1.new_client_session("4_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess4, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh4 = res.resarray[-1].object
    open_stateid4 = res.resarray[-2].stateid

    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    layout_stateid4 = res.resarray[-1].logr_stateid

    res = close_file(sess4, fh4, open_stateid4)
    print "closing Client 1", res
    check(res)


    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutreturn4(
        pnfs_obj_layoutreturn4(pnfs_obj_ioerr4("", 0, 0, True, 0)))

    env.sleep(2)

    #print "***** #   LAYOUTRETURN from client4 "
    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh1),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res)

def testROC2(t, env):
    """Check basic multiple clients

    FLAGS: nfs-obj
    CODE: ROC2
    """
    #print "Client 1 - create file"
    sess1 = env.c1.new_client_session("1_%s" % env.testname(t),
                                     flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = create_file(sess1, env.testname(t))
    check(res)

    fh1 = res.resarray[-1].object
    open_stateid1 = res.resarray[-2].stateid

    ops = [op.putfh(fh1),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid1, 0xffff)]
    res = sess1.compound(ops)
    check(res)

    layout_stateid1 = res.resarray[-1].logr_stateid

    #print "Client 2 - only open created file"
    sess2 = env.c2.new_client_session("2_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess2, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    check(res)

    fh2 = res.resarray[-1].object
    open_stateid2 = res.resarray[-2].stateid

    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid2, 0xffff)]
    res = sess2.compound(ops)
    check(res)

    layout_stateid2 = res.resarray[-1].logr_stateid

    #print "Client 3 - only open created file"

    sess3 = env.c3.new_client_session("3_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess3, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)

    check(res)

    fh3 = res.resarray[-1].object
    open_stateid3 = res.resarray[-2].stateid

    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    layout_stateid3 = res.resarray[-1].logr_stateid

    #print "Client 4 - only open created file"

    sess4 = env.c4.new_client_session("4_%s" % env.testname(t),
                           flags=EXCHGID4_FLAG_USE_PNFS_MDS)

    res = open_file(sess4, env.testname(t), access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    check(res)

    fh4 = res.resarray[-1].object
    open_stateid4 = res.resarray[-2].stateid

    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, open_stateid4, 0xffff)]
    res = sess4.compound(ops)
    check(res)

    layout_stateid4 = res.resarray[-1].logr_stateid

    #print "closing Client 1- 4"

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

    #print "***** #   LAYOUTRETURN from client4 "
    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid3,
                                                            p.get_buffer())))]
    res = sess3.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh2),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid2,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

    ops = [op.putfh(fh4),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0,
                                                            8192,
                                                            layout_stateid1,
                                                            p.get_buffer())))]
    res = sess2.compound(ops)
    check(res, NFS4ERR_BAD_STATEID)

def testROC3(t, env):
    """Multiopen for read, write, close from same client.
       Open_r&layoutget-->Open_w-->Delegreturn-->layoutget_rw-->close_r-->write-->close-->layoutreturn

    FLAGS: nfs-obj
    CODE: ROC3
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
                           LAYOUT4_OBJECTS_V2,
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


def testROC4(t, env):
    """Multiopen for read, write from one client and open for read from others client simultaneously.
      (OPen_w&layouget-->write-->Open_r&layoutget)C2--> (Open_r&layoutget_r)C3-->closeC2-->closeC3-->layoutupdate,commit,return C2

    FLAGS: nfs-obj
    CODE: ROC4
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
                        0, 0xffffffffffffffff, 0, stateid3, 0xffff)]
    res = sess3.compound(ops)
    check(res)

    #print "***** Return read layout  *********"
    layout_stateid = res.resarray[-1].logr_stateid
    roc = res.resarray[-1].logr_return_on_close
    res = sess3.compound([op.putfh(fh3),
                          op.read(stateid3, 0, 1000)])
    print "***** sess3 ***", res

    #print "***** Create - Client3 *************************** "
    sessionName = env.testname(t) + "4"
    sess4 = env.c3.new_client_session(sessionName, flags=EXCHGID4_FLAG_USE_PNFS_MDS)
    sess4.client.cb_pre_hook(OP_CB_RECALL, pre_hook)
    sess4.client.cb_post_hook(OP_CB_RECALL, post_hook)

    #print "***** Open same fail for Read client3*************************** "
    claim = open_claim4(CLAIM_NULL, fileName)
    owner = open_owner4(0, fileOwner)
    how = openflag4(OPEN4_NOCREATE)
    res = open_file(sess3, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
    deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_NONE, want_deleg=False)
    #print "***********res from openClient4Read=", res
    check(res)

    fh4 = res.resarray[-1].object
    stateid4 = res.resarray[-2].stateid
    ops = [op.putfh(fh4),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
                           layoutreturn4(LAYOUTRETURN4_FILE,
                                         layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid, p.get_buffer())))]
    res = sess2.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


def testROC5(t, env):
    """Multiopen for read, write, close from same client.
       Open_r&layoutget-->Open_w-->Delegreturn-->layoutget_rw-->close_r-->write-->close-->layoutupdate,commit,return

    FLAGS: nfs-obj
    CODE: ROC5
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

    print "***** openClient2Read: Opening file '%s', for READ  with owner '%s', with session name '%s'" % (filePath, fileOwner, sessionName)
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_READ,
        deny=OPEN4_SHARE_DENY_NONE, deleg_type=OPEN_DELEGATE_READ, want_deleg=True)
    print "***********res from openClient2Read=", res
    check(res)

    fh2 = res.resarray[-1].object
    stateid2 = res.resarray[-2].stateid
    ops = [op.putfh(fh2),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
                           layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
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


def testROC6(t, env):
    """Create a file1, open_r it from client2, open_w from client2, expected result is nfs4err_delay until file closed from  .

    FLAGS: nfs-obj
    CODE: ROC6
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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

    print "Open the file from client2Write"
    res = open_file(sess2, fileOwner, filePath, access=OPEN4_SHARE_ACCESS_WRITE,
                    deleg_type=OPEN_DELEGATE_WRITE, want_deleg=True)
    check(res)

    fh3 = res.resarray[-1].object
    stateid3 = res.resarray[-2].stateid
    ops = [op.putfh(fh3),
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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

    ops = [op.putfh(fh3),
           op.layoutcommit(0, 2 * 8192, False, layout_stateid4, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess2.compound(ops)
    check(res)

    res = close_file(sess2, fh3, stateid=stateid3)
    check(res)

    res = close_file(sess3, fh4, stateid=stateid4)
    check(res)

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY,
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


def testROC7(t, env):

    """Multiopen to read four clients, open_write and write from one.
              Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_w.

    FLAGS: nfs-obj
    CODE: ROC7
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess4.compound(ops)

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4_OK
    else:
        state = NFS4ERR_BAD_STATEID

    check(res, state)

def testROC8(t, env):

    """Multiopen to read 4 clients, open_write and write from one+ close_w session.
        Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_w.

    FLAGS: nfs-obj
    CODE: ROC8
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)


def testROC9(t, env):

    """Multiopen to read four clients, open_write and write from one + close _r session.
        Clients C1,2,3 --> opens_r, client4 open_r, open_w, delegreturn C4--> C1,2,3,4_r. close C1,2,3,4_w layoutreturn_ok_for read,layoutreturn_bad_session -> for C1,2,3,4_r
                                                                --> write_C4_w, commit, layoureturn_OK, close,layoureturn_BAD_SESSION.

    FLAGS: nfs-obj
    CODE: ROC9
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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

    #print "*****  layout after closed file ***********"
    p = ObjV2Packer()
    p.pack_pnfs_obj_layoutupdate4(
        pnfs_obj_layoutupdate4(pnfs_obj_deltaspaceused4(True, 0), False))

    time = newtime4(True, get_nfstime())

    ops = [op.putfh(fh3),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            layout_stateid4,
                                                            p.get_buffer())))]
    res = sess1.compound(ops)

    if roc:
        state = NFS4_OK
    else:
        state = NFS4ERR_BAD_STATEID

    check(res, state)

    data = "sess4 with data for write....write test data from client4  secound loop"
    res = sess4.compound([op.putfh(fh5),
                          op.write(stateid5, 5, FILE_SYNC4, data)])
    check(res)
    res = sess4.compound([op.putfh(fh5),
                          op.read(stateid5, 0, 1000)])
    check(res)

    #print "*****  layout update / commit after closed file ***********"
    ops = [op.putfh(fh5),
           op.layoutcommit(0, 2 * 8192, False, lo_stateid5, newoffset4(True, 2 * 8192 - 1),
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess4.compound(ops)

    #print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

    #print "***** Return Layot *****************"

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
                                                                                       layoutreturn_file4(0, 0xffffffffffffffff,
                                                            lo_stateid5,
                                                            p.get_buffer())))]
    res = sess4.compound(ops)

    if roc:
        state = NFS4ERR_BAD_STATEID
    else:
        state = NFS4_OK

    check(res, state)



def testROC10(t, env):

    """Multiopen to read four clients, open_write and write from two.
                   Clients C1,2,3 --> opens_r, clientC4 open_r, C4(open_w, delegreturn C4--> C1,2,3,4_r),C3(open_w, delegreturn C3--> C1,2,3,4_r). close all, layoutreturn_bad_session.

    FLAGS: nfs-obj
    CODE: ROC10
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_READ,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
           op.layoutget(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_RW,
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
                           time, layoutupdate4(LAYOUT4_OBJECTS_V2, p.get_buffer()))]

    res = sess4.compound(ops)

 #   print "close file client4Write"
    res = close_file(sess4, fh5, stateid5)
    check(res)

  #  print "***** Return Layot "

    ops = [op.putfh(fh5),
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
           op.layoutreturn(False, LAYOUT4_OBJECTS_V2, LAYOUTIOMODE4_ANY, layoutreturn4(LAYOUTRETURN4_FILE,
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
