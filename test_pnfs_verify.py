from tonat.test_base import TestBase
from tonat.libtests import attr
from tonat.product.objects.mount import Mount, NfsVersion
from tonat.context import ctx
from os.path import join
from pd_tests import test_ha


class Test_pnfs(TestBase):

    @attr(jira='PD-5396', name='pnfs_verify')
    def test_pnfs_verify(self, get_pd_share):
        mnt_tmpl = Mount(get_pd_share, NfsVersion.pnfs) # can change path=''
        mounts = ctx.clients.nfs.mount(mnt_tmpl)
        for (client, path) in zip(ctx.clients, mounts.path):
            fname = "verify-pnfs-" + client.address
            #client.execute(["echo", "blah", ">", join(path, fname)])

            nfsstat_file = join("/tmp", "nfsstat.out")
            client.execute(["nfsstat", "-Z", "1", "-l", ">", nfsstat_file, "&", "KILLPID=$!", "&&", "echo", "blah", ">", join(path, fname),\
                         "&&", "sync", "&&", "kill", "-2", "$KILLPID" ])
            res = client.execute(['cat', nfsstat_file])

            nfsinfo = dict()
            for line in res.stdout.splitlines():
                if not line.startswith('nfs'):
                    continue
                _, version, _, opt, count = line.split()

                try:
                    nfsinfo[version].update([(opt.strip()[:-1], int(count.strip()))])
                except KeyError:
                    nfsinfo[version] = {opt.strip()[:-1]: int(count.strip())}

            try:
                if nfsinfo['v3']['write'] < 1:
                    raise KeyError
            except KeyError:
                raise Exception('Test Failed - inband IO {}'.format(nfsinfo))
