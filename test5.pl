 @attr(name="group_777_666_two_users")   # pcap from dd - check that possible write-read to same directory from difference users with difference permissions, A_SYNC.    
    def test_group_777_666_two_users(self, conf_3u_2g):
        directory = 'user_dir'
        print 'mounts', self.mounts
        for mount in self.mounts:
            print 'path from mount', mount.path
        path = "{}/{}".format(self.mounts[0].path,directory)
        print "print", path
        users = []
        users = self.udb.get_users(self.groups[0])
        ctx.clients[0].execute(['mkdir', '-p',path], user=users[0])
        ctx.clients[0].chmod(path, 0777)
        fname = path + '/user_file'
        res = ctx.clients[0].execute(['id'], user=users[0])
        self._logger.info(res.stdout)
        self._logger.debug("Starting PCAPs")
        pcaps=PE(ctx.cluster.get_active_dd(), tmp_dir='/opt')       
        fd0 = ctx.clients[0].open_file(fname, os.O_CREAT | os.O_RDWR , 0777, users[0])
 # write in thr loop
        fv = self._write_loop(ctx.clients[0], fd0, 0, 32, 4, users[0], block=False)
        
        ctx.clients[0].read_file(fd0, 0, 32, 4, users[0])
        
#  second user from second group will change file-permissions to owner in the writing time. 
        path2 = "{}/{}".format(self.mounts[1].path,directory)
        print "path2", path2      
        users2 = []        
        users2 = self.udb.get_users(self.groups[1])
        ctx.clients[1].execute(['chmod', '-R', '444', path2])
        fname2 = '{}/{}'.format(path2, 'user_file')
        print "file_name", fname2
        fd1 = ctx.clients[1].open_file(fname2, os.O_CREAT | os.O_RDWR | os.O_ASYNC , 0777, users2[1])
        ctx.clients[1].write_file(fd1, 32, 64, 4, users2[1])      
        time.sleep(1)
        fv.kill()
        fv.get()

        ctx.clients[0].close_file(fd0, users[0])
        ctx.clients[1].close_file(fd1, users2[1])
        self._logger.info("AFTER CLOSE") 

        results = pcaps.get_nfs_ops(False)
        ctx.clients[0].close_agent()
        ctx.clients[1].close_agent()                    
                    
                    
