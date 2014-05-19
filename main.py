#!/usr/bin/python
# -*- coding: utf-8 -*-
import errno
import fuse
import stat
import time
import logging


fuse.fuse_python_api = (0, 2)
logger = logging.getLogger(__name__)

class Coda(fuse.Fuse):
    def __init__(self, *args, **kw):
        fuse.Fuse.__init__(self, *args, **kw)

    def getattr(self, path):
        st = fuse.Stat()
        st.st_mode = stat.S_IFDIR | 0755
        st.st_nlink = 2
        st.st_atime = int(time.time())
        st.st_mtime = st.st_atime
        st.st_ctime = st.st_atime
	logger.debug('Getting file attrs')

	if path[1:] == "some_dir" or path == '/':
            st.st_mode = stat.S_IFDIR | 0755
        elif path[1:] == "some_file":
            st.st_mode = stat.S_IFREG | 0644
        #if path == '/':
         #   pass
        else:
            return - errno.ENOENT
        return st

    def readdir(self, path, offset):
        for e in '.', '..', 'some_dir', 'some_file':
            yield fuse.Direntry(e) 

if __name__ == '__main__':
    fs = Coda()
    fs.parse(errex=1)
    fs.main()

