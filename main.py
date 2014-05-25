#!/usr/bin/python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from fuse import FUSE, FuseOSError, Operations
import coda_remote.venus
import sqlite3

if not hasattr(__builtins__, 'bytes'):
	bytes = str

class FS(Operations):
	"""Creates an in-memory filesystem using FUSE,
	self.files stores the file information (similar to inode),
	self.data stores filedata, self.fd is the filedescriptor"""

   	def __init__(self):
		"""Database to store file information"""
		self.conn = sqlite3.connect('files.db')
		self.op = self.conn.cursor()
        	self.files = {}
        	self.data = defaultdict(bytes)
        	self.fd = 0
		try:
			self.op.execute(''' CREATE TABLE inodes (
				path		PRIMARY KEY, 
				st_mode		INT NOT NULL,
				st_ctime	INT NOT NULL,
				st_mtime 	INT NOT NULL,
				st_atime 	INT NOT NULL,
				st_nlink	INT NOT NULL,
				f_dir		BLOB,
				data		BLOB
			) ''' )	
		except sqlite3.OperationalError:
			pass	

        	now = time()
		st_mode =  (S_IFDIR | 0755) 
		st_mtime = st_atime = st_ctime = now
		st_nlink = 2
		f_dir = list()
        	self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime = now,
                               st_mtime = now, st_atime = now, st_nlink = 2, f_dir = list())

    	def chmod(self, path, mode):
        	self.files[path]['st_mode'] &= 0770000
        	self.files[path]['st_mode'] |= mode
        	return 0

    	def chown(self, path, uid, gid):
        	self.files[path]['st_uid'] = uid
        	self.files[path]['st_gid'] = gid

    	def create(self, path, mode):
		"""Create a file at the specified path with the specified mode,
		mode is by default S_IFREG which is the linux equivalent
		of a regular file ORed with the mode specified"""
        	self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

		#Not root directory
		if len(path.split('/')) != 2:
			self.files[path[:path.rfind('/')]]['f_dir'].append(path)
		else:
		#Root directory
			self.files['/']['f_dir'].append(path)

        	self.fd += 1
        	return self.fd

    	def getattr(self, path, fh = None):
		"""If file not present, return error"""
        	if path not in self.files:
            		raise FuseOSError(ENOENT)

        	return self.files[path]

    	def getxattr(self, path, name, position = 0):
        	attrs = self.files[path].get('attrs', {})

        	try:
            		return attrs[name]
        	except KeyError:
			#Should return ENOATTR but has an error, need to fix
            		return ''

    	def listxattr(self, path):
        	attrs = self.files[path].get('attrs', {})
        	return attrs.keys()

    	def mkdir(self, path, mode):
		"""Create a new directory with the default linux flag
		for a directory, S_IFDIR ORed with any additional modes
		specified by the application"""
		print "Making directory ", path, mode
        	self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), f_dir = [])

		#Not root directory
		if len(path.split('/')) != 2:
			self.files[path[:path.rfind('/')]]['f_dir'].append(path)
		else:
		#Root directory
			self.files['/']['f_dir'].append(path)

        	self.files['/']['st_nlink'] += 1

    	def open(self, path, flags):
        	self.fd += 1
		print "Opening"
        	return self.fd

    	def read(self, path, size, offset, fh):
		print "Reading"
        	return self.data[path][offset:offset + size]

    	def readdir(self, path, fh):
		"""Reads directory as specified by path, filehandler is empty for now,
		gotta fix multi-level directories - How does this work?
		A dir. inside a dir., /hello/dude starts with the prefix /hello/ which
		only to those of path + 1"""
        	return ['.', '..'] + [x[x.rfind('/') + 1:] for x in self.files[path]['f_dir']]

    	def readlink(self, path):
		"""Only reads the link and returns data"""
		print "Reading link"
        	return self.data[path]

    	def removexattr(self, path, name):
        	attrs = self.files[path].get('attrs', {})

        	try:
            		del attrs[name]
        	except KeyError:
            		pass        # Should return ENOATTR

    	def rename(self, old, new):
		"""Remove old filename and insert new one"""
    		self.files[new] = self.files.pop(old)

		if len(old.split('/')) != 2:
			self.files[new[:new.rfind('/')]]['f_dir'].remove(old)
			self.files[new[:new.rfind('/')]]['f_dir'].append(new)
		else:
			self.files['/']['f_dir'].remove(old)
			self.files['/']['f_dir'].append(new)

    	def rmdir(self, path):
		"""Remove directory by popping old filepath
		and removing file counter, have to implement
		recursive removal rm -rf"""
        	self.files.pop(path)

		if len(old.split('/')) != 2:
			self.files[new[:new.rfind('/')]]['f_dir'].remove(old)
		else:
			self.files['/']['f_dir'].remove(old)

        	self.files['/']['st_nlink'] -= 1

    	def setxattr(self, path, name, value, options, position=0):
        	"""Set extended attributes"""
        	attrs = self.files[path].setdefault('attrs', {})
        	attrs[name] = value

    	def statfs(self, path):
        	return dict(f_bsize = 512, f_blocks = 4096, f_bavail = 2048)

    	def symlink(self, target, source):
        	self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        	self.data[target] = source

    	def truncate(self, path, length, fh = None):
        	self.data[path] = self.data[path][:length]
        	self.files[path]['st_size'] = length

    	def unlink(self, path):
        	self.files.pop(path)

    	def utimens(self, path, times=None):
        	now = time()
        	atime, mtime = times if times else (now, now)
        	self.files[path]['st_atime'] = atime
        	self.files[path]['st_mtime'] = mtime

    	def write(self, path, data, offset, fh):
        	self.data[path] = self.data[path][:offset] + data
        	self.files[path]['st_size'] = len(self.data[path])
        	return len(data)


if __name__ == '__main__':
    	if len(argv) != 2:
        	print('Usage: %s <mountpoint>' % argv[0])
        	exit(1)

    	fuse = FUSE(FS(), argv[1], foreground = True)
