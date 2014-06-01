#!/usr/bin/python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations
from coda_remote.venus import Venus
from pyftpdlib.log import logger
from pyftpdlib.ioloop import _config_logging
import json, pickle, os, logging, sys, rpyc, threading
from thread import start_new_thread
import threading, socket

if not hasattr(__builtins__, 'bytes'):
	bytes = str

class FS(Operations):
	"""Creates an in-memory filesystem using FUSE,
	self.files stores the file information (similar to inode),
	self.data stores filedata, self.fd is the filedescriptor"""

   	def __init__(self):
		_config_logging()
		self.files = {}
        	self.data = defaultdict(bytes)
        	self.fd = 0
		self.venus = Venus()		

        	now = time()
		st_mode =  (S_IFDIR | 0755) 
		st_mtime = st_atime = st_ctime = now
		st_nlink = 2
		f_dir = list()

		if not os.path.exists('/cache'):
			os.mkdir('/cache')
		#If root metadata exists, then load it in-memory
		#TODO: change this for disconnected operations vs.
		# new guy coming online first time 
		if os.path.isfile('/cache/root.dmeta'):
			logger.info('Root metadata exists, loading into memory')
			self.files = pickle.load(open('/cache/root.dmeta', 'r'))
			self.venus.meta(pickle.dumps(self.files['/']))

		elif self.venus.is_exist()['Status'] == True:
			logger.info('Root metadata does not exist locally but remotely')
			open('/cache/root.dmeta', 'w+').write(self.venus.is_exist()['Content'])

		else:
			logger.info('Root metadata does not exist locally and remotely, '+\
				'creating locally and uploading to remote')
			#Recreate new root directory
        		self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime = now,
                               st_mtime = now, st_atime = now, st_nlink = 2, f_dir = list())
	
			pickle.dump(self.files, open('/cache/root.dmeta', 'w+'))
			self.venus.meta(pickle.dumps(self.files['/']))

	
    	def chmod(self, path, mode):
		logger.info("Changing permissions of file %s into %s" % (path, mode))
        	self.files[path]['st_mode'] &= 0770000
        	self.files[path]['st_mode'] |= mode
		f = open('/cache' + path + '.meta', 'w')
		f.write(self.files[path])
		f.close()
        	return 0

    	def chown(self, path, uid, gid):
		logger.info("Changing permissions of file %s into %s" % (path, mode))
        	self.files[path]['st_uid'] = uid
        	self.files[path]['st_gid'] = gid
		f = open('/cache' + path + '.meta', 'w')
		f.write(self.files[path])
		f.close()
	
    	def create(self, path, mode):
		"""Create a file at the specified path with the specified mode,
		mode is by default S_IFREG which is the linux equivalent
		of a regular file ORed with the mode specified
		Creating a new file does not create issues at the local cache
		if the file exists but may cause issues if local cache is not 
		the most updated copy because Venus will have to fetch entries
		into cache before issuing a create request"""

		logger.info("Creating a new file %s with mode %s" % (path, mode))
		res = self.venus.create(path, mode)
	 
		if res['Status'] == False:
			logger.info('Status is false')
			#File does not exist
			self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
					st_size=0, st_ctime=time(), st_mtime=time(),
					st_atime=time())
		
			self.venus.update_meta(path, pickle.dumps(self.files[path]))	
	
			#Not root directory
			if len(path.split('/')) != 2:
				logger.info('File created in non-root dir')
				newpath = path[:path.rfind('/')]
				self.files[newpath]['f_dir'].append(path)
				#Store parentdir metadata in server
				self.venus.update_meta(newpath, pickle.dumps(self.files[newpath]))
	
				pickle.dump(self.files[newpath], open('/cache' + newpath + '.dmeta', 'w+'))
			else:
			#Root directory
				logger.info('File created in root directory')
				self.files['/']['f_dir'].append(path)
				self.venus.update_meta('/', pickle.dumps(self.files['/']))

			pickle.dump(self.files, open('/cache/root.dmeta', 'w')) 
			#Store newfile meta
			pickle.dump(self.files[path], open('/cache' + path + '.meta', 'w+'))
			f = open('/cache' + path, 'w')
			f.close()
		
		else:
			logger.info("Contents of %s brought in from server, %s" % (path, res['Content-x']))
			#File does not exist, unpickle metadata from string
			meta = pickle.loads(res['Meta'])
			#Update in-memory metadata using new information
                        self.files[path] = meta

                        #Not root directory
                        if len(path.split('/')) != 2:
                                self.files[path[:path.rfind('/')]]['f_dir'].append(path)
                                #Store parentdir metadata
                                pickle.dump(self.files[path[:path.rfind('/')]], open('/cache' + path[:path.rfind('/')] + '.dmeta', 'w+'))
                        else:
                        #Root directory
                                self.files['/']['f_dir'].append(path)
                                pickle.dump(self.files, open('/cache/root.dmeta', 'w'))

                        #Store newfile meta
			f = open('/cache' + path + '.meta', 'w+')
			f.write(res['Meta'])
			f.close
                        f = open('/cache' + path, 'w+')
                        f.write(res['Content-x'])
			f.close()
		
		return self.fd + 1

    	def getattr(self, path, fh = None):
		logger.info("Getting attributes of file %s", path)
		"""If file not present, return error"""
        	if path not in self.files:
            		raise FuseOSError(ENOENT)

        	return self.files[path]
	
    	def getxattr(self, path, name, position = 0):
		logger.info('Getting extended attributes of %s'  % path)
        	attrs = self.files[path].get('attrs', {})

        	try:
            		return attrs[name]
        	except KeyError:
			#Should return ENOATTR but has an error, need to fix
            		return ''

    	def listxattr(self, path):
		logger.info('Listing extended attributes of %s' % path)
        	attrs = self.files[path].get('attrs', {})
        	return attrs.keys()
	
    	def mkdir(self, path, mode):
		"""Create a new directory with the default linux flag
		for a directory, S_IFDIR ORed with any additional modes
		specified by the application"""
		logger.info("Creating new directory %s with mode %s" % (path, mode))
        	self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), f_dir = [])

		if not os.path.exists('/cache' + path):
			os.mkdir('/cache' + path)

		if len(path.split('/')) != 2:
			self.files[path[:path.rfind('/')]]['f_dir'].append(path)
			#Store parentdir metadata
			pickle.dump(self.files[path[:path.rfind('/')]], open('/cache' + path[:path.rfind('/')] + '.dmeta', 'w+')) 
			#Store newdir metadata
			pickle.dump(self.files[path], open('/cache' + path + '.dmeta', 'w+'))
		else:
			self.files['/']['f_dir'].append(path)
			pickle.dump(self.files[path], open('/cache' + path + '.dmeta', 'w+'))

        	self.files['/']['st_nlink'] += 1
		pickle.dump(self.files, open('/cache/root.dmeta', 'w+'))

    	def open(self, path, flags):
		logger.info("Opening file %s with flags %s" % (path, flags))
        	self.fd += 1
        	return self.fd

    	def read(self, path, size, offset, fh):
		logger.info("Reading %d bytes of file %s from offset %d" % (size, path, offset))
		self.venus.read(path)
        	return self.data[path][offset:offset + size]

    	def readdir(self, path, fh):
		"""Reads directory as specified by path, filehandler is empty for now,
		gotta fix multi-level directories - How does this work?
		A dir. inside a dir., /hello/dude starts with the prefix /hello/ which
		only to those of path + 1"""
		logger.info("Reading directory %s" % path)
        	return ['.', '..'] + [x[x.rfind('/') + 1:] for x in self.files[path]['f_dir']]

    	def readlink(self, path):
		"""Only reads the link and returns data"""
		logger.info('Reading link of %s' % path)
        	return self.data[path]
	
    	def removexattr(self, path, name):
		logger.info('Remove extended attributes of %s' % path)
        	attrs = self.files[path].get('attrs', {})

        	try:
            		del attrs[name]
        	except KeyError:
            		pass        # Should return ENOATTR
	
    	def rename(self, old, new):
		"""Remove old filename and insert new one"""
		logger.info('Renaming %s to %s' % (old, new))
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
		logger.info("Removing %s", path)
        	self.files.pop(path)

		if len(old.split('/')) != 2:
			self.files[new[:new.rfind('/')]]['f_dir'].remove(old)
		else:
			self.files['/']['f_dir'].remove(old)

        	self.files['/']['st_nlink'] -= 1

	
    	def setxattr(self, path, name, value, options, position=0):
        	"""Set extended attributes"""
		logger.info('Setting extended attributes')
        	attrs = self.files[path].setdefault('attrs', {})
        	attrs[name] = value

    	def statfs(self, path):
		logger.info('Calling statfs')
        	return dict(f_bsize = 512, f_blocks = 4096, f_bavail = 2048)

    	def symlink(self, target, source):
		logger.info('Creating symlink')
        	self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        	self.data[target] = source

    	def truncate(self, path, length, fh = None):
		logger.info('Calling truncate')
        	self.data[path] = self.data[path][:length]
        	self.files[path]['st_size'] = length
	
    	def unlink(self, path):
		if path not in self.files.keys():
			raise Exception('File does not exist')
		logger.info('File %s unlinked and deleted' % path)
        	self.files.pop(path)
		self.files['/']['f_dir'].remove(path)
		os.remove('/cache' + path)
		os.remove('/cache' + path + '.meta')
		pickle.dump(self.files, open('/cache/root.dmeta', 'w+'))

    	def utimens(self, path, times=None):
		logger.info('Calling utimens')
        	now = time()
        	atime, mtime = times if times else (now, now)
        	self.files[path]['st_atime'] = atime
        	self.files[path]['st_mtime'] = mtime

    	def write(self, path, data, offset, fh):
		logger.info("Writing into file %s from offset %d" % (path, offset))
        	self.data[path] = self.data[path][:offset] + data
        	self.files[path]['st_size'] = len(self.data[path])
		self.files[path]['st_mtime'] = time()
		self.files[path]['st_atime'] = time()
		pickle.dump(self.files[path], open('/cache' + path + '.meta', 'w+'))
		pickle.dump(self.files, open('/cache/root.dmeta', 'w+'))
	
		f = open('/cache' + path, 'w+')
		f.write(data)
		f.close()
		
		self.venus.write(path, data, pickle.dumps(self.files[path]))
        	return len(data)


if __name__ == '__main__':
    	if len(argv) != 2:
        	print('Usage: %s <mountpoint>' % argv[0])
        	exit(1)

	_config_logging()
	
	logger.info('File System mounted at /%s' % argv[1])
    	fuse = FUSE(FS(), argv[1], foreground = True)
