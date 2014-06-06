#!/usr/bin/python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import *

from fuse import FUSE, FuseOSError, Operations
from coda_remote.venus import Venus
from pyftpdlib.log import logger
from pyftpdlib.ioloop import _config_logging
import json, pickle, os, logging, sys, rpyc, threading
from thread import start_new_thread
import threading, socket

cachedir = '/cache'

if not hasattr(__builtins__, 'bytes'):
	bytes = str

class FS(Operations):
	"""Creates an in-memory filesystem using FUSE,
	self.files stores the file information (similar to inode),
	self.data stores filedata, self.fd is the filedescriptor"""


   	def __init__(self):
		global cachedir
		_config_logging()
		self.files = {}
        	self.data = defaultdict(bytes)
        	self.fd = 0
		self.log = []
		#For reintegration, load log if it exists
		#We write into log during disconnected operations
		if os.path.isfile(cachedir + '/log.txt'):
			try:
				self.log = pickle.load(open(cachedir + '/log.txt', 'r'))
			except:
				pass
	
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
			logger.info('Disconnected operation')

        	now = time()
		st_mode =  (S_IFDIR | 0755) 
		st_mtime = st_atime = st_ctime = now
		st_nlink = 2
		f_dir = list()

		if not os.path.exists(cachedir):
			os.mkdir(cachedir)
		#If root metadata exists, then load it in-memory
		#TODO: change this for disconnected operations vs.
		# new guy coming online first time 

		is_exist = self.venus.is_exist(self.dc)
		if os.path.isfile(cachedir + '/root.dmeta'):
			logger.info('Root metadata exists, loading into memory')
			self.files = pickle.load(open(cachedir + '/root.dmeta', 'r'))
			print self.files
			self.venus.meta(pickle.dumps(self.files['/']), self.dc)

		elif is_exist is not None and is_exist['Status'] == True:
			logger.info('Root metadata does not exist locally but remotely')
			open(cachedir + '/root.dmeta', 'w+').write(is_exist['Content'])

		else:
			logger.info('Root metadata does not exist locally and remotely, '+\
				'creating locally and uploading to remote')
			#Recreate new root directory
        		self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime = now,
                               st_mtime = now, st_atime = now, st_nlink = 2, f_dir = list())
	
			pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w'))
			self.venus.meta(pickle.dumps(self.files['/']), self.dc)

	
    	def chmod(self, path, mode):
		global cachedir
		logger.info("Changing permissions of file %s into %s" % (path, mode))
		#Reloading metadata here
		#Support for disconnected operation and reloading
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True

		if self.dc == False:
			self.venus.reintegration()

		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	self.files[path]['st_mode'] &= 0770000
        	self.files[path]['st_mode'] |= mode
		f = open(cachedir + path + '.meta', 'w')
		f.write(self.files[path])
		f.close()
        	return 0

    	def chown(self, path, uid, gid):
		global cachedir
		logger.info("Changing permissions of file %s into %s" % (path, mode))
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	self.files[path]['st_uid'] = uid
        	self.files[path]['st_gid'] = gid
		f = open(cachedir + path + '.meta', 'w')
		f.write(self.files[path])
		f.close()
	
    	def create(self, path, mode):
		global cachedir
		"""Create a file at the specified path with the specified mode,
		mode is by default S_IFREG which is the linux equivalent
		of a regular file ORed with the mode specified
		Creating a new file does not create issues at the local cache
		if the file exists but may cause issues if local cache is not 
		the most updated copy because Venus will have to fetch entries
		into cache before issuing a create request"""

		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)

		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		logger.info("Creating a new file %s with mode %s" % (path, mode))
		res = self.venus.create(path, mode, self.dc)

		self.log.append(dict(TIME=ctime(time()), ops='create', path=path, mode=mode, data='', meta='', dc=False))	

		if (res is not None and res['Status'] == False) or (res is None):
			logger.info('Status is false')
			#File does not exist
			self.files[path] = dict(st_mode=(S_IFREG | mode), st_nlink=1,
					st_size=0, st_ctime=time(), st_mtime=time(),
					st_atime=time())
			#Case when server failed in between server check and the following server write
			try:
				self.venus.update_meta(path, pickle.dumps(self.files[path]), self.dc)
			except:
				self.dc = True	
			
			self.log.append(dict(TIME=ctime(time()), ops='update_meta', path=path,\
				 mode='', data='', meta=pickle.dumps(self.files[path]), dc=False))	

			#Not root directory
			if len(path.split('/')) != 2:
				logger.info('File created in non-root dir')
				newpath = path[:path.rfind('/')]
				self.files[newpath]['f_dir'].append(path)
				#Store parentdir metadata in server
				try:
					self.venus.update_meta(newpath, pickle.dumps(self.files[newpath]), self.dc)
				except:
					self.dc = True

				self.log.append(dict(TIME=ctime(time()), ops='update_meta', path=newpath,\
					mode='', data='', meta=pickle.dumps(self.files[newpath]), dc=False))	
				
				#Update local root dmeta copy	
				pickle.dump(self.files[newpath], open(cachedir + newpath + '.dmeta', 'w+'))
			else:
			#Root directory
				logger.info('File created in root directory')
				self.files['/']['f_dir'].append(path)
				try:
					self.venus.update_meta('/', pickle.dumps(self.files['/']), self.dc)
				except:
					self.dc = True

				self.log.append(dict(TIME=ctime(time()), ops='update_meta', path='/',\
				 mode='', data='', meta=pickle.dumps(self.files['/']), dc=False))	

			pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w')) 
			#Store newfile meta
			pickle.dump(self.files[path], open(cachedir + path + '.meta', 'w+'))
			f = open(cachedir + path, 'w')
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
                                pickle.dump(self.files[path[:path.rfind('/')]], open(cachedir + path[:path.rfind('/')] + '.dmeta', 'w+'))
                        else:
                        #Root directory
                                self.files['/']['f_dir'].append(path)
                                pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w'))

                        #Store newfile meta
			f = open(cachedir + path + '.meta', 'w+')
			f.write(res['Meta'])
			f.close
                        f = open(cachedir + path, 'w+')
                        f.write(res['Content-x'])
			f.close()
		
		pickle.dump(self.log, open(cachedir + '/log.txt', 'w+'))	 
		return self.fd + 1

    	def getattr(self, path, fh = None):
		global cachedir
		logger.info("Getting attributes of file %s", path)
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True

		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		try:
			self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		except:
			pass
		"""If file not present, return error"""
        	if path not in self.files:
            		raise FuseOSError(ENOENT)

        	return self.files[path]
	
    	def getxattr(self, path, name, position = 0):
		global cachedir
		logger.info('Getting extended attributes of %s'  % path)
		#Reloading metadata here
		self.venus.fetch_meta(self.dc)
		try:
			self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		except EOFError:
			pass

        	attrs = self.files[path].get('attrs', {})

        	try:
            		return attrs[name]
        	except KeyError:
			#Should return ENOATTR but has an error, need to fix
            		return ''

    	def listxattr(self, path):
		global cachedir
		logger.info('Listing extended attributes of %s' % path)
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))

        	attrs = self.files[path].get('attrs', {})
        	return attrs.keys()
	
    	def mkdir(self, path, mode):
		global cachedir
		"""Create a new directory with the default linux flag
		for a directory, S_IFDIR ORed with any additional modes
		specified by the application"""
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		logger.info("Creating new directory %s with mode %s" % (path, mode))
        	self.files[path] = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time(), f_dir = [])

		if not os.path.exists(cachedir + path):
			os.mkdir(cachedir + path)

		if len(path.split('/')) != 2:
			self.files[path[:path.rfind('/')]]['f_dir'].append(path)
			#Store parentdir metadata
			pickle.dump(self.files[path[:path.rfind('/')]], open(cachedir + path[:path.rfind('/')] + '.dmeta', 'w+')) 
			#Store newdir metadata
			pickle.dump(self.files[path], open(cachedir + path + '.dmeta', 'w+'))
		else:
			self.files['/']['f_dir'].append(path)
			pickle.dump(self.files[path], open(cachedir + path + '.dmeta', 'w+'))

        	self.files['/']['st_nlink'] += 1
		pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w+'))

    	def open(self, path, flags):
		global cachedir
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		logger.info("Opening file %s with flags %s" % (path, flags))
        	self.fd += 1
        	return self.fd

    	def read(self, path, size, offset, fh):
		global cachedir
		logger.info("Reading %d bytes of file %s from offset %d" % (size, path, offset))
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)

		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		res = self.venus.read(path, self.dc)

		if res['Status'] == True:
			logger.info('Obtained file contents')
			#Writing data and metadata of fetched file
			f = open(cachedir + path, 'w')
			f.write(res['Content-x'])
			f.close()
			f = open(cachedir + path + '.meta', 'w')
			f.write(res['Meta'])
			f.close()

			return res['Content-x'][offset:offset + size]
		elif res['Status'] == False:
			raise FuseOSError(ENOENT)
 
        	return self.data[path][offset:offset + size]

    	def readdir(self, path, fh):
		global cachedir
		"""Reads directory as specified by path, filehandler is empty for now,
		gotta fix multi-level directories - How does this work?
		A dir. inside a dir., /hello/dude starts with the prefix /hello/ which
		only to those of path + 1"""
		logger.info("Reading directory %s" % path)
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		#Reloading metadata here
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	return ['.', '..'] + [x[x.rfind('/') + 1:] for x in self.files[path]['f_dir']]

    	def readlink(self, path):
		global cachedir
		"""Only reads the link and returns data"""
		logger.info('Reading link of %s' % path)
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	return self.data[path]
	
    	def removexattr(self, path, name):
		global cachedir
		logger.info('Remove extended attributes of %s' % path)
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	attrs = self.files[path].get('attrs', {})

        	try:
            		del attrs[name]
        	except KeyError:
            		pass        # Should return ENOATTR
	
    	def rename(self, old, new):
		global cachedir
		"""Remove old filename and insert new one"""
		logger.info('Renaming %s to %s' % (old, new))
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
    		self.files[new] = self.files.pop(old)

		if len(old.split('/')) != 2:
			self.files[new[:new.rfind('/')]]['f_dir'].remove(old)
			self.files[new[:new.rfind('/')]]['f_dir'].append(new)
		else:
			self.files['/']['f_dir'].remove(old)
			self.files['/']['f_dir'].append(new)

    	def rmdir(self, path):
		global cachedir
		"""Remove directory by popping old filepath
		and removing file counter, have to implement
		recursive removal rm -rf"""
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		logger.info("Removing %s", path)
        	self.files.pop(path)

		if len(old.split('/')) != 2:
			self.files[new[:new.rfind('/')]]['f_dir'].remove(old)
		else:
			self.files['/']['f_dir'].remove(old)

        	self.files['/']['st_nlink'] -= 1

	
    	def setxattr(self, path, name, value, options, position=0):
		global cachedir
        	"""Set extended attributes"""
		logger.info('Setting extended attributes')
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	attrs = self.files[path].setdefault('attrs', {})
        	attrs[name] = value

    	def statfs(self, path):
		global cachedir
		logger.info('Calling statfs')
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		try:
			self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
		except:
			pass
        	return dict(f_bsize = 512, f_blocks = 4096, f_bavail = 2048)

    	def symlink(self, target, source):
		global cachedir
		logger.info('Creating symlink')

		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))
        	self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        	self.data[target] = source

    	def truncate(self, path, length, fh = None):
		global cachedir
		logger.info('Calling truncate')
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))

        	self.data[path] = self.data[path][:length]
        	self.files[path]['st_size'] = length
	
    	def unlink(self, path):
		global cachedir
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))

		if path not in self.files.keys():
			raise Exception('File does not exist')
		logger.info('File %s unlinked and deleted' % path)
        	self.files.pop(path)
		self.files['/']['f_dir'].remove(path)
		os.remove(cachedir + path)
		os.remove(cachedir + path + '.meta')
		pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w+'))

    	def utimens(self, path, times=None):
		global cachedir
		logger.info('Calling utimens')
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))

        	now = time()
        	atime, mtime = times if times else (now, now)
        	self.files[path]['st_atime'] = atime
        	self.files[path]['st_mtime'] = mtime

    	def write(self, path, data, offset, fh):
		global cachedir
		#Reloading metadata here
		#Disconnected operation code comes here
		self.venus = Venus(cachedir)	
		self.dc = False
		try:
			self.venus.is_dc()
		except AttributeError:
			self.dc = True

		if self.dc == False:
			self.venus.reintegration()
		self.venus.fetch_meta(self.dc)
		self.files = pickle.load(open(cachedir + '/root.dmeta','r'))

		logger.info("Writing into file %s from offset %d" % (path, offset))
        	self.data[path] = self.data[path][:offset] + data
        	self.files[path]['st_size'] = len(self.data[path])
		self.files[path]['st_mtime'] = time()
		self.files[path]['st_atime'] = time()
		pickle.dump(self.files[path], open(cachedir + path + '.meta', 'w+'))
		pickle.dump(self.files, open(cachedir + '/root.dmeta', 'w+'))
	
		f = open(cachedir + path, 'w+')
		f.write(data)
		f.close()
		
		try:
			self.venus.write(path, data, pickle.dumps(self.files[path]), self.dc)
		except:
			self.dc = True

		self.log.append(dict(TIME=ctime(time()), ops='write', path=path,\
				 mode='', data=data, meta=pickle.dumps(self.files[path]), dc=False))	
		pickle.dump(self.log, open(cachedir + '/log.txt', 'w'))

        	return len(data)


if __name__ == '__main__':
    	if len(argv) != 2:
        	print('Usage: %s <mountpoint>' % argv[0])
        	exit(1)

	_config_logging()
	
	logger.info('File System mounted at /%s' % argv[1])
	cachedir += argv[1]

	if not os.path.isfile(cachedir + '/log.txt'):
		open(cachedir + '/log.txt', 'w+')

    	fuse = FUSE(FS(), argv[1], foreground = True)
