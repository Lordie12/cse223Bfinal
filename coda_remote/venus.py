#!/usr/bin/python

import os
import hashlib
import collections

def hash(filename):
	return hashlib.md5(filename).hexdigest()

class Cache:
	"""Cache, has an active list of cached files
	and a bitmap indicating dirty files."""
	def __init__(self):
		self.filecache = []
		self.bitmap = {}
		self.log = []
		self.FD = {}

	def exists(self, filename):
		"""See if file exists in cache"""
		direntry, filename = os.path.split(filename)
		#if directory does not exist
		if not os.path.exists(direntry + '/'):
			return False
		else:
			#if file does not exist
			if filename not in os.listdir(direntry):
				return False
		return True

	def get_FD(self, filename):
		return self.FD[hash(filename)]

	def cache_add(self, filename, mode):
		"""Bring file into cache from remote server
		if not found in local cache"""
		self.filecache.append(filename)
		self.bitmap[hash(filename)] = 1
		ofname = filename

		#code to bring file from server, if this fails
		#create local copy and send it to server
		direntry, filename = os.path.split(filename)
		try:
			#Creating new directory in cache
			os.mkdir(direntry + '/')
			#todo: create directories in server too
		except OSError:
			#directory already exists
			pass

		self.FD[hash(ofname)] = open(ofname, mode)
		#todo: create file in server too 

	def cache_read(self, filename):
		self.FD[hash(filename)] = open(filename, 'r')
		data = self.FD[hash(filename)].read()
		self.FD[hash(filename)].close()
		return data

	def cache_delete(self, filename):
		try:
			os.remove(filename)
		except OSError:
			print 'File not found'
			return
		print 'File deleted'
		self.log.append("Timestamp: " + filename + "Operation: deleted")
	
	def write_logs(self, filename, data):
		self.log.append("Timestamp: " + filename + "Operation: Write " + "Data: " + data)
	def print_logs(self):
		return self.log

class Venus:
	"""Local Venus cache manager, checks 
	cache for file, returns local FD if file exists,
	query server manager if no local copy"""
	def __init__(self, master):
		self.master = master
		self.cache = Cache()

	def print_logs(self):
		return self.cache.print_logs()

	def write_logs(self, filename, data):
		self.cache.write_logs(filename, data)

	def write(self, filename,  data): # filename is full namespace path
		"""Full namespace with path, if present in local cache, return
		FD else need to contact server"""
		if self.exists(filename): # if already exists, overwrite
			FD = open(filename, 'w')
		else:

			self.cache_add(filename, 'w')
			FD = self.get_FD(filename)

		self.write_file(FD, data)
		self.write_logs(filename, data)

    	def write_file(self, FD, data):
		FD.write(data)
		FD.close()

    	def write_append(self, filename, data):
        	if not self.exists(filename):
            		raise Exception("append error, file does not exist: " \
                 	+ filename)
		self.cache_add(filename, 'a')	
		FD = self.get_FD(filename)
		FD.write(data)
		FD.close()

	def cache_add(self, filename, mode):
		return self.cache.cache_add(filename, mode)

    	def exists(self, filename):
		return self.cache.exists(filename)

	def get_FD(self, filename):
		return self.cache.get_FD(filename)

    	def read(self, filename):
        	if not self.exists(filename):
            		raise Exception("read error, file does not exist: " \
                	+ filename)
        	return self.cache.cache_read(filename)

	def delete(self, filename):
		self.cache.cache_delete(filename)
