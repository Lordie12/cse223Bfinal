#!/usr/bin/python

import os
import hashlib
import collections
import rpyc

def hash(filename):
	return hashlib.md5(filename).hexdigest()

class Venus:
	"""Local Venus cache manager, checks 
	cache for file, returns local FD if file exists,
	query server manager if no local copy"""
	def __init__(self):
		self.conn = rpyc.connect('localhost', 2222)

	def create(self, path, mode):
		#If remote file also does not exist, then create
		return self.conn.root.create(path)

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
