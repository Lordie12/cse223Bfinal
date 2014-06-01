#!/usr/bin/python

import os, rpyc, hashlib, sys, pickle

cachedir = ''

def hash(filename):
	return hashlib.md5(filename).hexdigest()

class Callback(rpyc.Service):
	def exposed_invalidate(self, meta):
		'''Invalidating this client's copy
		of metadata received from server
		upon some other client writing'''
		global cachedir
		print 'Metadata invalidated after a write'
		f = open(cachedir + '/root.dmeta', 'w')
		f.write(pickle.loads(meta))
		f.close()
			

class Venus:
	"""Local Venus cache manager, checks 
	cache for file, returns local FD if file exists,
	query server manager if no local copy"""
	def __init__(self, mydir):
		self.conn = None
		global cachedir
		cachedir = mydir
		f = open('/Users/Lanfear/Desktop/Spring 14/CSE223B/cse223Bfinal/coda_remote/list', 'r')
		for servers in f:
			hostname, port = servers.split(':')
			try:
				self.conn = rpyc.connect(hostname, int(port), service=Callback)
			except:	
				continue

	def create(self, path, mode, dc):
		#If remote file also does not exist, then create
		if dc == False:
			return self.conn.root.create(path)
	
	def is_dc(self):
		return self.conn.root.is_dc()

	def update_meta(self, path, meta, dc):
		if dc == False:
			return self.conn.root.update_meta(path, meta)

	def read(self, path, dc):
		if dc == False:
			return self.conn.root.read(path)

	def write(self, path, data, meta):
		return self.conn.root.write(path, data, meta)

	def meta(self, data, dc):
		if dc == False:
			return self.conn.root.rootmeta(data)

	def is_exist(self, dc):
		if dc == False:
			return self.conn.root.is_exist()
		else:
			return None

	def fetch_meta(self, dc):
		if dc == False:
			res = self.conn.root.fetch_meta()
			if res is None:
				pass
			else:
				f = open(cachedir + '/root.dmeta', 'w')
			f.write(res)
