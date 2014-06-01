#!/usr/bin/python

import os, rpyc, hashlib
import sys

def hash(filename):
	return hashlib.md5(filename).hexdigest()

class Callback(rpyc.Service):
	def exposed_sample(self):
		pass

class Venus:
	"""Local Venus cache manager, checks 
	cache for file, returns local FD if file exists,
	query server manager if no local copy"""
	def __init__(self):
		self.conn = rpyc.connect('localhost', 2222, service=Callback)

	def create(self, path, mode):
		#If remote file also does not exist, then create
		return self.conn.root.create(path)
	
	def update_meta(self, path, meta):
		return self.conn.root.update_meta(path, meta)

	def read(self, path):
		return self.conn.root.read(path)

	def write(self, path, data, meta):
		return self.conn.root.write(path, data, meta)

	def meta(self, data):
		return self.conn.root.rootmeta(data)

	def is_exist(self):
		return self.conn.root.is_exist()
