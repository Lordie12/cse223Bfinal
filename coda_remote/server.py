#!/usr/bin/python

import rpyc, os, pickle, socket, sys, threading, getopt, hashlib, re
from pyftpdlib.log import logger
from pyftpdlib.ioloop import _config_logging
from rpyc.utils.server import *
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import *
from thread import start_new_thread
_config_logging()

known_clients = []
sname = ''
lock = threading.Lock()
log = {}
if os.path.isfile(sname + '/log.txt'):
	log = pickle.load(open(sname + '/log.txt', 'r'))
else:
	log = dict(CP=0, Clock=0, OPS=[])

clock = log['Clock']
files = {}
myport = None

def natural_sort(l): 
    convert = lambda text: int(text) if text.isdigit() else text.lower() 
    alphanum_key = lambda key: [ convert(c) for c in re.split('([0-9]+)', key) ] 
    return sorted(l, key = alphanum_key)

def myhash(inp):
	h = hashlib.new('ripemd160')
	h.update(inp)
	return h.hexdigest() 

class MyService(rpyc.Service):
	def process_log(self, ID):

		"""First sort according to storeID<ID, clock>, now process log from CP 
		upto ID,  but if any non checked line encountered, return false or error"""
	
		global log, files
		#log sorted according to clock and then STORE ID
		#log = natural_sort(log)
		logger.info('Start log processing with CP %d and ID %d' % (log['CP'], ID))
		res = {}

		for r in range(log['CP'], ID + 1):
			path = log['OPS'][r]['PATH']

			logger.info('Processing log with path %s' % path)	

			if log['OPS'][r]['OP'] == 'CREATE':
				logger.info('Processing CREATE with CP %d ID %d' % (log['CP'], ID))
				if os.path.isfile(sname + path):
					res = {'Status' : True, 'Content-x' : open(sname + path).read(),\
						'Meta' : open(sname + path + '.meta', 'r').read()}
				else:
					#Create an empty file at server
					meta = dict(st_mode=(S_IFREG | 33188), st_nlink=1,
							st_size=0, st_ctime=time(), st_mtime=time(),
							st_atime=time())
					
					open(sname + path, 'w+').close()
					pickle.dump(meta, open(sname + path + '.meta', 'w+'))
					res = {'Status' : False, 'Content-x': '', 'Meta' : ''}

				log['CP'] += 1
				pickle.dump(log, open(sname + '/log.txt', 'w+'))

				logger.info('New CREATED metadata')

			elif log['OPS'][r]['OP'] == 'READ':
				logger.info('Processing READ with CP %d ID %d' % (log['CP'], ID))
				if os.path.isfile(sname + path):
					res = {'Status': True, 'Content-x' : open(sname + path).read(),\
						'Meta' : open(sname + path + '.meta', 'r').read()}
				else:
					res = {'Status' : False}

				log['CP'] += 1
				pickle.dump(log, open(sname + '/log.txt', 'w+'))

			elif log['OPS'][r]['OP'] == 'WRITE': 
	
				curr_meta = pickle.load(open(sname + path + '.meta', 'r'))
				print curr_meta
				if curr_meta['storeID']['ID']  == None:
					continue

				logger.info('Processing WRITE with CP %d ID %d' % (log['CP'], ID))
				#Writing into server copy
				open(sname + path, 'w+').write(log['OPS'][r]['CONTENT'])

				#Incrementing commit point and rewriting new log info to log
				log['CP'] += 1
				pickle.dump(log, open(sname + '/log.txt', 'w+'))

				#Setting result to true, writing new metadata to root.dmeta
				res = {'Status' : True}
				files[path] = pickle.loads(log['OPS'][r]['META'])
				pickle.dump(files, open(sname + '/root.dmeta', 'w+'))

		return res

	def exposed_is_dc(self):
		return None
	
	def exposed_is_exist(self):
		if os.path.isfile(sname + '/root.dmeta'):
			return {'Status': True, 'Content' : open(sname + '/root.dmeta', 'r').read()}
		else:
			return {'Status': False}	

	def exposed_update_meta(self, path, data):
		global files	
	
		if os.path.isfile(sname + '/root.dmeta'):
			files = pickle.load(open(sname + '/root.dmeta', 'r'))

		files[path] = pickle.loads(data)
		pickle.dump(files, open(sname + '/root.dmeta', 'w'))
	
	def exposed_rootmeta(self, data):
		global files
		logger.info('Received new metadata from client')
		try:
			os.mkdir(sname)
		except OSError:
			pass
		files['/'] = pickle.loads(data)
		pickle.dump(files, open(sname + '/root.dmeta', 'w'))
		return True

	def exposed_read(self, path):		
		global log, clock
		ts = str(ctime(time())) + ', ACCESS: ' + self._conn._config['endpoints'][1][0] +\
			':' + str(self._conn._config['endpoints'][1][1]) + ', READ, ' + 'PATH: ' + path + '\n' 
		
		logger.info('Reading from %s' % path)
		log = dict(CP=0, OPS=[])
		if os.path.isfile(sname + '/log.txt'):
			log = pickle.load(open(sname + '/log.txt', 'r'))

		log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR= self._conn._config['endpoints'][1][0] +\
                          ':' + str(self._conn._config['endpoints'][1][1]), OP='READ', PATH=path))
		ID = clock
		clock += 1
		log['Clock'] += 1
		pickle.dump(log, open(sname + '/log.txt', 'w+'))
	
		return self.process_log(ID)


	def exposed_ABCAST(self, log, entry):
		"""First method to be called on each exposed_method called
		by client"""
		"""Called on any method like write, create with the log
		first prepare on everymethod with passed log, return a timestamp
		this is the lamport clock and ID of server
		select max and send this to expose_confirm to confirm
		if everyone confirms(write to log), process log"""
		"""now execute query after everyone confirmed i.e., process_log"""
		global myport

		logger.info('Calling ABCAST here')
		listofports = [1000, 2222, 2223, 2224, 2225, 2226]
		reply = []
		clock = []
		connlist = []

		for port in listofports:
			if port != myport:
				try:
					conn = rpyc.connect('localhost', port, service=MyService)
					ret = conn.root.PREPARE(entry)
					reply.append(ret)
					clock.append(ret['Clock'])
					connlist.append(conn)
				except:
					continue

		mc = max(clock)
		#TODO: Get only one duplicate out based on sorted serverID
		duplicates = [k for k in reply if k['Clock'] == mc]
		entry['storeID'] = duplicates[0]
		#TODO: Error sometimes causing multiple entries to show
		
		with lock:
			for conn2 in connlist:
				try:
					conn2.root.confirm(entry)
				except:
					continue

	def clock_ops(self):
		global clock
		res = {'Clock' : clock, 'storeID' : sname[1:]}
		clock += 1
		return res

	def exposed_PREPARE(self, entry):
		"""For prepare messages to himself and other servers"""
		"""Add log entry to queue and store ID = <ID, clock>"""
		"""Checked field, is set to false in prepare"""
		global log, lock

		logger.info('Calling PREPARE')
		
		#Locking my logging procedure
		with lock:
			k = self.clock_ops()
			entry['storeID']['Clock'] = k['Clock']
			entry['storeID']['storeID'] = k['storeID']
			try:
				log['OPS'].append(entry)
				logger.info('Successfully appended to log in prepare')
				print 'PREPARE APPEND', log
			except KeyError:
				pass
		
		return k

	def exposed_confirm(self, entry):
		"""For confirm messages to himself and other servers"""
		"""confirm is passed the ID by prepare, changes timestamp to whatever
		is sent to it"""
		"""checked field set to true here"""
		global log
		logger.info('Calling confirm')
		print 'LOG', log
		print 'CONFIRM', entry

		entry['CHECKED'] = True
		for line in log['OPS']:
			print line
			if line['logID'] == str(entry['storeID']['Clock']) + '/' + entry['storeID']['storeID']:
				line['storeID'] = entry['storeID']

	def exposed_cleanup(self):
		"""Cleanup method"""
		pass

	def exposed_commit(self):
		"""Commit method"""
		#For now, confirm commits too, log processes entry if commit is ready
		pass

	def exposed_test(self):
		logger.info('Called Test')

	def exposed_create(self, path, storeID):
		"""Check if a file exists, if not create 
		the file"""
		global lock, log, clock

		with lock:
			logger.info('Create acquiring lock')
			if os.path.isfile(sname + '/log.txt'):
				try:
					log = pickle.load(open(sname + '/log.txt', 'r'))
				except:
					pass

			k = dict(logID=str(clock) + sname, ID=clock, TIME=ctime(time()), ADDR=self._conn._config['endpoints'][1][0] + \
				':' + str(self._conn._config['endpoints'][1][1]), OP='CREATE', PATH=path,\
				 storeID = pickle.loads(storeID), CHECKED=False)
			k['storeID']['Clock'] = clock
			k['storeID']['storeID'] = sname[1:]
	
			log['OPS'].append(k)

			ID = clock
			clock += 1
			log['Clock'] += 1
			pickle.dump(log, open(sname + '/log.txt', 'w+'))
			logger.info('ID %d and File check for %s' % (ID, sname + path))


		self.exposed_ABCAST(log['OPS'], k)

		#res = self.process_log(ID)

		return None#res


	def exposed_write(self, path, data, meta, ID):
		
		global log, clock, lock

		with lock:
			path.encode('ascii', 'ignore')
			log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR= self._conn._config['endpoints'][1][0] +\
			':' + str(self._conn._config['endpoints'][1][1]), OP='WRITE', PATH=path, CONTENT = data, \
				META = meta, storeID = ID))

			pickle.dump(log, open(sname + '/log.txt', 'w+'))
		
			ID = clock
			clock += 1
			log['Clock'] += 1

		res = self.process_log(ID)

		return res

	def exposed_fetch_meta(self):
		if os.path.isfile(sname + '/root.dmeta'):
			return open(sname + '/root.dmeta', 'r').read()
		else:
			return None

	def on_connect(self):
		pass

    	def on_disconnect(self):
        # code that runs when the connection has already closed
        	pass


if __name__ == "__main__":

	try:
		opts, args = getopt.getopt(sys.argv[1:], "h:p:", ["host=", "port="])
	except:
		print './server.py -h hostname -p portnumber'
		sys.exit(2) 

	for opt, args in opts:   
		if opt in ['-h', '--host']:
			hostname = args
		elif opt in ['-p', '--port']:
			port = int(args)

	t = ThreadedServer(MyService, hostname=hostname, port=port)
	logger.info('Server started at host %s and port %d' % (hostname, port))
	try:
		sname = hostname + str(port) + '/scache'
		sname = '/' + myhash(sname)
		t.start()
	except:
		raise
