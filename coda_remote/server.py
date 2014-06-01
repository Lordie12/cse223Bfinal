#!/usr/bin/python

import rpyc, os, pickle, socket, sys, threading, getopt, hashlib
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

def invalidate_copies(known_clients):
	for clients in known_clients:
		logger.info('Invaliding %s' % clients)
		#Sending new metadata to all clients server knows of
		if True: #clients != self._conn.root:
			try:
				clients.invalidate(pickle.dumps(open(sname + '/root.dmeta', 'r').read()))
			except:
				pass

def myhash(inp):
	h = hashlib.new('ripemd160')
	h.update(inp)
	return h.hexdigest()

class MyService(rpyc.Service):
	def process_log(self, ID):

		global log, files
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
	
	def exposed_create(self, path):
		"""Check if a file exists, if not create 
		the file"""
		global lock, log, clock

		with lock:
			logger.info('Create acquiring lock')
			if os.path.isfile(sname + '/log.txt'):
				log = pickle.load(open(sname + '/log.txt', 'r'))

			log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR=self._conn._config['endpoints'][1][0] + \
				':' + str(self._conn._config['endpoints'][1][1]), OP='CREATE', PATH=path))

			ID = clock
			clock += 1
			log['Clock'] += 1
			pickle.dump(log, open(sname + '/log.txt', 'w+'))
			logger.info('ID %d and File check for %s' % (ID, sname + path))

		res = self.process_log(ID)

		start_new_thread(invalidate_copies, (known_clients,))
		return res

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

	def exposed_write(self, path, data, meta):
		
		global log, clock, lock

		with lock:
			path.encode('ascii', 'ignore')
			log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR= self._conn._config['endpoints'][1][0] +\
			':' + str(self._conn._config['endpoints'][1][1]), OP='WRITE', PATH=path, CONTENT = data, META = meta))

			pickle.dump(log, open(sname + '/log.txt', 'w+'))
		
			ID = clock
			clock += 1
			log['Clock'] += 1

		res = self.process_log(ID)

		'''
		Unable to pickle and invalidate clients,
		so lets call update metadata on each FUSE operation
		#Invalidating everyone else's copy here
		for clients in known_clients:
			#Sending new metadata to all clients server knows of
			if True: #clients != self._conn.root:
				try:
					clients.invalidate(pickle.dumps(open(sname + '/root.dmeta', 'r').read()))
				except:
					pass
		'''
		return res

	def exposed_fetch_meta(self):
		if os.path.isfile(sname + '/root.dmeta'):
			return open(sname + '/root.dmeta', 'r').read()
		else:
			return None

	def on_connect(self):
		global known_clients , lock

		'''
		if os.path.isfile('/servercache/root.dmeta'):
			files = pickle.load(open('/servercache/root.dmeta', 'r'))
		else:
			open('/servercache/root.dmeta', 'w+')

		logger.info('Client connected')
		if os.path.isfile('/servercache/log.txt'):
			log = pickle.load(open('/servercache/log.txt', 'r'))
		'''
        # code that runs when a connection is created

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

	t = ThreadedServer(MyService, hostname=hostname, port = port)
	logger.info('Server started at host %s and port %d' % (hostname, port))
	try:
		sname = hostname + str(port) + '/scache'
		sname = '/' + myhash(sname)
    		t.start()
	except:
		raise
	
		
