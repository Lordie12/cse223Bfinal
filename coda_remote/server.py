#!/usr/bin/python

import rpyc, os, pickle, socket, sys, threading
from pyftpdlib.log import logger
from pyftpdlib.ioloop import _config_logging
from rpyc.utils.server import *
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import *
from thread import start_new_thread
_config_logging()

lock = threading.Lock()
log = {}
if os.path.isfile('/servercache/log.txt'):
	log = pickle.load(open('/servercache/log.txt', 'r'))
else:
	log = dict(CP=0, Clock=0, OPS=[])

clock = log['Clock']
files = {}

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
				if os.path.isfile('/servercache' + path):
					res = {'Status' : True, 'Content-x' : open('/servercache' + path).read(),\
						'Meta' : open('/servercache' + path + '.meta', 'r').read()}
				else:
					#Create an empty file at server
					meta = dict(st_mode=(S_IFREG | 33188), st_nlink=1,
							st_size=0, st_ctime=time(), st_mtime=time(),
							st_atime=time())
					
					open('/servercache' + path, 'w+').close()
					pickle.dump(meta, open('/servercache' + path + '.meta', 'w+'))
					res = {'Status' : False, 'Content-x': '', 'Meta' : ''}

				log['CP'] += 1
				pickle.dump(log, open('/servercache/log.txt', 'w+'))

				logger.info('New CREATED metadata')
				#pickle.dump(files, open('/servercache/root.dmeta', 'w'))

			elif log['OPS'][r]['OP'] == 'READ':
				pass

			elif log['OPS'][r]['OP'] == 'WRITE': 
				logger.info('Processing WRITE with CP %d ID %d' % (log['CP'], ID))
				open(path, 'w+').write(log['OPS'][r]['CONTENT'])

				log['CP'] += 1
				pickle.dump(log, open('/servercache/log.txt', 'w+'))

				res = {'Status' : True}
				files[path] = pickle.loads(log['OPS'][r]['META'])
				logger.info('New write metadata received %s' % log['OPS'][r]['META'])
				pickle.dump(files, open('/servercache/root.dmeta', 'w+'))
				open('/servercache' + path, 'w').write(log['OPS'][r]['CONTENT'])

		return res

	def exposed_is_exist(self):
		if os.path.isfile('/servercache/root.dmeta'):
			#logger.info('Metadata exists on server only')
			return {'Status': True, 'Content' : open('/servercache/root.dmeta', 'r').read()}
		else:
			return {'Status': False}	

	def exposed_update_meta(self, path, data):
		global files	
	
		if os.path.isfile('/servercache/root.dmeta'):
			files = pickle.load(open('/servercache/root.dmeta', 'r'))

		files[path] = pickle.loads(data)
		pickle.dump(files, open('/servercache/root.dmeta', 'w'))
	
	def exposed_rootmeta(self, data):
		global files
		logger.info('Received new metadata from client')
		try:
			os.mkdir('/servercache')
		except OSError:
			pass
		files['/'] = pickle.loads(data)
		pickle.dump(files, open('/servercache/root.dmeta', 'w'))
		return True
	
	def exposed_create(self, path):
		"""Check if a file exists, if not create 
		the file"""
		global lock, log, clock

		with lock:
			logger.info('Create acquiring lock')
			if os.path.isfile('/servercache/log.txt'):
				log = pickle.load(open('/servercache/log.txt', 'r'))

			log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR=self._conn._config['endpoints'][1][0] + \
				':' + str(self._conn._config['endpoints'][1][1]), OP='CREATE', PATH=path))

			ID = clock
			clock += 1
			log['Clock'] += 1
			pickle.dump(log, open('/servercache/log.txt', 'w+'))
			logger.info('ID %d and File check for %s' % (ID, '/servercache' + path))

		return self.process_log(ID)

	def exposed_read(self, path):		
		global log, clock
		ts = str(ctime(time())) + ', ACCESS: ' + self._conn._config['endpoints'][1][0] +\
			':' + str(self._conn._config['endpoints'][1][1]) + ', READ, ' + 'PATH: ' + path + '\n' 
		
		logger.info('Reading from %s' % path)
		log = dict(CP=0, OPS=[])
		if os.path.isfile('/servercache/log.txt'):
			log = pickle.load(open('/servercache/log.txt', 'r'))

		log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR= self._conn._config['endpoints'][1][0] +\
                          ':' + str(self._conn._config['endpoints'][1][1]), OP='READ', PATH=path))
		ID = clock
		clock += 1
		log['Clock'] += 1
		pickle.dump(log, open('/servercache/log.txt', 'w+'))

	def exposed_write(self, path, data, meta):
		
		global log, clock, lock

		with lock:
			path.encode('ascii', 'ignore')
			log['OPS'].append(dict(ID=clock, TIME=ctime(time()), ADDR= self._conn._config['endpoints'][1][0] +\
			':' + str(self._conn._config['endpoints'][1][1]), OP='WRITE', PATH=path, CONTENT = data, META = meta))

			pickle.dump(log, open('/servercache/log.txt', 'w+'))
		
			logger.info(log)	
			ID = clock
			clock += 1
			log['Clock'] += 1

		return self.process_log(ID)

	def on_connect(self):
		'''global log, files

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
	hostname = 'localhost'
	port = 2222
    	t = ThreadedServer(MyService, hostname=hostname, port = port)
	logger.info('Server started at host %s and port %d' % (hostname, port))
	try:
		pass
    		start_new_thread(t.start, ())
	except EOFError:
		raise
	raw_input('')
	
		
