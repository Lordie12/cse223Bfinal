#!/usr/bin/python

from coda_client import *
from venus import *
import sys

if __name__ == '__main__':
	# test script for filesystem

    	# setup
    	client = Venus(None)

    	# test write, exist, read
    	print "\nWriting..."
    	client.write("/usr/python/readme.txt", """
        	A Sample Coda test, if this file exists in the remote
		server, then a copy of the same is brought into this 
		local cache machine.""")
	print "File exists? ", client.exists("/usr/python/readme.txt")
    	print client.read("/usr/python/readme.txt")

    	# test append, read after append
    	print "\nAppending..."
    	client.write_append("/usr/python/readme.txt", \
        	" Appending to a local copy in the client cache.\n")
    	print client.read("/usr/python/readme.txt")

    	# test delete
    	print "\nDeleting..."
    	client.delete("/usr/python/readme.txt")
    	print "File exists? ", client.exists("/usr/python/readme.txt")

    	# test exceptions
    	print "\nTesting Exceptions..."
    	try:
        	client.read("/usr/python/readme.txt")
    	except Exception as e:
        	print "This exception should be thrown:", e
    	try:
        	client.write_append("/usr/python/readme.txt", "foo")
    	except Exception as e:
        	print "This exception should be thrown:", e

    	# show logs
    	print "\nVenus logs..."
    	print client.print_logs()
