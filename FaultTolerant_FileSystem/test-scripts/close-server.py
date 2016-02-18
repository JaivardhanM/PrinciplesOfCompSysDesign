import xmlrpclib
from xmlrpclib import Binary

import unittest
import os
import subprocess
from subprocess import check_output
from subprocess import Popen, PIPE
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
import signal
import subprocess
import threading
url="http://localhost:"+sys.argv[1]

sh = xmlrpclib.Server(url)
print "terminated"
sh.terminate()
