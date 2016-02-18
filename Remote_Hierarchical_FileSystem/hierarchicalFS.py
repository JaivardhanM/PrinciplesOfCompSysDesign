#!/usr/bin/env python

import logging

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str

#************************************************************************
# Name:      read_metdat
# arguments: Cur_dict - refernce point to start with
#            path - path of file/directory whose dictionary is required.
# return:    Cur_dict - dictionary held by required file/folder
# func:      This function takes a reference point and path as argument\
#             and returns the dictionary at location pointed by 'path'
#************************************************************************
def read_metdat(Cur_dict, path):

    if path == '/':
	#if the path is root, return Cur_dict which is already located at root
	return Cur_dict

    #extract path elements from given path
    path_elements = path.split('/')
	#delete blank character from the list of path elements
    del path_elements[0]
    
	#loop through the path until the dictionary of required item is reached 
    for element in path_elements:
    #if destination is not present return 0
	if element == path_elements[-1]:
	    if element not in Cur_dict['Sub']:
	        return 0
	#point to dictionary of next element of the path
        Cur_dict = Cur_dict['Sub'][element]
        		
    return Cur_dict

#************************************************************************
# Name:      write_metdat
# arguments: Cur_dict - refernce point to start with
#            path - path of file/directory whose dictionary is to be added.
#            MData - meta data to be written
# return:    None
# func:      This function takes a reference point, path and meta-data as\
#             arguments and write the metadata to the dictionary at\
#             location pointed by 'path'
#************************************************************************
def write_metdat(Cur_dict, path, MData):

    #extract path elements from given path
    path_elements = path.split('/')
    #delete blank character from the list of path elements
    del path_elements[0]

    Cur_dict = Cur_dict['Sub']

    #loop until you reach parent of final element in the path
    if len(path_elements) != 1:
        for element in path_elements[:-1]:
            Cur_dict = Cur_dict[element]['Sub']

    #Write meta data to required path	
    Cur_dict[path_elements[-1]] = MData
    #if the directory is new, add an element which can accomodate sub-directories or files inside it
    if 'Sub' not in Cur_dict[path_elements[-1]]: 
        Cur_dict[path_elements[-1]]['Sub'] = {}	

    return
 
class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self):
        self.files = {}
        self.data = defaultdict(bytes)
        self.fd = 0
        now = time()
        self.files['/'] = dict(st_mode=(S_IFDIR | 0755), st_ctime=now,
                               st_mtime=now, st_atime=now, st_nlink=2)
        self.files['/']['Sub'] = {}

    def chmod(self, path, mode):
        target_dict = read_metdat(self.files['/'], path)
	
        target_dict['st_mode'] &= 0770000
        target_dict['st_mode'] |= mode
        return 0

    def chown(self, path, uid, gid):
        target_dict = read_metdat(self.files['/'], path)

        target_dict['st_uid'] = uid
        target_dict['st_gid'] = gid

    def create(self, path, mode):
	MetaData = dict(st_mode=(S_IFREG | mode), st_nlink=1,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())
	write_metdat(self.files['/'], path, MetaData)

        self.fd += 1
        return self.fd

    def getattr(self, path, fh=None):
        result = read_metdat(self.files['/'], path)
        if result == 0:
            raise FuseOSError(ENOENT)

	return result

    def getxattr(self, path, name, position=0):
        target_dict = read_metdat(self.files['/'], path)
        attrs = target_dict.get('attrs', {})

        try:
            return attrs[name]
        except KeyError:
            return ''       # Should return ENOATTR

    def listxattr(self, path):
        target_dict = read_metdat(self.files['/'], path)
        attrs = target_dict.get('attrs', {})
        return attrs.keys()

    def mkdir(self, path, mode):
        MetaData = dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time())

        write_metdat(self.files['/'], path, MetaData) 
        self.files['/']['st_nlink'] += 1

    def open(self, path, flags):
        self.fd += 1
        return self.fd

    def read(self, path, size, offset, fh):
        target_dict = read_metdat(self.files['/'], path)
        return target_dict['Data'][offset:offset + size]

    def readdir(self, path, fh):
        target_dict = read_metdat(self.files['/'], path)
        return ['.', '..'] + [x for x in target_dict['Sub'] if x != '/']


    def readlink(self, path):
        target_dict = read_metdat(self.files['/'], path)
        return target_dict['Data']

    def removexattr(self, path, name):
        target_dict = read_metdat(self.files['/'], path)
        attrs = target_dict.get('attrs', {})

        try:
            del attrs[name]
        except KeyError:
            pass        # Should return ENOATTR

    def rename(self, old, new):
        new_elements = new.split('/')
        old_elements = old.split('/')
        old = '/'.join(old_elements[:-1])
        target_dict = read_metdat(self.files['/'], old)
        target_dict['Sub'][new_elements[-1]] = target_dict['Sub'].pop(old_elements[-1])

    def rmdir(self, path):
        path_elements = path.split('/')
        path = '/'.join(path_elements[:-1])
        target_dict = read_metdat(self.files['/'], path)
        target_dict['Sub'].pop(path_elements[-1])
        target_dict['st_nlink'] -= 1

    def setxattr(self, path, name, value, options, position=0):
        # Ignore options
        target_dict = read_metdat(self.files['/'], path)
        attrs = target_dict.setdefault('attrs', {})
        attrs[name] = value

    def statfs(self, path):
        return dict(f_bsize=512, f_blocks=4096, f_bavail=2048)

    def symlink(self, target, source):
        MetaData = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source))

        write_metdat(self.files['/'], target, MetaData)
        target_dict = read_metdat(self.files['/'], target)
        target_dict['Data'] = source

    def truncate(self, path, length, fh=None):
        target_dict = read_metdat(self.files['/'], path)
        target_dict['Data'] = target_dict['Data'][:length]
        target_dict['st_size'] = length

    def unlink(self, path):
        path_elements = path.split('/')
        path = '/'.join(path_elements[:-1])
        target_dict = read_metdat(self.files['/'], path)
        target_dict['Sub'].pop(path_elements[-1])

    def utimens(self, path, times=None):
        now = time()
        atime, mtime = times if times else (now, now)
        target_dict = read_metdat(self.files['/'], path)
        target_dict['st_atime'] = atime
        target_dict['st_mtime'] = mtime

    def write(self, path, data, offset, fh):
        target_dict = read_metdat(self.files['/'], path)
        if 'Data' in target_dict == True:
            target_dict['Data'] = target_dict['Data'][:offset] + data
        else:
            target_dict['Data'] = data
        target_dict['st_size'] = len(target_dict['Data'])

        return len(data)


if __name__ == '__main__':
    if len(argv) != 2:
        print('usage: %s <mountpoint>' % argv[0])
        exit(1)

    logging.getLogger().setLevel(logging.DEBUG)
    fuse = FUSE(Memory(), argv[1], foreground=True)
