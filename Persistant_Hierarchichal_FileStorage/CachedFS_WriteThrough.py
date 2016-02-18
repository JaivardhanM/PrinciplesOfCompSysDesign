#!/usr/bin/env python
import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time
from time import time
import datetime
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib

import pymongo
from pymongo import MongoClient
import memcache


#client = MongoClient('mongodb://localhost:27017/')
#fs_db = client.filesys_database
#fnodes = fs_db.filenodes.remove()
#fnodes = fs_db.filenodes



#$$$$$$$$$$$$$$$Need to remove below Line ###%%%%%%%%%%%%%%%%%%%%%###################
fnodes = MongoClient('mongodb://localhost:27017/').filesys_database.filenodes.remove()


count = 0
Max_CacheSize = 10
Cache_cnt = 0
Cache_Files = []
Cache_Index = 0

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn

if not hasattr(__builtins__, 'bytes'):
    bytes = str


class FileNode:
    def __init__(self,name,isFile,path,url):
        self.name = name
        self.path = path
        self.url = url    # 'mongodb://localhost:27017/'
        self.isFile = isFile # true if node is a file, false if is a directory.
        #if #$%^&*(#%^&()#$%&*()_@#$%^&*()@#$%^&*(W#$%^&*()@#$%^&*(@#$%^&*WERTYUIOSDFGHJ@#$%^&UICVB
        self.put("data","") # used if it is a file
        self.put("meta",{})
        self.put("list_nodes",{})# contains a tuple of <name:FileNode>  used only if it is a dir. 
        Cache_cnt = 0

    def put(self,key,value):
        self.C_put(key,value)
        self.db_put(key,value)
        
    def get(self,key): 
        return self.C_get(key)
        
    def C_put(self,key,value):
        hostname="127.0.0.1" 
        port="11211"
        hostname = "%s:%s" % (hostname, port)
        server = memcache.Client([hostname])
        #Cache_entry = {"key":str(key), "value" : pickle.dumps(value)}
        C_key = str(self.path)
        global Cache_Files 
        global Cache_cnt 
       
        print "Checking if entry is presetn","[ C_key",C_key, "][ Cache_Files", Cache_Files,"][ Value", (C_key in Cache_Files),"]"
        if C_key in Cache_Files:
            Cache_fetch = server.get(C_key)
            print "/n printing [Cache_fetch:", Cache_fetch, "]"
            Cache_fetch[key] = value
            server.replace(C_key, Cache_fetch, 900)
            Cache_Index = Cache_Files.index(C_key)
            Cache_Files = Cache_Files[:Cache_Index]+Cache_Files[Cache_Index+1:]
            Cache_Files = [C_key]+Cache_Files
        else:
            if Cache_cnt == Max_CacheSize:
                print "When Cache is Full, [Cache_Files:", Cache_Files, "]"
                #Cache_fetch = server.get(Cache_Files[-1])
                #print "[Cache_Files[-1]:", Cache_Files[-1], "]....[Cache_fetch:", Cache_fetch, "]"
                #print "[Cache_fetch.keys():", Cache_fetch.keys(), "]"
                #for key_type in Cache_fetch.keys():
                    #print "[Key_TYPE:", key_type, "]"
                    #self.db_put(Cache_Files[-1],key_type,Cache_fetch[key_type])
                    
                server.delete(Cache_Files[-1])
                Cache_Files = Cache_Files[:-1]
                print "After deleting last item,", Cache_Files
                
            Cache_entry = {key: value}
            print "While writing to Cache: [C_key = ", C_key, "][ Cache_entry = ", Cache_entry, "][key = ", key, "][value = ", value, "]"
            server.set(C_key, Cache_entry, 900)
            print "Cache array",Cache_Files, C_key
            Cache_Files = [C_key]+Cache_Files
            #Cache_Index = (Cache_Index+1)%10
            if Cache_cnt < Max_CacheSize: 
                Cache_cnt = Cache_cnt+1
                    
    def C_get(self,key): 
        hostname="127.0.0.1" 
        port="11211"
        hostname = "%s:%s" % (hostname, port)
        server = memcache.Client([hostname])
        C_key = str(self.path)
        global Cache_Files
        global Cache_cnt
        
        #Cache_fetch = server.get(C_key)
        #print "/n [Cache-FILES:", Cache_Files, "] [Cache_fetch:", Cache_fetch,"]"
        if C_key in Cache_Files:
            print C_key, " present in Cache"
            Cache_Index = Cache_Files.index(C_key)
            Cache_Files = Cache_Files[:Cache_Index]+Cache_Files[Cache_Index+1:]
            Cache_Files = [C_key]+Cache_Files
            print "/n [Printing from Cache:", server.get(C_key), "][C_key:", C_key, "][Key:", key,"]"
            return (server.get(C_key)[key])
        else:
            print C_key, " not present in Cache"
            res = self.db_get(key)
            print "[Key:", key, "]  [PRINTING RES:", res,"]"
            print "[RES.KEYS():", res.keys(), "]"
            if key in res.keys():
                Cache_entry = {"meta":pickle.loads(res["meta"]), "data": pickle.loads(res["data"]), "list_nodes": pickle.loads(res["list_nodes"])}
                server.set(C_key, Cache_entry, 900)
                #print "KEY IS IN RES"
                #self.C_put("meta", pickle.loads(res["meta"]))
                #print "META WRITTEN"
                #self.C_put("data", pickle.loads(res["data"]))
                #print "DATA WRITTEN"
                #self.C_put("list_nodes", pickle.loads(res["list_nodes"]))
                print "LIST WRITTEN"
                print "Check if Stored properly:", server.get(C_key)
                return pickle.loads(res[key])
            else:
                print 
                return None
                        
            
    def db_put(self,key,value):
        client = MongoClient(self.url)
        fs_db = client.filesys_database
        fnodes = fs_db.filenodes
        #dict_temp = {str(self.path) : key , str(key): pickle.dumps(value)}
        Node_id = fnodes.update({str(self.path) : key},{'$set': {str(key): pickle.dumps(value)}},upsert = True)
        print Node_id , "dbug node id"
        
    def db_get(self,key):
        client = MongoClient(self.url)
        fs_db = client.filesys_database
        fnodes = fs_db.filenodes
        return fnodes.find_one({str(self.path):key})
        print "get key", {str(self.path):key}
        print "PRINTING RES", res, "KEY", key, "RES.KEYS()", res.keys()
        if key in res.keys():
            #print "rv: = 1", pickle.loads(res[key])
            if ret_type == 1:
                return res
            else:
                return pickle.loads(res[key])
        else:
            return None
        
        
    def set_data(self,data_blob):
        self.put("data",data_blob)
        

    def set_meta(self,meta):
        self.put("meta",meta)

    def get_data(self):
        return self.get("data")

    def get_meta(self):
        return self.get("meta")

    def list_nodes(self):
        print "##Inside List Nodes##", self.get("list_nodes")
        return self.get("list_nodes").values()

    def add_node(self,newnode):
        list_nodes = self.get("list_nodes")
        list_nodes[newnode.name]=newnode
        self.put("list_nodes",list_nodes)

    def contains_node(self,name): # returns node object if it exists
        if (self.isFile==True):
            return None
        else:
            if name in self.get("list_nodes").keys():
                print "List Nodes dict", self.get("list_nodes")
                return self.get("list_nodes")[name]
            else:
                return None


class FS:
    def __init__(self,url):
        self.url = url
        self.root = FileNode('/',False,'/',url)
        now = time()
        self.fd = 0
        #if@#$%^&*()!@#$%^&*()!@#$%^&*()!@#$%^&*()!@#$%^&*()
        self.root.set_meta(dict(st_mode=(S_IFDIR | 0755), st_ctime=now,st_mtime=now,\
                                         st_atime=now, st_nlink=2))
    # returns the desired FileNode object
    def get_node_wrapper(self,path): # pathname of the file being probed.
        # Handle special case for root node
        if path == '/':
            return self.root
        PATH = path.split('/') # break pathname into a list of components
        name = PATH[-1]
        PATH[0]='/' # splitting of a '/' leading string yields "" in first slot.
        return self.get_node(self.root,PATH,name) 


    def get_node(self,parent,PATH,name):
        next_node = parent.contains_node(PATH[1])
        if (next_node == None or next_node.name == name):
            return next_node
        else:
            return self.get_node(next_node,PATH[1:],name)

    def get_parent_node(self,path):
        parent_path = "/"+("/".join(path.split('/')[1:-1]))
        parent_node = self.get_node_wrapper(parent_path)
        return parent_node

    def add_node(self,node,path):
        parent_path = "/"+("/".join(path.split('/')[1:-1]))
        parent_node = self.get_node_wrapper(parent_path)
        parent_node.add_node(node)
        if (not node.isFile):
            meta = parent_node.get("meta")
            meta['st_nlink']+=1
            parent_node.put("meta",meta)
        else:
            self.fd+=1
            return self.fd

    def add_dir(self,path,mode):
        # create a file node
        temp_node = FileNode(path.split('/')[-1],False,path,self.url)
        temp_node.set_meta(dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                                st_size=0, st_ctime=time(), st_mtime=time(),
                                st_atime=time()))
        # Add node to the FS
        self.add_node(temp_node,path)
  

    def add_file(self,path,mode):
        # create a file node
        temp_node = FileNode(path.split('/')[-1],True,path,self.url)
        temp_node.set_meta(dict(st_mode=(S_IFREG | mode), st_nlink=1,
        st_size=0, st_ctime=time(), st_mtime=time(),
        st_atime=time()))
        # Add node to the FS
        # before we do that, we have to manipulate the path string to point
        self.add_node(temp_node,path)
        self.fd+=1
        return self.fd

    def write_file(self,path,data=None, offset=0, fh=None):
        # file will already have been created before this call
        # get the corresponding file node
        filenode = self.get_node_wrapper(path)
        # if data == None, this is just a truncate request,using offset as 
        # truncation parameter equivalent to length
        node_data = filenode.get("data")
        node_meta = filenode.get("meta")
        if (data==None):
            node_data = node_data[:offset]
            node_meta['st_size'] = offset
        else:
            node_data = node_data[:offset]+data
            node_meta['st_size'] = len(node_data)
        filenode.put("data",node_data)
        filenode.put("meta",node_meta)
        

    def read_file(self,path,offset=0,size=None):
        # get file node
        filenode = self.get_node_wrapper(path)
        # if size==None, this is a readLink request
        if (size==None):
            return filenode.get_data()
        else:
            # return requested portion data
            return filenode.get("data")[offset:offset + size]

    def rename_node(self,old,new):
        # first check if parent exists i.e. destination path is valid
        future_parent_node = self.get_parent_node(new)
        if (future_parent_node == None):
            raise  FuseOSError(ENOENT)
            return
        # get old filenodeobject and its parent filenode object
        filenode = self.get_node_wrapper(old)
        parent_filenode = self.get_parent_node(old)
        # remove node from parent
        list_nodes = parent_filenode.get("list_nodes")
        del list_nodes[filenode.name]
        parent_filenode.put("list_nodes",list_nodes)
        # if filenode is a directory decrement 'st_link' of parent
        if (not filenode.isFile):
            parent_meta = parent_filenode.get("meta")
            parent_meta["st_nlink"]-=1
            parent_filenode.put("meta",parent_meta)
        # add filenode to new parent, also change the name
        filenode.name = new.split('/')[-1]
        future_parent_node.add_node(filenode)

    def utimens(self,path,times):
        filenode = self.get_node_wrapper(path)
        now = time()
        atime, mtime = times if times else (now, now)
        meta = filenode.get("meta")
        meta['st_atime'] = atime
        meta['st_mtime'] = mtime
        filenode.put("meta",meta)


    def delete_node(self,path):
        # get parent node
        parent_filenode = self.get_parent_node(path)
        # get node to be deleted
        filenode = self.get_node_wrapper(path)
        # remove node from parents list
        list_nodes = parent_filenode.get("list_nodes")
        del list_nodes[filenode.name]
        parent_filenode.put("list_nodes",list_nodes)
        # if its a dir reduce 'st_nlink' in parent
        if (not filenode.isFile):
            parents_meta = parent_filenode.get("meta")
            parents_meta["st_nlink"]-=1
            parent_filenode.put("meta",parents_meta)

    def link_nodes(self,target,source):
        # create a new target node.
        temp_node = FileNode(target.split('/')[-1],True,target,self.url)
        temp_node.set_meta(dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
                                  st_size=len(source)))
        temp_node.set_data(source)
        # add the new node to FS
        self.add_node(temp_node,target)

    def update_meta(self,path,mode=None,uid=None,gid=None):
        # get the desired filenode.
        filenode = self.get_node_wrapper(path)
        # if chmod request
        meta = filenode.get("meta")
        if (uid==None):
            meta["st_mode"] &= 0770000
            meta["st_mode"] |= mode
        else: # a chown request
            meta['st_uid'] = uid
            meta['st_gid'] = gid
        filenode.put("meta",meta)

class Memory(LoggingMixIn, Operations):
    'Example memory filesystem. Supports only one level of files.'

    def __init__(self,url):
        global count # count is a global variable, can be used inside any function.
        count +=1 # increment count for very method call, to track count of calls made.
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time())) # print the parameters passed to the method as input.(used for debugging)
        print('In function __init__()') #print name of the method called

        self.FS = FS(url)
       
        
       
        
    def getattr(self, path, fh=None):
        global count
        count +=1
        print ("CallCount {} " " Time {} arguments:{} {} {}".format(count,datetime.datetime.now().time(),type(self),path,type(fh)))
        print('In function getattr()')
        
        file_node =  self.FS.get_node_wrapper(path)
        if (file_node == None):
            raise FuseOSError(ENOENT)
        else:
            return file_node.get_meta()


    def readdir(self, path, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function readdir()')

        file_node =  self.FS.get_node_wrapper(path)
        
        print "******8Printing FIle NODE****", file_node
        #xyz = file_node.list_nodes()
        #print " FIlE_NODE>LIST_NODES ", xyz
        print "x.name:"
#        print x.name for x in file_node.list_nodes()
        print"done"
        #m = []
        m = ['.','..']+[x.name for x in file_node.list_nodes()]
        print m
        return m

    def mkdir(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {}" "," "argumnets:" " " "path;{}" "," "mode:{}".format(count,datetime.datetime.now().time(),path,mode))
        print('In function mkdir()')       
        # create a file node
        self.FS.add_dir(path,mode)

    def create(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {} path {} mode {}".format(count,datetime.datetime.now().time(),path,mode))
        print('In function create()')
        
        return self.FS.add_file(path,mode) # returns incremented fd.

    def write(self, path, data, offset, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print ("Path:{}" " " "data:{}" " " "offset:{}" " "  "filehandle{}".format(path,data,offset,fh))
        print('In function write()')
        
        self.FS.write_file(path, data, offset, fh)
        return len(data)

    def open(self, path, flags):
        global count
        count +=1
        print ("CallCount {} " " Time {}" " " "argumnets:" " " "path:{}" "," "flags:{}".format(count,datetime.datetime.now().time(),path,flags))
        print('In function open()')

        self.FS.fd += 1
        return  self.FS.fd 

    def read(self, path, size, offset, fh):
        global count
        count +=1
        print ("CallCount {} " " Time {}" " " "arguments:" " " "path:{}" "," "size:{}" "," "offset:{}" "," "fh:{}".format(count,datetime.datetime.now().time(),path,size,offset,fh))
        print('In function read()')

        return self.FS.read_file(path,offset,size)

    def rename(self, old, new):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function rename()')

        self.FS.rename_node(old,new)

    def utimens(self, path, times=None):
        global count
        count +=1
        print ("CallCount {} " " Time {} Path {}".format(count,datetime.datetime.now().time(),path))
        print('In function utimens()')

        self.FS.utimens(path,times)

    def rmdir(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function rmdir()')

        self.FS.delete_node(path)

    def unlink(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function unlink()')

        self.FS.delete_node(path)

    def symlink(self, target, source):
        global count
        count +=1
        print ("CallCount {} " " Time {}" "," "Target:{}" "," "Source:{}".format(count,datetime.datetime.now().time(),target,source))
        print('In function symlink()')

        self.FS.link_nodes(target,source)

    def readlink(self, path):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function readlink()')
        
        return self.FS.read_file(path)

    def truncate(self, path, length, fh=None):
        global count
        print ("CallCount {} " " Time {}""," "arguments:" "path:{}" "," "length:{}" "," "fh:{}".format(count,datetime.datetime.now().time(),path,length,fh))
        print('In function truncate()')
        
        self.FS.write_file(path,offset=length)

    def chmod(self, path, mode):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function chmod()')

        self.FS.update_meta(path,mode=mode)
        return 0

    def chown(self, path, uid, gid):
        global count
        count +=1
        print ("CallCount {} " " Time {}".format(count,datetime.datetime.now().time()))
        print('In function chown()')

        self.FS.update_meta(path,uid=uid,gid=gid)
        
        
    
        
    
        

   
if __name__ == "__main__":
  if len(argv) != 3:
    print 'usage: %s <mountpoint> <remote hashtable>' % argv[0]
    exit(1)
  url = argv[2]
  # Create a new HtProxy object using the URL specified at the command-line
  fuse = FUSE(Memory(url), argv[1], foreground=True, debug=True)
