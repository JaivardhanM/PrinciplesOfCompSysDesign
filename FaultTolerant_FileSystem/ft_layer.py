#!/usr/bin/env python
import logging
from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from sys import argv, exit
from time import time,sleep
import datetime
from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
from xmlrpclib import Binary
import sys, pickle, xmlrpclib
import signal

from fuse import FUSE, FuseOSError, Operations, LoggingMixIn
import hashlib

QR =1
QW =1
ping_time=0.1
def put_fault_handler(fservers,path,key,value):
    #handle failed puts
    print "Put fault handler is handling...."
    count = len(fservers)-1
    while count > -1:
        try:
            if fservers[count].put(Binary(key),Binary(pickle.dumps(value)),6000) == True:
                fservers.pop(count)
                count = count -1
        except:
            sleep(ping_time)
            continue
    print "PUT handler successful"
    return None

def update_checksum(meta_url,path,key,pickled_value):
    key = path +key+"&&checksum"
 
    dat_checksum = hashlib.md5(pickled_value).hexdigest()
    #put checksum on the meta server\
    meta_server  = xmlrpclib.Server(meta_url)
    meta_server.put(Binary(key),Binary(dat_checksum),6000)


def validate_checksum(meta_url,data_servers,path,key,rdata):
    fkey = path + key+"&&checksum"
    key = path + "&&" + key
    meta_server  = xmlrpclib.Server(meta_url)
    valid_checksum = meta_server.get(Binary(fkey))
    print valid_checksum, path, fkey
    valid_checksum = valid_checksum["value"].data
    print valid_checksum

    # checks if any server has corrupted data
    count = 0
    isCorrupted  = False
    print rdata
    no_valid_data =0
    failed_server_ids=[]
    good_server_ids=[]
    for ndat in rdata:
        curr_checksum = hashlib.md5(ndat).hexdigest()
        
        print "server ",count," ",curr_checksum
        print "data ",pickle.dumps(ndat)
        # if not valid data append the failed server id
        if valid_checksum != curr_checksum:
            print "valid_checksum"
            print "Data server",count," is corrupted"
            failed_server_ids.append(count)
            isCorrupted = True
        # if valid data append good server id
        if valid_checksum == curr_checksum:
            print "date_server ", count," is good"
            good_server_ids.append(count)
            no_valid_data =no_valid_data +1
        
        count = count +1
    global QR
    if len(good_server_ids) == 0:
        print len(good_server_ids) ," < ", QR
        print "Corrupted data on all the servers"
        return []  
        
    good_server_id = good_server_ids[0]
    good_data = rdata[good_server_id]
    
    # correct the corrupted         
    if isCorrupted:
        #correct the data on each of the corrupted servers  
        for fs_id in failed_server_ids:
            dh = xmlrpclib.Server(data_servers[fs_id])
            try:
                dh.put(Binary(key),Binary(good_data),6000)
            except:
                print "Server Down"
            
    return  pickle.loads(good_data)

class ReliableLayer:
    def __init__(self,qr,qw,urls):        
        self.urls = urls        
        self.meta_url = urls[0]
        self.data_urls = urls[1:]
        global QR
        global QW
        QR=qr
        QW=qw
        self.Qw = qr;
        self.Qr = qw;

        # getting meta server handlers
        try:
          self.meta_hdl = xmlrpclib.Server(self.meta_url)
        except:
          print "couldn't connect to meta server"
        # setting up data server handlers
        self.data_hdls = []
        failed_servers = []        


    def reliable_put(self,path,key,value):
        pickled_value = pickle.dumps(value)
        if key == "meta" or key == "list_nodes":
            key = path +"&&" + key
            self.meta_hdl = xmlrpclib.Server(self.meta_url)
            self.meta_hdl.put(Binary(key),Binary(pickled_value),6000)
        else:           
            update_checksum(self.meta_url,path,key,pickled_value)
            count = 0
            #contains handlers for failed puts
            failed_server_ids = []
            live_server_ids = []
            key = path + "&&" + key
            check = False
            server_id = 0
            global QW
         
            for url in self.data_urls:
                isConnected = False
                no_tries = 0
                while isConnected == False and no_tries < 5:
                    no_tries = no_tries+1
                    try:
                        # try to connect to the server
                        dh = xmlrpclib.Server(url)
                        dh.put(Binary(key),Binary(pickled_value),6000)
                        isConnected = True
                        live_server_ids.append(server_id)
                        server_id = server_id + 1
                    except:
                        #print "appending fault servers list"
                        sleep(ping_time)
                        if no_tries > 4:
                            isConnected=True
                            failed_server_ids.append(server_id)
                            server_id = server_id + 1 
                        print "Trying to reconnect to the server",server_id
                    
            if len(live_server_ids) < QW:
                print "Failed to put in the ",live_server_ids ," servers"
                
    def reliable_get(self,path,key):    
        
        if key == "meta" or key == "list_nodes":
            key = path+"&&"+key
            self.meta_hdl = xmlrpclib.Server(self.meta_url)
            res = self.meta_hdl.get(Binary(key))
            if "value" in res:
                return pickle.loads(res["value"].data)
            else:
                return None

        else:
            tkey = key
            key = path +"&&" + key
            
            #Append data recieved from server to rdata
            rdata = []
            isConnected = False
            s_id = 0
            g_count =0
            
            for url in self.data_urls:
                isConnected = False
                no_tries=0
                while isConnected == False and no_tries < 5:
                    try:
                        # try to connect to the server
                        dh = xmlrpclib.Server(url)
                        res = dh.get(Binary(key))
                        g_count = g_count+1
                        # append to rdata
                        if "value" in res:
                            rdata.append(res["value"].data)
                        else:
                            rdata.append(pickle.dumps(res))
                        isConnected = True
                    except:
                        #print "appending fault servers list"
                        sleep(ping_time)
                        no_tries=no_tries+1
                        if no_tries > 4:
                            isConnected=True
                            rdata.append(pickle.dumps("corrupted"))
                        print "Trying to reconnect to the server",s_id
                s_id =s_id+1
         
            print "R__DATA -> ", rdata
            if g_count < QR:
                return None

            # validate checksum
            result  =validate_checksum(self.meta_url,self.data_urls,path,tkey,rdata)

            return result
           
