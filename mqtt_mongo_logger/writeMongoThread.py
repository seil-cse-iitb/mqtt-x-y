import time 
import multiprocessing 
import Queue
import struct
import socket
from pymongo import MongoClient
import pymongo
import os
import gzip
from urllib2 import Request,URLError, urlopen
import json

# DATABASE_URL = "http://10.129.23.22/meta/database/"
# SENSOR_LOOKUP_URL = "http://10.129.23.22/meta/sensor_lookup/"
# CHANNEL_URL = "http://10.129.23.22/meta/channel/"
DATABASE_URL = "http://10.129.23.41:8080/meta/database/"
SENSOR_LOOKUP_URL = "http://10.129.23.41:8080/meta/sensor_lookup/"
CHANNEL_URL = "http://10.129.23.41:8080/meta/channel/"


class writeMongoThread(multiprocessing.Process):
    def __init__(self,threadID,qname,params):
        super(writeMongoThread, self).__init__()
        self.threadID=threadID
        self.qname=qname
        self.params = params
        self.exit = multiprocessing.Event()
        self.sensor_info = {}
        self.db_info = {}
        self.channel_info = {}
        self.mongo_Client = {}

    def stop(self):
        self.exit.set()

    def get_database_info(self,database_id):
        if database_id not in self.db_info:
            #print DATABASE_URL + str(database_id)
            req = Request(DATABASE_URL + str(database_id))
            try:
                resp = urlopen(req)
                d = resp.read()
                j = json.loads(d)
                self.db_info[database_id] = j
            except URLError as e:
                print(e)

        return self.db_info[database_id]

    def get_channel_fields(self,channel_id):
        if channel_id not in self.channel_info:
            col = []
            print CHANNEL_URL + str(channel_id)
            req = Request(CHANNEL_URL + str(channel_id) )
            try:
                resp = urlopen(req)
                d = resp.read()
                j = json.loads(d)
                #print j
                # print len(j[0])
                for n in range(len(j)):
                    #print n
                    for rec in j:
                        #print rec
                        if n + 1 == int(rec["field_number"]):
                            #print rec["field_name"]
                            col.append(rec["field_name"])
                self.channel_info[channel_id] = col
            except URLError as e:
                print(e)
        return self.channel_info[channel_id]


    def sensor_lookup(self,topic):
        sensor_id = "/".join(topic)

        if sensor_id not in self.sensor_info:
            #print SENSOR_LOOKUP_URL + sensor_id
            req = Request(SENSOR_LOOKUP_URL + sensor_id  )
            try:
                resp = urlopen(req)
                d = resp.read()
                j = json.loads(d)
                #print j
                if "error" in j:
                    print "Query failed for " , sensor_id
                    return None
                else:
                    self.sensor_info[sensor_id] = j
            except URLError as e:
                print(e)
        return self.sensor_info[sensor_id]


    def get_table_info(self,topic):
        sensor_info = self.sensor_lookup(topic)
        #print sensor_info['database'], sensor_info
        if sensor_info is None:
            return None
        db_info =  self.get_database_info(sensor_info['database'])
        table = sensor_info['sensor_id']
        channel = sensor_info['channel']
        collection = db_info['schema']
        db_ip = db_info['ip']
        port = db_info['port']
        #print(table,collection,db_ip,port)
        return (channel,table,collection,db_ip,port)

    def post_next_queue_message(self):
        #print("----------------------------------------")
        try:
            data = self.qname.get(timeout=0.1)
            #print("received",data[0] )
            q_size = self.qname.qsize()
            if q_size % 10 == 9:
                print "-" , q_size

            (channel,table,collection,db_ip,port) = self.get_table_info(data[0].split("/"))

            col = self.get_channel_fields(channel)

            #print data[0]
            dataDict = {}
            #print data[2]
            dataDict["TS_RECV"] = data[2]
            fields = str(data[1]).split(',')
            for i in range(len(col)):
                dataDict[col[i]]= float(fields[i])
            #print dataDict
            reconnect = False
            if db_ip not in self.mongo_Client:
                reconnect = True
            elif self.mongo_Client[db_ip] is None:
                reconnect = True
            if reconnect: 
                self.mongo_Client[db_ip] = MongoClient(db_ip, 27017,w=1,j=False)
            db_table =  self.mongo_Client[db_ip][collection][table]
            try:
                db_table.insert_one(dataDict)
            except pymongo.errors.DuplicateKeyError, e:
                print e

            #print "Logged", data[0]
        except Queue.Empty:
            #print "Error at end"
            None
        except Exception as err:
            print('*',err)
            print("Error in run 2")
            self.mongo_Client[db_ip] = None
            
    def run(self):
        print("Starting Mongo thread ",self.threadID)
        msg_to_send = []
        while not self.exit.is_set() or self.qname.qsize() > 0 :
                               
            try:
                self.post_next_queue_message()
            except Exception as err:
                print('*',err)
                print("Error in run 1")
        #for conn in self.mongo_Client:
        #    self.mongo_Client[conn].disconnect()