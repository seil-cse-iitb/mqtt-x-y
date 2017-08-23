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


class writeMongoThread(multiprocessing.Process):
    def __init__(self,threadID,qname,params):
        super(writeMongoThread, self).__init__()
        self.threadID=threadID
        self.qname=qname
        self.params = params
        self.exit = multiprocessing.Event()
        self.meter_info = {}
        self.mongo_Client = {}

    def stop(self):
        self.exit.set()

    def get_cols(self,topic):
        sensor_type_url = "http://10.129.23.65:9999/json_sensor_type_details/"
        t = topic.split("/")
        req = Request(sensor_type_url + t[0] + "/" + t[2] + "/" )
        col = []
        try:
            resp = urlopen(req)
            d = resp.read()
            j = json.loads(d)
            #print len(j[0])
            for n in range(len(j[0])):
                for rec in j[0]:
                    if n + 1 == int(rec["field_number"]):
                        #print e["name"]
                        col.append(rec["name"])
            #print j
        except URLError as e:
            print(e)
        return col
        
    def get_table_info (self,topic):
        sensor_url = "http://10.129.23.65:9999/json_sensor/"
        print(sensor_url + topic)
        req = Request(sensor_url + topic)
        try:
            resp = urlopen(req)
            d = resp.read()
            #print(d)
            j = json.loads(d)
            table = j[0][0]['sensor_collection_name']
            db = j[0][1]['database_name_1']
            db_ip = j[0][1]['database_ip_1']
            port = j[0][1]['database_port_1']
            #print(table,db,db_ip,port)
            return [table,db,db_ip,port]
        except URLError as e :
            print(e)
        
    def post_next_queue_message(self):
        #print("----------------------------------------")
        try:
            data = self.qname.get(timeout=0.1)
            #print("received",data[0] )
            q_size = self.qname.qsize()
            if q_size % 10 == 9:
                print "-" , q_size
            if data[0] not in self.meter_info:
                col = self.get_cols(data[0])
                t = self.get_table_info(data[0])
                t.append(col)
                self.meter_info[data[0]] = t


            paramNames =  self.meter_info[data[0]][4]
            db_ip = self.meter_info[data[0]][2]
            tab = self.meter_info[data[0]][0]
            db = self.meter_info[data[0]][1]
            dataDict = {}
            #print data[2]
            dataDict["TS_RECV"] = data[2]
            fields = str(data[1]).split(',')
            for i in range(len(fields)):
                dataDict[paramNames[i]]= float(fields[i])
            #print dataDict
            if db_ip not in self.mongo_Client:
                self.mongo_Client[db_ip] = MongoClient(db_ip, 27017,w=1,j=False)
            #meter_list = {}
            #for x in mongo_Client.smartmeters.kresit_meters.find():
            #        meter_list[int(x['id'])] = x['collection']
            #print meter_list
            #data = [mongo_Client,meter_list]    
            db_table =  self.mongo_Client[db_ip][db][tab]
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