#import numpy as np
import sqlite3 as db
import argparse
import paho.mqtt.client as mqtt
import os
import time 
from pymongo import MongoClient
import multiprocessing 
import writeMongoThread

def getParams(db_string):
    conn = db.connect(db_string)
    cur = conn.cursor()
    query  = "SELECT id, name, value from params"
    
    #print("query is: ", query )
    cur.execute(query)
    rows = cur.fetchall()
    params = {}
    if  len(rows) > 0:
        for row in rows:
            params[row[0].rstrip()] = [row[1].rstrip(),row[2].rstrip()]
    conn.close()
    return params 

def setParam(db_string,id,val):
    conn = db.connect(db_string)
    cur = conn.cursor()
    query  = "UPDATE params set value = ? where id = ?"
    
    #print("query is: ", query )
    cur.execute(query,(val,id))
    conn.commit()
    conn.close()
   
def on_message(client, user_data, msg):
    #print "Received",msg.topic
    #print(msg.topic)
    user_data.put([msg.topic,msg.payload,time.time()])
    q_size = user_data.qsize()
    if q_size % 10 == 9:
        print "+", q_size 
    #if userdata[1] == 1:
    #print("Message Received:"+ msg.topic)
    #print msg.topic.split('/')[-1]
    #print("Message Received:", msg)
    #print msg.topic.split('/')[-1:] ,time.time() ,  msg.payload
    #paramNames =  ['srl','Timestamp','VA','W','VAR','PF','VLL','VLN','A','F','VA1','W1','VAR1','PF1','V12','V1','A1','VA2','W2'\
    #                        ,'VAR2','PF2','V23','V2','A2','VA3','W3','VAR3','PF3','V31','V3','A3','FwdVAh','FwdWh','FwdVARh','FwdVARh']

    #dataDict = {}
    #fields = str(msg.payload).split(',')
    #for i in range(len(fields)):
    #    dataDict[paramNames[i]]= float(fields[i])
    #print dataDict
    
    #user_data[0].smartmeters[user_data[1][int(msg.topic.split('/')[-1])]].insert(dataDict)
    #dest = userdata[0] + msg.topic + dt + '.csv'
    #path = '/'.join(dest.split('/')[0:-1])
    #if not os.path.exists(path):
    #        os.makedirs(path)
    #fo = open(dest,'a')
    #data = msg.topic.split('/')[-1:][0] + ',' + str(time.time()) + ',' + msg.payload + '\n'
    #fo.write(data)
    #fo.close()
 
def run_processes(db_string,params):

    q = multiprocessing.Queue()

    mongoProcess = writeMongoThread.writeMongoThread("writeMongo",q,params)

    mongoProcess.start()    
    #mongo_Client = MongoClient(params['mongo_server_ip'][1], 27017)
    #meter_list = {}
    #for x in mongo_Client.smartmeters.kresit_meters.find():
    #        meter_list[int(x['id'])] = x['collection']
    #print meter_list
    #data = [mongo_Client,meter_list]
    client = mqtt.Client(params['client_id'][1],userdata = q,protocol=mqtt.MQTTv311,clean_session=False)
    #client.on_connect = on_connect
    client.on_message = on_message
    #client.on_subscribe = on_subscribe
    #client.on_unsubscribe = on_unsubscribe
    #client.on_log = on_log
    #client.username_pw_set(params['user_id'][1], params['password'][1])
    #client.connect(params['mqtt_server_ip'][1], int(params['mqtt_port'][1]), 600)
    #print "Subscribing to:" , ":" + params['subscription'][1] + ":"
    #client.subscribe(str(params['subscription'][1]),qos=1)
    try:
        Connected = False
        while Connected != True:
            print("Try to connect")
            try:
                client.connect(params['mqtt_server_ip'][1], int(params['mqtt_port'][1]), 600)
                Connected = True
            except IOError:
                print("Could not Connect")
        print("Subscribing to:" , ":" + params['subscription'][1] + ":")
        client.subscribe(str(params['subscription'][1]),qos=1)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    #client.loop_forever()

        client.loop_start()
        print("Process Started")
        valid =False
        validResponse = 'exit'
        while (valid == False):
            print("Type command (exit) to exit.")
            userInput = raw_input(">>> ")
            if (userInput == validResponse):
                valid = True
            if (userInput == ''):
                data = [params['folder'][1],0]
                client.user_data_set(data)
        client.loop_stop(force=False)
    except Exception as err:
        print('*123 ',err)
        print("Error in run")

    mongoProcess.stop()
    mongoProcess.join()      

    
def main(args):
    #print(args)
    
    db_name = "mongo_logger.sqlite"
    g_params = getParams(db_name)
	

    parser = argparse.ArgumentParser(description='Smart Meter data selection')
    group1 = parser.add_mutually_exclusive_group(required=False)
    group1.add_argument('-l', action="store_true", dest="list", help="List parameters")
    group1.add_argument('-r', action="store_true", dest="run", help="Run the program")
    group1.add_argument('-s', action="store", dest="set", help="set <parameter>=<value>")

    arguments = parser.parse_args()
    #print arguments
    
    if arguments.run:
        print("running !!!")
        run_processes(db_name,g_params)

    if arguments.list:
        for key in g_params:
            print('%-20s%-30s%-29s' % (key ,  ":" + g_params[key][1],"  (" + g_params[key][0] +")"))

    if arguments.set is not None:
        param_val = arguments.set.split("=")
        if len(param_val) != 2:
            print("need <parameter>=<value> ")
        else:
            setParam(db_name,param_val[0],param_val[1])
            g_params = getParams(db_name)
            for key in g_params:
                print('%-20s%-30s%-29s' % (key ,  ":" + g_params[key][1],"  (" + g_params[key][0] +")"))
        
if __name__ == '__main__':
    import sys
    main(sys.argv[1:]) 
	