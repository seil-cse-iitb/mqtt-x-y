#import numpy as np
import sqlite3 as db
import argparse
import paho.mqtt.client as mqtt
import os
import time 

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
    query  = "UPDATE params set value =? where id = ?"
    
    #print("query is: ", query )
    cur.execute(query,(val,id))
    conn.commit()
    conn.close()

def sm_join(source1,source2, dest,col_update,task,join_table):
    #print source 
    #cols =["ID","TS_count","A","A1","A2","A3","PF","PF1","PF2","PF3","V1","V2","V3","V12","V23","V31","W","W1","W2","W3","VA","VA1","VA2","VA3","VAR","VAR1","VAR3","VAR2","F","VLL","VLN","FwdWh","FwdVARh","FwdVAh","FwdVARh1"]
    
    try:
        #data = pd.read_table(open(source,'r'),sep=',',names=cols)
        d1 = pd.read_table(open(source1,'r'),sep=',')
        d2 = pd.read_table(open(source2,'r'),sep=',')
        path = '\\'.join(dest.split('\\')[0:-1])
        if not os.path.exists(path):
            os.makedirs(path) 
        if col_update >= 2:
            print "At 2" , join_table
            d2.columns = ['TS'] + [join_table + "." + x for x in d2.columns[1:]]
        if col_update >= 1:
            print "At 1" , task
            d1.columns = ['TS'] + [task + "." + x for x in d1.columns[1:]]
        #print d1.columns , d2.columns
        print d1.shape,d2.shape
        data = res = pd.merge(d1, d2, on = 'TS', how='outer')  
        #count_hour = data.groupby(['T'], as_index=False).count() 
        #print data["T"]
        data.to_csv(dest, index=False,  sep = ',',float_format= '%.3f')
        if col_update == 1:
            os.remove(source2)
        result = "Success"
    except IOError:
        print "File not Found"
        result  = "File not Found"
    return result
   
def on_message(client, userdata, msg):
    #if userdata[1] == 1:
    #    print("Message Received:"+ msg.topic+"$"+str(msg.payload))
        #print("Message Received:", msg)
        #print msg.topic.split('/')[-1:] ,time.time() ,  msg.payload
    t_local = time.localtime()
    dt = time.strftime("_%Y-%m-%d", t_local)
    yyyy = time.strftime("%Y", t_local)
    mm = time.strftime("%m", t_local)
    dd = time.strftime("%d", t_local)
    path = userdata['folder'][1]
    path = path.replace("<YYYY>",yyyy)
    path = path.replace("<MM>",mm)
    path = path.replace("<DD>",dd)
    dest = path + msg.topic + dt + '.csv'
    path = '/'.join(dest.split('/')[0:-1])
    if not os.path.exists(path):
            os.makedirs(path)
    fo = open(dest,'a')
    data = msg.topic.split('/')[-1:][0] + ',' + str(time.time()) + ',' + msg.payload + '\n'
    fo.write(data)
    fo.close()
 
def run_processes(db_string,params):
    
    data = [params['folder'][1],int(params['output'][1])]
    client = mqtt.Client(params['client_id'][1],userdata = params,protocol=mqtt.MQTTv311,clean_session=False)
    #client.on_connect = on_connect
    client.on_message = on_message
    #client.on_subscribe = on_subscribe
    #client.on_unsubscribe = on_unsubscribe
    #client.on_log = on_log
    client.abcd = False
    client.username_pw_set(params['user_id'][1], params['password'][1])
    Connected = False
    while Connected != True:
        print "Try to connect"
        try:
            client.connect(params['server_ip'][1], int(params['server_port'][1]), 600)
            Connected = True
        except IOError:
            print "Could not Connect"
    print "Subscribing to:" , ":" + params['subscription'][1] + ":"
    client.subscribe("#",qos=1)

    # Blocking call that processes network traffic, dispatches callbacks and
    # handles reconnecting.
    # Other loop*() functions are available that give a threaded interface and a
    # manual interface.
    #client.loop_forever()
    client.loop_start()
    print "Process Started"
    valid =False
    validResponse = 'exit'
    while (valid == False):
        print "Type command (exit) to exit."
        userInput = raw_input(">>> ")
        if (userInput == validResponse):
            valid = True
        if (userInput == ''):
            data = [params['folder'][1],0]
            client.user_data_set(data)
    client.loop_stop(force=False)
    
def main(args):
    #print(args)
    
    db_name = "mqtt_csv_logger.sqlite"
    g_params = getParams(db_name)
	

    parser = argparse.ArgumentParser(description='Smart Meter data selection')
    group1 = parser.add_mutually_exclusive_group(required=False)
    group1.add_argument('-l', action="store_true", dest="list", help="List parameters")
    group1.add_argument('-r', action="store_true", dest="run", help="Run the program")
    group1.add_argument('-s', action="store", dest="set", help="set <parameter>=<value>")

    arguments = parser.parse_args()
    #print arguments
    
    if arguments.run:
        print "running !!!"
        run_processes(db_name,g_params)

    if arguments.list:
        for key in g_params:
            print  '%-15s%-30s%-30s' % (key ,  ":" + g_params[key][1],"  (" + g_params[key][0] +")") 

    if arguments.set is not None:
        param_val = arguments.set.split("=")
        if len(param_val) != 2:
            print "need <parameter>=<value> "
        else:
            setParam(db_name,param_val[0],param_val[1])
            g_params = getParams(db_name)
            for key in g_params:
                print  '%-15s%-30s%-30s' % (key ,  ":" + g_params[key][1],"  (" + g_params[key][0] +")") 
        
if __name__ == '__main__':
    import sys
    main(sys.argv[1:]) 
	