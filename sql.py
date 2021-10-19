from google.cloud import pubsub_v1
import time
import mysql.connector
import datetime
import numpy as np
import threading
import json
import sys
from mysql.connector.errors import OperationalError, ProgrammingError, InterfaceError
def sql_exec(lst,query,start_point,subscription_id):
    with open("lright.json") as f1:
        data = json.load(f1)
    try:
        mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"], 
    ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
        #mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_998',database='db_prod')
        my_cursor = mydb.cursor()
        my_cursor.executemany(query,lst)
        mydb.commit()
        cursor = mydb.cursor()
        #s = "insert into dummy_telemetry (number_of_messages) values (" + str(len(lst)) + ")"
        a = "subscribe"
        s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('" + a + "' ,'" + str(subscription_id) + "', '" + str(start_point) + "', '" + str(datetime.datetime.now()) + "', " + str(len(lst)) + ")"
        cursor.execute(s)
        mydb.commit()
        mydb.close()
    except OperationalError as e:
        try:
            cnx.reconnect(attempts=3, delay=30.0)
            my_cursor = mydb.cursor()
            my_cursor.executemany(query,lst)
            mydb.commit()
            cursor = mydb.cursor()
            a = "subscribe"
            s = "insert into pubsub_log (Task,Topic_name,start_time,end_time,message_count) values ('" + a + "' ,'" + str(subscription_id) + "', '" + str(start_time) + "', '" + str(datetime.datetime.now()) + "', " + str(len(lst)) + ")"
            cursor.execute(s)
            mydb.commit()
        except InterfaceError as e:
            print("data failed to insert. The MYSQL database must be down. Try again after a few hours")
            textfile = open("failed_data.txt", "w")
            for element in lst:
                textfile.write(element + "\n")
            textfile.close()
        #f = open("telemetry.txt", "w")
        #f.write(str(e))
    #sys.exit(1)
    except ProgrammingError as e:
        print("data failed to insert. Check the schema of the data and the mysql connection parameters")
        textfile = open("failed_data.txt", "w")
        for element in lst:
            textfile.write(element + "\n")
        textfile.close()

#if __name__ == "__main__":
    #lst = []
    #query = ""
    #start_point=datetime.datetime.now()
    #subscription_id = "a"
    #sql_exec(lst,query,start_point,subscription_id)
    