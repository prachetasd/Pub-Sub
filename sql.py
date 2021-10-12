from google.cloud import pubsub_v1
import time
import mysql.connector
import datetime
import numpy as np
import threading
import json
from mysql.connector.errors import OperationalError, ProgrammingError
def sql_exec(lst,query):
    try:
        mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_1998',database='db_prod',connect_timeout=28800)
        my_cursor = mydb.cursor()
        my_cursor.executemany(query,lst)
        mydb.commit()
        cursor = mydb.cursor()
        s = "insert into telemetry (number_of_messages) values (" + str(len(lst)) + ")"
        cursor.execute(s)
        mydb.commit()
        mydb.close()
    except ProgrammingError as e:
        f = open("telemetry.txt", "w")
        f.write(str(e))
    