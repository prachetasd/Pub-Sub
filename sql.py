from google.cloud import pubsub_v1
import time
import mysql.connector
import datetime
import numpy as np
import threading
from mysql.connector.errors import OperationalError, ProgrammingError
def sql_exec(lst,query):
    mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_1998',database='db_prod',connect_timeout=28800)
    my_cursor = mydb.cursor()
    my_cursor.executemany(query,lst)
    mydb.commit()
    mydb.close()
    