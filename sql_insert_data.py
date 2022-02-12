from google.cloud import pubsub_v1
import time
import mysql.connector
import datetime
from mysqlx import DatabaseError
import numpy as np
import threading
import json
import sys
from mysql.connector.errors import OperationalError, ProgrammingError, InterfaceError, IntegrityError, DatabaseError
def sql_exec(lst,query,data,subscription_id):
    try:
        #lst2 = []
        the_lst = []
        the_lst = lst
        mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"],db = data["mysql"]["db"])
        #mydb=mysql.connector.connect(host='localhost',user='root',password='Genius_998',database='db_prod')
        #print(query)
        my_cursor = mydb.cursor()
        my_cursor.executemany(query,the_lst)
        mydb.commit()
        mydb.close()
        the_lst = []
    except OperationalError as e:
        mydb.reconnect(attempts=3, delay=30.0)
        my_cursor = mydb.cursor()
        my_cursor.executemany(query,lst)
        mydb.commit()
        cursor = mydb.cursor()
    except InterfaceError as e:
        mydb.reconnect(attempts=3, delay=30.0)
        my_cursor = mydb.cursor()
        my_cursor.executemany(query,lst)
        mydb.commit()
        print("data failed to insert. The MYSQL database must be down. Try again after a few hours")
        textfile = open("failed_data.txt", "w")
        for element in lst:
            textfile.write(str(element) + "\n")
        textfile.close()
    except ProgrammingError as e:
        print("data failed to insert. Check the schema of the data and the mysql connection parameters")
        #print(str(lst[0]) + "\n" + str(lst[len(lst)-1]))
        textfile = open("failed_data.txt", "w")
        for element in lst:
            textfile.write(str(element) + "\n")
        textfile.close()
    except IntegrityError as e:
        textfile = open("failed_data.txt", "w")
        for element in lst:
            textfile.write(str(element) + "\n")
        textfile.close()
    except DatabaseError as e:
        sql_exec(lst,query,data,subscription_id)
    sys.exit()
    
