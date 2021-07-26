#import pandas as pd
import mysql.connector
import datetime
from mysql.connector import cursor
from mysql.connector.errors import ProgrammingError
#import okay as cfg
import json
#import pymysql
#file = open(file_name,'r')
#f = file.readlines()
sql = ""
#print(f)
mydb=""
y = datetime.date.today()
r = "00:00:00"
with open("alright.json") as f1:
    data = json.load(f1)
file = open(data["file"]["file_name"],'r')
f = file.readlines()
try:
    #mydb = mysql.connector.connect(host="34.94.178.213",user="root",password="juhAXaZUswaSP5NLtlsW&!ip@esoDrit",
    #ssl_key = "D:/DoNotDelete/Documents/US_DEV_DB/client-key.pem",
    #ssl_cert = "D:/DoNotDelete/Documents/US_DEV_DB/client-cert.pem",
    #ssl_ca = "D:/DoNotDelete/Documents/US_DEV_DB/server-ca.pem")
    mydb = mysql.connector.connect(host=data["mysql"]["host"],user = data["mysql"]["user"],passwd = data["mysql"]["passwd"], 
    ssl_key = data["mysql"]["ssl_key"],ssl_cert = data["mysql"]["ssl_cert"],ssl_ca = data["mysql"]["ssl_ca"],db = data["mysql"]["db"])
    if(mydb):
        print("connection successfull")
except ProgrammingError as e:
    with open ('my_file.txt', 'w') as writer:
        writer.write(str(e))
my_cursor = mydb.cursor()
for i in range(len(f)):
    x = f[i].split(",")
    for j in range(len(x)):
        if(j==1):
            z = x[j]
            y = datetime.date(int(z[0:4]),int(z[4:6]),int(z[6:8]))
            #print(y)
        elif(j==2):
            g = x[j]
            r = g[0:2] + ":" + g[2:4] + ":" + g[4:6]
    if(x[0]=="117"):
        sql= "insert into dummy_sales(property_id,space_id,date,hour,sales_amount,transaction_count) values (%s,%s,%s,%s,%s,%s)"
        val = (1, 7,y,r,x[3],x[4])
        my_cursor.execute(sql,val)
        mydb.commit()
    elif(x[0]=="123"):
        sql= "insert into dummy_sales(property_id,space_id,date,hour,sales_amount,transaction_count) values (%s,%s,%s,%s,%s,%s)"
        val = (2, 7,y,r,x[3],x[4])
        my_cursor.execute(sql,val)
        mydb.commit()
#result = my_cursor.fetchall()   
#print(len(result))

