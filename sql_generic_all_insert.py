import os
import shutil
import distutils
import json
import mysql
import pymysql
import sqlalchemy
import sys
from sqlalchemy import create_engine
import pandas as pd
import mysql.connector
from mysql.connector.errors import OperationalError, ProgrammingError
import urllib.parse

def insert_all_data(df,data):
    gk = df.groupby(['property_id','table_name'])
    lst = list(gk.groups.keys())
    for (t,y),z in zip(gk,lst):
        try:
            ssl_args = {
                "ssl_ca": "/home/gcpuser_pdeshpande/server-ca.pem",
                "ssl_cert": "/home/gcpuser_pdeshpande/client-cert.pem",
                "ssl_key": "/home/gcpuser_pdeshpande/client-key.pem"
            }
            engine = create_engine("mysql+pymysql://{user}:{pw}@{hostname}:{port}/{db}"
                        .format(user=data["mysql"][str(z[0])]["user"],
                                hostname=data["mysql"][str(z[0])]["host"],
                                pw= urllib.parse.quote_plus(data["mysql"][str(z)]["passwd"]),
                                port = 3306,
                                db=data["mysql"][str(z[0])]["db"]),connect_args=ssl_args)
            new_df = gk.get_group(t)
            name_of_table = str(z[1])
            new_df.drop(['table_name'],inplace=True)
            new_df.to_sql(name_of_table, con = engine, index = False, if_exists = 'append', method = 'multi',chunksize = 10000)
        except sqlalchemy.orm.exc or pymysql.err as e :
            missed_data = new_df.to_dict('records')
            with open("failed_data.txt","a") as missed:
                missed.writelines(missed_data)
    del gk

        
