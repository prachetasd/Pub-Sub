from matplotlib.pyplot import connect
import pandas as pd
from itertools import groupby
import json
import mysql
import pymysql
import sqlalchemy
import sys
from sqlalchemy import create_engine
import urllib.parse

def insert_all_data(df,data):
    gk = df.groupby(['property_id'])
    lst = list(gk.groups.keys())
    for (t,y),z in zip(gk,lst):
        try:
            ssl_args = {
                "ssl_ca": "D:\\DoNotDelete\\Documents\\US_DEV_DB\\dn-db-us-west2-a-dev\\dn-db-us-west2-a-dev\\server-ca.pem",
                "ssl_cert": "D:\\DoNotDelete\\Documents\\US_DEV_DB\\dn-db-us-west2-a-dev\\dn-db-us-west2-a-dev\\client-cert.pem",
                "ssl_key": "D:\\DoNotDelete\\Documents\\US_DEV_DB\\dn-db-us-west2-a-dev\\dn-db-us-west2-a-dev\\client-key.pem"
            }
            engine = create_engine("mysql+pymysql://{user}:{pw}@{hostname}:{port}/{db}"
                        .format(user=data["mysql"][str(z)]["user"],
                                hostname=data["mysql"][str(z)]["host"],
                                pw= urllib.parse.quote_plus(data["mysql"][str(z)]["passwd"]),
                                port = 3306,
                                db=data["mysql"][str(z)]["db"]),connect_args=ssl_args)
            new_df = gk.get_group(t)
            new_df.to_sql(data["mysql"][str(z)]["table_name"], con = engine, index = False, if_exists = 'append', method = 'multi',chunksize = 10000)
        except sqlalchemy.orm.exc or pymysql.err as e :
            missed_data = new_df.to_dict('records')
            with open("failed_data.txt","a") as missed:
                missed.writelines(missed_data)
    del gk
sys.exit()

