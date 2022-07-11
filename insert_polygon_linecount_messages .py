import sqlalchemy
#import mysql
import pymysql
import datetime
import mysql.connector
from sqlalchemy import create_engine
import itertools
import pytz
def sort_by_company(key):
    return key[0]

def sort_by_property(sec_key):
    return sec_key[1]

def insert_into_database(lst_of_messages,base_query,line_id_or_polygon_id,engine,table_name):
    log_list = []
    lst_sorted_by_company = sorted(lst_of_messages, key=sort_by_company)
    for company_keys, company_messages in itertools.groupby(lst_sorted_by_company, sort_by_company):
        lst_of_messages_by_company = list(company_messages)
        for property_keys, property_messages in itertools.groupby(lst_of_messages_by_company, sort_by_property):
            lst_of_messages_by_property = list(property_messages)
            #with engine.begin() as conn:
            try:
                #conn.execute("SET FOREIGN_KEY_CHECKS = 0")
                #print("set foregin key executed")
                company_id = lst_of_messages_by_property[0][0]
                property_id = lst_of_messages_by_property[0][1]
                with engine.begin() as deepnorth_conn:
                    deepnorth_conn.execute("use deepnorth")
                    #print("use deepnorth query executed")
                    mysql_info = deepnorth_conn.execute("select hostname,username,password,database_name,location from database_info where company_id = " + str(company_id) + " and property_id = " + str(property_id))
                #print("mysql_info query executed")
                mysql_config = mysql_info.fetchone()
                hostname = str(mysql_config[0])
                username = str(mysql_config[1])
                password = str(mysql_config[2])
                database_name = str(mysql_config[3])
                location = str(mysql_config[4])
                print("Creating mysql connector")
                #sql_engine = create_engine("mysql+pymysql://{user}:{pw}@{hostname}"
                        #.format(user=username,
                                #hostname=host,
                                #pw=password))
                prod_db = mysql.connector.connect(host = hostname, user = username, passwd = password, db = database_name)
                print("mysql connector successfully created")
                cursor = prod_db.cursor()
                for message in lst_of_messages_by_property:
                    message_id = message[2]
                    message = message[1:]
                    message = message[1:] 
                    #message_query = tuple("','".join(message))
                    #print(message)
                    try:
                        cursor.execute(base_query,message)
                        prod_db.commit()
                        log_list.append([company_id,property_id,message_id])
                    except Exception as e:
                        print(str(e))
                        with open("/opt/failed_data_storage/" + str(table_name) + "_failed_messages.txt","a") as f:
                            for failed_message in lst_of_messages_by_property:
                                f.write(str(failed_message)+"\n")
                cursor.close()
                prod_db.disconnect()
                if prod_db.is_connected():
                    cursor.close()
                    prod_db.close()
                    print("mysql connection is closed")
                update_log_table(engine,log_list,line_id_or_polygon_id,location)
                #mydb.close()
                print("connection successfully closed")
                log_list = []
            except Exception as e:
                print(str(e))
                with open("/opt/failed_data_storage/" + str(table_name) + "_failed_messages.txt","a") as f:
                    for failed_message in lst_of_messages_by_property:
                        f.write(str(failed_message)+"\n")

def update_log_table(engine,log_list,line_id_or_polygon_id,location):
    #lst = [company_id,property_id,message_id]
    insert_query = ""
    update_query = ""
    result = ""
    with engine.begin() as conn:
        for log in log_list:
            conn.execute("use deepnorth")
            if line_id_or_polygon_id == "line_id":
                result = "select count(*) from data_log where line_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"  
            elif line_id_or_polygon_id == "polygon_id":
                result = "select count(*) from data_log where polygon_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"
            else:
                result = "select count(*) from data_log where camera_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"
            response = conn.execute(result)
            value_returned = response.fetchone()[0]
            last_update_time = datetime.datetime.now(pytz.timezone(location))
            #updated_time = last_update_time.split(".")
            cur_hour = str(last_update_time.hour)
            if len(cur_hour)==1:
                cur_hour = "0" + cur_hour
            cur_minute = str(last_update_time.minute)
            if len(cur_minute)==1:
                cur_minute = "0" + cur_minute
            cur_second = str(last_update_time.second)
            if len(cur_second)==1:
                cur_second = "0" + cur_second
            final_timestamp = str(last_update_time.date()) + " " + cur_hour + ":" +  cur_minute + ":" + cur_second
            #final_timestamp = str(current_time)
            if value_returned == 0:
                log.append(final_timestamp)
                if line_id_or_polygon_id == "line_id":
                    insert_query = "insert into data_log (company_id,property_id,line_id,last_update_time) values"
                elif line_id_or_polygon_id == "polygon_id":
                    insert_query = "insert into data_log (company_id,property_id,polygon_id,last_update_time) values"
                else:
                    insert_query = "insert into data_log (company_id,property_id,camera_id,last_update_time) values"
                log_query = insert_query + "('" + "','".join(log) + "')"
                conn.execute(log_query)
            else:
                if line_id_or_polygon_id == "line_id":
                    update_query = "update data_log set last_update_time='" + final_timestamp + "' where line_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"  
                elif line_id_or_polygon_id == "polygon_id":
                    update_query = "update data_log set last_update_time='" + final_timestamp + "' where polygon_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"
                else:
                    update_query = "update data_log set last_update_time='" + final_timestamp + "' where camera_id = " + "'" + str(log[2]) + "' and company_id = '" + str(log[0]) + "' and property_id = '" + str(log[1]) + "'"    
                conn.execute(update_query)
        conn.close()
    engine.dispose()

