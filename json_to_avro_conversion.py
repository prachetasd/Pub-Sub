import io
#import avro
#import avro.schema
#from avro.datafile import DataFileReader, DataFileWriter
#from avro.io import DatumReader, DatumWriter
import csv
import time
import os
import json
import os.path
from os import path
#import numpy as np
import sys
from collections import namedtuple
import shutil
data = ""
lst = []
lst2 = []
#files_already_read = []
num_of_files = 0
cool = {}
f_name = ""
FORECAST = "linecount_part8.txt"
#fields = ("zone_id", "camera_id", "line_id", "is_enter", "video_id", "track_id", "object_id", "zone_object_id", "create_time")
fields = ("line_id", "enter_count", "exit_count", "datetime")
with open("json_to_avro_config.json") as f1:
    data = json.load(f1)
#the_path = "/data/deepnorth/result/counting/"
#cool_path = "/data/deepnorth/result-avro/"
#new_file_path = "/data/deepnorth/result-avro/"
cwd = str(os.getcwd())
the_path = str(data["project"]["Files_to_read"]) + "/"
cool_path = cwd
new_file_path = str(data["project"]["Files_converted"])
a = ""
real_path = str(data["project"]["Files_to_read"])
files_already_read = str(data["project"]["Files_read"])
json_file = ""
#os.chdir(cool_path)       
#print(len(lst))
#print(num_of_files)
os.chdir(real_path)
while True:
    try:
        if len(os.listdir())!=0:
            for k in os.listdir():
                new_path = the_path + str(k)
                if str(path.isdir(new_path)) == "True":
                    if len(os.listdir(real_path + "/" + k))==0:
                        os.makedirs(files_already_read + k)
                        os.rmdir(real_path + "/" + k)
                    os.chdir(new_path)
                    for file in os.listdir():
                        json_file = file
                        if file.endswith(".json"):
                            file_path = f"{new_path}/{file}"
                            with open(file_path, 'r') as info:
                                data = json.load(info)
                            cord = data['operations'][0]['values']
                            for j in cord:
                                lst.append(j)
                        try:
                            os.makedirs(files_already_read + k)
                            shutil.move(real_path + "/" + k + "/" + json_file, files_already_read + k + "/" + json_file)
                        except FileExistsError as e:
                            shutil.move(real_path + "/" + k + "/" + json_file, files_already_read + k + "/" + json_file)
                            #os.chdir("D:/DoNotDelete/Documents/counting/" + k)
                            os.chdir(real_path)
                            #os.chdir(real_path + "/" + k)
                            if(len(os.listdir(real_path + "/" + k))==0):
                                os.rmdir(real_path + "/" + k)
                                #shutil.move("D:/DoNotDelete/Documents/counting/" + k, "D:/DoNotDelete/Documents/avro_read_files/" + k)
                            #os.chdir(re_path)
                    if lst:
                        num_of_files = num_of_files + 1
                        for i in lst:
                            a = "{'property_id':'1'" +",'" + fields[0] + "':'" + i[0] + "','" + fields[3] + "':'" + i[3] + "','" + fields[1] + "':'" + i[1] + "','" + fields[2] + "':'" + i[2] + "'}"
                            lst2.append(a)
                        os.chdir(new_file_path)
                        f_name = "linecount_whole_" + str(num_of_files) + ".txt"
                        file1 = open(f_name,"w")
                        for y in range(0,len(lst2)):
                            if y==len(lst2)-1:
                                file1.write(str(lst2[y]))
                            else:
                                file1.write(str(lst2[y]) + "\n")
                        file1.close()
                        lst = []
                        lst2 = []
                os.chdir(cool_path)
                
                #shutil.move("/data/deepnorth/result/counting/" + k, "D:/DoNotDelete/Documents/avro_read_files")
                os.chdir(real_path)
    except FileNotFoundError as e:
        continue