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
files_already_read = []
num_of_files = 0
cool = {}
f_name = ""
FORECAST = "linecount_part8.txt"
#fields = ("zone_id", "camera_id", "line_id", "is_enter", "video_id", "track_id", "object_id", "zone_object_id", "create_time")
fields = ("line_id", "enter_count", "exit_count", "datetime")

#the_path = "\\data\\deepnorth\\result\\counting\\"
#cool_path = "\\data\\deepnorth\\result-avro\\"
#new_file_path = "\\data\\deepnorth\\result-avro\\"
the_path = "D:\\DoNotDelete\\Documents\\counting\\"
cool_path = "C:\\Users\\prachetas.deshpande\\pub_sub_try"
new_file_path = "D:\\DoNotDelete\Documents\\pub_files"
a = ""
real_path = "D:\\DoNotDelete\\Documents\\counting"
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
                    if len(os.listdir("D:\\DoNotDelete\\Documents\\counting\\" + k))==0:
                        os.makedirs("D:\\DoNotDelete\\Documents\\avro_read_files\\" + k)
                        os.rmdir("D:\\DoNotDelete\\Documents\\counting\\" + k)
                    os.chdir(new_path)
                    for file in os.listdir():
                        json_file = file
                        if file.endswith(".json"):
                            file_path = f"{new_path}\\{file}"
                            with open(file_path, 'r') as info:
                                data = json.load(info)
                            cord = data['operations'][0]['values']
                            for j in cord:
                                lst.append(j)
                        try:
                            os.makedirs("D:\\DoNotDelete\\Documents\\avro_read_files\\" + k)
                            shutil.move("D:\\DoNotDelete\\Documents\\counting\\" + k + "\\" + json_file, "D:\\DoNotDelete\\Documents\\avro_read_files\\" + k + "\\" + json_file)
                        except FileExistsError as e:
                            shutil.move("D:\\DoNotDelete\\Documents\\counting\\" + k + "\\" + json_file, "D:\\DoNotDelete\\Documents\\avro_read_files\\" + k + "\\" + json_file)
                            #os.chdir("D:\\DoNotDelete\\Documents\\counting\\" + k)
                            os.chdir(real_path)
                            #os.chdir(real_path + "\\" + k)
                            if(len(os.listdir(real_path + "\\" + k))==0):
                                os.rmdir("D:\\DoNotDelete\\Documents\\counting\\" + k)
                                #shutil.move("D:\\DoNotDelete\\Documents\\counting\\" + k, "D:\\DoNotDelete\\Documents\\avro_read_files\\" + k)
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
                
                #shutil.move("\\data\\deepnorth\\result\\counting\\" + k, "D:\\DoNotDelete\\Documents\\avro_read_files")
                os.chdir(real_path)
    except FileNotFoundError as e:
        continue