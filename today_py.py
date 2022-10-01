from multiprocessing.sharedctypes import Value
import findspark
from pandas import value_counts
findspark.init("/usr/local/spark")

import json
import configparser

def input_display(data):
    #f=open(file,"r")
    #str="{muffin : lolz, foo : 'kitty'}"
 
    for key , values in data.items():
        print(key, values)

def fun(config_p):
    print("inside start offunction")
    
    print(config_p)
    #json_acceptable_string = config_p.replace("'", "\"")
    s=config_p.replace("{", "").replace("}", "").split(",")
            
    dictionary = {}

    for i in s:
        dictionary[i.split(":")[0].strip('\'').replace("\"", "")] = i.split(":")[1].strip('"\'')
            
    print(dictionary)
    
    input_display(dictionary)


if __name__=="__main__":
    config_p = configparser.ConfigParser()
    config_p.read('/home/test/Desktop/python_basics/pyspark_program/param.conf')
    file=config_p['gna']['input']
    print(file)
    fun(file)

# from datetime import datetime
# from datetime import date , timedelta
# from pyspark import year , add_months , cast

# def cam_acc():
#     today=date.today()
#     print("today: " + str(today))
#     first=today.replace(day=1)
#     print("first: " + str(first))
#     last=first - timedelta(days=1)
#     print("last: " + str(last))



