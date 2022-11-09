import json
import pandas as pd
import ast
from collections import namedtuple
from google.cloud import storage
import os

#schema_bucket = os.environ.get("schema_bucket" , None)
#validation_schema_file = os.environ.get("validation_schema_file" , None)
#storage_client = storage.Client()
#bucket = storage_client.get_bucket(schema_bucket)
#blob = bucket.blob(validation_schema_file)
#df = json.loads(blob.download_as_string())

#schema = df

def strt(d):
    # print('dt',type(d[0]))
    dt = d
    data = [{}]
    # dt={}
    s = '{}'.format(dt)
    s = s.replace('[', '')
    s = s.replace(']', '')
    print(s)
    s = s.replace(': ,', ':{},')
    s = s.replace(': }', ':{}}')
    s = s.replace('true', 'True')
    s = s.replace('false', 'False')
    print('s', s)
    res = ast.literal_eval(s)
    # s=s.replace("'", "\"")
    # print(type(res))
    data[0] = res
    print(type(data[0]))
    df = pd.json_normalize(data)
    print('df', df)
    print(sorted(df))
    loadkey = sorted(df)
    return loadkey


def appen(de):
    l = []
    se = schem(de, l)
    # l=l+se
    return se


def schem(de, led):
    l = led
    s = ''
    print('s', s)
    print('de', de)
    for d in de:
        if len(d) == 3:
            print(s)
            if s == '':
                l.append(d["name"])
            elif s != '':
                l.append(s + '.' + d["name"])
        elif len(d) != 3:
            name = d["name"]
            if type(d["fields"]) == list:
                li = d["fields"]
                for ele in li:
                    ele["name"] = '{}.{}'.format(name, ele["name"])
                print(li)
                lem = appen(li)
                l = l + lem
    return l


# dmain=strt(d)
def main(d,schema_bucket,validation_schema_file):
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(schema_bucket)
    blob = bucket.blob(validation_schema_file)
    df = json.loads(blob.download_as_string())
    le = []
    schema = df
    d1 = schema
    print("in schema validation main")
    print(d1, type(d1))
    l = schem(d1, le)
    dmain = sorted(l)
    s1 = strt(d)
    set1 = set(s1)
    set2 = set(dmain)
    is_subset = set1.issubset(set2)
    return is_subset


def validate(d,schema_bucket,validation_schema_file):
    print(" payload from hapi :    "  , d)
    for ele in range(0,1):
        print("element sent for validation :  " , d[ele] )
        if main(d[ele],schema_bucket,validation_schema_file) != True:
            return False
    else:
        return True

# print('valida',validate(d))
