#!/usr/bin/env python3

import time, os, argparse, pymongo, re, time
from datetime import datetime

parser=argparse.ArgumentParser()
parser.add_argument('log_path',help="Enter path for HTTP logs e.g. \"/var/log/apache2/access.log\"")
parser.add_argument('Mongo_Server',help = "Please enter the details for mongo server e.g.\n\"mongodb://USER_NAME:PASSWORD@IP_ADDRESS:27017/\"")
args=parser.parse_args()
filename=args.log_path
Mongo_Credentials = args.Mongo_Server


APACHE_LOG_PATTERN = '(?P<ip>[.:0-9a-fA-F]+) - - \[(?P<time>.*?)\] "(?P<method>[A-Z]*) (?P<uri>.*?) (?P<http_version>HTTP\/1.\d)" (?P<status_code>\d+) (?P<content_length>\d+) "(?P<referral>.*?)" "(?P<agent>.*?)"'
search = re.compile(APACHE_LOG_PATTERN).search


file = open(filename,'r')
st_results = os.stat(filename)
st_size = st_results[6]
file.seek(st_size)


conn = pymongo.MongoClient(Mongo_Credentials.replace("\n",""))
db = conn.http_bank

def json_response(x):
    return {
        'server_ip':x.group('ip'),
        'method':x.group('method'),
        'uri':x.group('uri'),
        'status_code':int(x.group('status_code')),
        'http_version':x.group('http_version'),
        'content_length':int(x.group('content_length')),
        'agent':x.group('agent'),
        'referral':x.group('referral'),
        'time':datetime.strptime((x.group('time'))[:-6],"%d/%b/%Y:%H:%M:%S"),
}


def Mongo_Entry(HTTP):
    try:
        matches = search(HTTP)
        db.http_bank.insert_one(json_response(matches))
    except:
        #Still need to do error logging
        pass


def tail_f():
    while 1:
        where = file.tell()
        line = file.readline()
        if not line:
            time.sleep(3)
            file.seek(where)
        else:
            Mongo_Entry(line)

def main():
    tail_f()


if __name__ == '__main__':
    main()
