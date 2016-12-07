#!/usr/bin/env python
# 
# This script performs full dump for any ES index.
# Can be run on an AWS node, where python module 'elasticsearch' is installed.
# The '--url' argument is required, where the internal domain name and port of the
# target cluster is provided.
# binnum range should be provided as input to this script.
# The standard ouput should be redirected to a log file for the use of report generation.
#
# Last updated: Dec 4, 2016
#

import os
import re
import sys
import json
import gzip
import Queue
import argparse
import datetime
import threading
import elasticsearch
import elasticsearch.helpers

binid_max = 2 ** 32
queue = Queue.Queue()
    
def makeDir(pathName):
    if not os.path.isdir(pathName):
        os.makedirs(pathName)
    
    
def out_log(msg):
    now = datetime.datetime.now()
    print '%s %s' % (now.strftime('%Y%m%d%H%M%S'), msg)
    

def file_size(file_name):
    if file_name == None or file_name == "":
        return 0
    else:
        return os.stat(file_name).st_size


def get_count(es, args):
    query = {"query":{"range": {"binnum": {"gte":args.binnum_start, "lt":args.binnum_end}}}}
    rep = es.count(index=args.index, body=query)
    return rep['count']


def seq2str(sequence):
    return ' '.join(str(elem) for elem in sequence)


def roundOfPower2(number):
    t = number
    t = t - 1
    t = t | t >> 1
    t = t | t >> 2
    t = t | t >> 4
    t = t | t >> 8
    t = t | t >> 16
    return t + 1
    
    
def chunk(total_hits, allow, start=0, end=binid_max):
    start = int(start)
    end = int(end)
    if total_hits <= allow:
        yield 0, end
    else:
        number = (end - start) * allow / total_hits
        number = roundOfPower2(number)
        while start != end:
            if start + number != end:
                yield start, start + number
            else:
                yield start, end
            start += number
    

def out_put(fsize_max, args):
    current_rec_number = 0
    total_count = 0
    seq = 0

    while True:
#        print "number of items in the queue: " + Queue.Queue.qsize(queue)
        ind, ind_path, records, last_chunk = queue.get()
        for record in records:
            if current_rec_number == 0:
                file_path = ind_path + "/" + ind + "_" + str(seq) + "_ori.json.gz"
                fgz = gzip.open(file_path, 'wb')
                seq += 1
            if args.fields[0] != 'all':
                line = ''
                for field in args.fields:
                    if field not in record:
                        line = line + ' '
                    elif isinstance(record[field], (list,tuple)):
                        line = line + '%s ' % seq2str(record[field])
                    else:
                        line = line + '%s ' % record[field]
                result = line.strip() + '\n'
            else:
                line_id = '{ "index" : { "_id" : "%s" } }' % record['_id']
                line_content = json.dumps(record['_source'], ensure_ascii=False)
                result = '%s\n%s\n' % (line_id, line_content)
            result = result.encode("utf-8")
            fgz.write(result)
            fgz.flush()
            current_rec_number += 1
            total_count += 1
            if file_size(file_path) >= fsize_max:
                current_rec_number = 0
                fgz.close()
        if last_chunk:
            break

    print '-%s %s' % (args.index, total_count)
    out_log('END DUMP %s' % args.index)


def process_es(args):
    
    es = elasticsearch.Elasticsearch([args.url], timeout=120)
    index_path = args.path + "/" + args.index
    makeDir(index_path)
    binid_end = binid_max
    last_chunk = False
    
    out_log('START DUMP %s' % args.index)
    
    total_hits = get_count(es, args)
    print '+%s %s' % (args.index, total_hits)
    
    for start, end in chunk (total_hits, args.bulk_size, 0, binid_end):
        if end == binid_end:
            last_chunk = True
        query = {"query" : {"bool" : {"must" : [{"range": {"binnum": {"gte":args.binnum_start, "lt":args.binnum_end}}}, {"range": {"binid": {"gte":start, "lt":end}}}]}}}
        if args.fields[0] != 'all':
            query["fields"] = args.fields
            for i in range(len(query["fields"])):
                if query["fields"][i] == "fuid" or query["fields"][i] == "docid":
                    if args.index == "superunif":
                        query["fields"][i] = "docid"
                    else:
                        query["fields"][i] = "fuid"
            results = [r["fields"] for r in elasticsearch.helpers.scan(es, query=query, index=args.index, _source=False, size=args.batch_size, scroll='5m', request_timeout=120)]
        else:
            results = elasticsearch.helpers.scan(es, query=query, index=args.index, size=args.batch_size, scroll='5m', request_timeout=120)

        queue.put((args.index, index_path, results, last_chunk))
    

def log(what):
    print what
    

if __name__ == '__main__':
    allow_file_size = 52428800

    parser = argparse.ArgumentParser()
    parser.add_argument("-u", "--url", required=True, help="the url to the engine")
    parser.add_argument("-i", "--index", required=True, help="the index to be processed")
    parser.add_argument("-p", "--path", required=True, help="the parent directory of indices")
    parser.add_argument("-k", "--bulk_size", type=int, default=512, help="the doc count in one binid range")
    parser.add_argument("-b", "--batch_size", type=int, default=1000, help="the number of doc to process per request")
    parser.add_argument("-v", "--verbose", action='store_true', help="enable verbose output")
    parser.add_argument("-f", "--fields", nargs="+", type=str, default="all", help="the specific fields to be extracted")

    m_args = parser.parse_args()
    
    m_args.binnum_start, m_args.binnum_end = sys.stdin.readline().strip().split(',')
    m_args.binnum_start = int(m_args.binnum_start)
    m_args.binnum_end = int(m_args.binnum_end)

#    log('url: %s' % m_args.url)
#    log('bulk_size: %s' % m_args.bulk_size)
#    log('batch_size: %s' % m_args.batch_size)
#    log('binnum_start: %s' % m_args.binnum_start)
#    log('binnum_end: %s' % m_args.binnum_end)
    
    producer = threading.Thread(target=process_es, args=(m_args,))
    consumer = threading.Thread(target=out_put, args=(allow_file_size, m_args))

    producer.start()
    consumer.start()
