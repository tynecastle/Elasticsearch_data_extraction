#!/usr/bin/env python
# 
# Generate report for ES extraction. Below is a sample report:
# collection    total hits  avg speed(items/hour)   avg time(hours)
# *****************************************************************
# fsta             1262873        15222420.024178          0.020722
# diidw           30710787        93916804.373746          0.065556
# cabi            10669033         9074681.014382          0.236222
# superunif      314815200        32249543.738631          1.987722
# bci             25447596        19443880.619823          0.261889
# drci             6456610         9926585.561159          0.135278
# cscd             4521284        16699346.773011          0.062722
# gci               871516        14079827.594020          0.014333
# kjd              1142055        25876405.433743          0.012167
# inspec          17148116        10456965.636582          0.331167
# zoorec           4232057         3042322.016678          0.287556
# bioabs          16659559        22208040.755871          0.150722
# medline         25996812         8637691.044802          0.641278
# wos             63474619        23417377.183562          0.547611
# biosis          25450641        18676496.897987          0.273000
# rsci              374939        11554888.831836          0.008333
# scielo            515070         7249361.596886          0.015722
# ccc             23372722        18427914.842537          0.254389
# *****************************************************************
# total time
# *****************************************************************
# 5.30638888889
# 
# The content of dump log files from all the nodes involved in the 
# extraction should be provided as input to this script. 
# The '-n' option is required to indicate the number of nodes involved
# in the extraction.
#
# Sample command:
# $ cat dump_20161011_data* | python dumpreport.py -n 5
#
# Last updated: Oct 25, 2016
#

import getopt
import sys
import re
import datetime
import elasticsearch


time_report = re.compile(r'(\d{4})(\d{2})(\d{2})(\d{2})(\d{2})(\d{2}) .*? DUMP (\w+)')


def get_report(date_string):
    year, month, day, hour, mininute, second, collection = time_report.findall(date_string)[0]
    year     = int(year)
    month    = int(month)
    day      = int(day)
    hour     = int(hour)
    mininute = int(mininute)
    second   = int(second)
    return datetime.datetime(year, month, day, hour, mininute, second), collection

   
def get_speed_latency(item, count):
    speed = latency = -1
    if 'start' in item and 'end' in item:
        time_delta = item['end'] - item['start']
        time_diff = time_delta.days * 24 * 60 * 60 + time_delta.seconds
        speed = count/(time_diff/60.0/60.0)
        latency = time_diff/60.0/60.0
    
    return speed, latency
    
    
def get_total_time(coll_table):
    s_time = e_time = None
    for coll, item in coll_table.iteritems():
        if 'start' in item:
            if not s_time or s_time > item['start']:
                s_time = item['start']
        if 'end' in item:
            if not e_time or e_time < item['end']:
                e_time = item['end']
                
    if s_time and e_time:
        time_delta = e_time - s_time
        time_diff = (time_delta.days * 24 * 60 * 60 + time_delta.seconds) / 60.0/60.0
    else:
        time_diff = -1
        
    return time_diff
        
        
def collection_start_time(line, coll_table):
    s_time, coll = get_report(line)
    
    if coll not in coll_table:
        coll_table[coll] = {'start': s_time, 'real_count':0, 'dump_count':0, 'nodes_num':0}
    elif s_time < coll_table[coll]['start']:
        coll_table[coll]['start'] = s_time
        
        
def collection_end_time(line, coll_table):
    e_time, coll = get_report(line)
    
    if 'end' not in coll_table[coll]:
        coll_table[coll]['end'] = e_time
    elif e_time > coll_table[coll]['end']:
        coll_table[coll]['end'] = e_time
        
        
coll_real_hits = re.compile(r'\+(\w+?) (\d+)')
def collection_real_count(line, coll_table, host, host_table):
    hits = 0
    
    if coll_real_hits.match(line):
        coll, hits = coll_real_hits.findall(line)[0]
        hits = int(hits)
        coll_table[coll]['real_count'] += hits
        coll_table[coll]['nodes_num'] += 1
        
        host_table[host][coll] = {'real': hits, 'dump': 0}
        
    return hits


coll_dump_hits = re.compile(r'-(\w+?) (\d+)')
def collection_dump_count(line, coll_table, host, host_table):
    hits = 0
    
    if coll_dump_hits.match(line):
        coll, hits = coll_dump_hits.findall(line)[0]
        hits = int(hits)
        coll_table[coll]['dump_count'] += hits
        
        host_table[host][coll]['dump'] = hits
        
    return hits
    
    
def get_count(es, coll):
    query = {"query":{"match_all":{}}}
    rep = es.count(index=coll, body=query)
    return rep['count']


host_tmp = re.compile(r'host: (.+)')
binnum_range_tmp = re.compile(r'binnum_range: (.+)')

if __name__ == "__main__":
	
    try:
        opts, args = getopt.getopt(sys.argv[1:], "n:u:")
    except getopt.GetoptError, err:
        print str(err)
        sys.exit(2)

    nodes_num = 0
    host = ''
    for o, a in opts:
        if o == "-n":
            nodes_num = int(a)
        elif o == "-u":
            host = a
	
    stat = "end"
    print "%-14s%10s%15s%23s%18s%10s" % ("collection", "real hits", "dumped hits", "speed (item/hour)", "time (hours)", "success")
    print "*" * 90
    
    coll_table = {}
    host_table = {}
    real_count = dump_count = 0
    node = binnum_range = None
    for line in sys.stdin:
        line = line.strip()
        
        if host_tmp.match(line):
            node = host_tmp.findall(line)[0]
            continue
            
        if binnum_range_tmp.match(line):
            binnum_range = binnum_range_tmp.findall(line)[0]
            host_table[node] = {'binnum_range': binnum_range}
            continue
        
        if coll_real_hits.match(line):
            real_count = collection_real_count(line, coll_table, node, host_table)
            continue
        elif coll_dump_hits.match(line):
            dump_count = collection_dump_count(line, coll_table, node, host_table)
            continue
            
        if stat != "start" and time_report.match(line):
            collection_start_time(line, coll_table)
            stat = "start"
            continue

        if stat == "start" and time_report.match(line):
            collection_end_time(line, coll_table)
            stat = "end"
            continue
      
    es = elasticsearch.Elasticsearch(host, timeout=120)  
    have_error = False    
    for coll, item in  coll_table.iteritems():
        count = get_count(es, coll)
        succ = 'N'
        if nodes_num == item['nodes_num'] and item['real_count'] == item['dump_count']:
            succ = 'Y'
        else:
            have_error = True
        #print "%-14s%10d%23f%18f" % (coll, count, (speed_table[coll]/number), (time_table[coll]/ 60.0/60.0)/number)
        speed, latency = get_speed_latency(item, item['dump_count'])
        print "%-14s%10d%15d%23f%18f%10s" % (coll, item['real_count'], item['dump_count'], speed, latency, succ)

    print "*" * 90
    total_time = get_total_time(coll_table)
    print "total time : %f" % total_time
    
    if have_error:
        print " "
        print "error information:"
        print "*" * 75
        print "%-20s%14s%16s%10s%15s" % ("host", "collection", "binnum_range", "real hits", "dumped hits")
        print "*" * 75
        for host, item in host_table.iteritems():
            for coll in coll_table:
                real_hits = dumped_hits = -1
                if coll in item:
                    real_hits = item[coll]['real']
                    dumped_hits = item[coll]['dump']
                    if real_hits != dumped_hits or (real_hits == -1 and dumped_hits == -1):
                        print "%-20s%14s%16s%10d%15d" % (host, coll, item["binnum_range"], real_hits, dumped_hits) 
                else:
                    print "%-20s%14s%16s%10d%15d" % (host, coll, item["binnum_range"], real_hits, dumped_hits)
    

