#!/bin/bash
# Rename extracted *_ori.json.gz files by reassigning sequence to each file.
# This script should be executed under the parent directory to 'data'.
# A multiplier is required as the argument to the script.
# Imagine 5 nodes are involved in the extraction, the script should be run like:
# [ec2-user@AWS_node1 /opt/reuters/data/elasticsearch]$ ./rename.sh 0
# [ec2-user@AWS_node2 /opt/reuters/data/elasticsearch]$ ./rename.sh 1
# [ec2-user@AWS_node3 /opt/reuters/data/elasticsearch]$ ./rename.sh 2
# [ec2-user@AWS_node4 /opt/reuters/data/elasticsearch]$ ./rename.sh 3
# [ec2-user@AWS_node5 /opt/reuters/data/elasticsearch]$ ./rename.sh 4
#
# Last updated: Dec 11, 2016
#

function usage()
{
    {
        echo "usage:"
        echo "$1 <datapath> <multiplier>"
    } >&2
    exit 1
}

function rename_index()
{
    local index=$1
    local multi=$2
    local offset=1
    index_count=`find $datapath/$index -name *_ori.json.gz | wc -l`
    let offset=offset+index_count*multi
    for f in `find $datapath/$index -name *_ori.json.gz`
    do
        cur_seq=`printf %05d $offset`
        new_name="${datapath}/${index}/${index}_${cur_seq}.json.gz"
        mv $f $new_name
        let offset++
    done
}

test -z "$1" && usage $0
test -z "$2" && usage $0

datapath=$1
multiplier=$2
epoch=`basename $datapath`

for index_path in `find $datapath -type d | grep -v "$epoch$"`
do
    index=`basename $index_path`
    rename_index $index $multiplier
done
