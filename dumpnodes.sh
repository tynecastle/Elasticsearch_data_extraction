#!/bin/bash -
if [ -z "$1" ]; then
    echo "usage:"
    echo "-l list the AWS nodes"
    echo "-d delete the AWS nodes"
    echo "<node1> <node2> add node1 node2 to list"
fi
dir_name=`dirname $0`
config_file="${dir_name}/.nodes"
if [ "$1" = "-l" ]; then
    test -f $config_file || touch $config_file
    cat $config_file 
    exit
fi

if [ "$1" = "-d" ]; then
    printf "" > $config_file
    exit 
fi

for i in $*
do
    echo $i >> $config_file
done
exit
