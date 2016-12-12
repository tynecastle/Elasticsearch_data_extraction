#!/bin/bash
#
# $1 - dump mode (full or id)
# $2 - batch ID
# $3 - indices

export AWS_ACCESS_KEY_ID=AKIAI2WSQ5635ISWITIQ
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-west-2

s3path_full="s3://tr-ips-ses-data/wos-extractions/wos-full-extractions"
s3path_id="s3://tr-ips-ses-data/wos-extractions/wos-id-extractions"

function info_log()
{
    timeStamp=`date +%Y%m%d%H%M%S`
    {
        echo $timeStamp $1
    } >&2
}

function usage()
{
    {
        echo "usage:"
        echo "$1 <dump_mode> <data_path> <batch_id> [indices]"
    } >&2
    exit 1
}


test ! -z "$1" || usage $0
test ! -z "$2" || usage $0
test ! -z "$3" || usage $0

[[ "$1" == "full" ]] || [[ "$1" == "id" ]] || usage $0
dumpmode="$1"
datapath="$2"
batchid="$3"
shift 3

if [[ -z "$@" ]] || [[ "$@" == "all" ]]
then
    if [[ $dumpmode == "full" ]]
    then
        aws s3 sync --quiet $datapath $s3path_full
    else
        aws s3 sync --quiet $datapath $s3path_id
    fi
else
    indices="$@"
    for index in $indices
    do
        if [[ $dumpmode == "full" ]]
        then
            aws s3 sync --quiet $datapath/$batchid/$index $s3path_full/$batchid/$index
        else
            aws s3 sync --quiet $datapath/$batchid/$index $s3path_id/$batchid/$index
        fi
    done
fi
