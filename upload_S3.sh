#!/bin/bash
#
# $1 - full or id
# $2 - batch ID
# $3 - indices

export AWS_ACCESS_KEY_ID=AKIAI2WSQ5635ISWITIQ
export AWS_SECRET_ACCESS_KEY=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
export AWS_DEFAULT_REGION=us-west-2

espath="/opt/reuters/data/elasticsearch"
datapath="${espath}/data"
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
        echo "$1 <full or id> <batch_id> [indices]"
    } >&2
    exit 1
}


test ! -z "$1" || usage $0
test ! -z "$2" || usage $0

[[ "$1" == "full" ]] || [[ "$1" == "id" ]] || usage $0
dump_mode="$1"
batch_id="$2"
shift 2

if [[ -z "$@" ]] || [[ "$@" == "all" ]]
then
    if [[ $dump_mode == "full" ]]
    then
        aws s3 sync $datapath $s3path_full
    else
        aws s3 sync $datapath $s3path_id
    fi
else
    indices="$@"
    for index in $indices
    do
        if [[ $dump_mode == "full" ]]
        then
            aws s3 sync $datapath/$batch_id/$index $s3path_full/$batch_id/$index
        else
            aws s3 sync $datapath/$batch_id/$index $s3path_id/$batch_id/$index
        fi
    done
fi
