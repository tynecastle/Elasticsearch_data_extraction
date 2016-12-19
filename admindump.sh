#!/bin/bash
#
# $1 - CLUSTER_URL:PORT
# $2 - name of data directory
# $3 - optional arguments
#
# Last updated: Dec 17, 2016
#

ssh_opt="-o LogLevel=quiet -o StrictHostKeyChecking=no"
keypath="KP_infrastructure.pem"
awsuser="ec2-user"
espath="/opt/reuters/data/elasticsearch"
toolpath="${espath}/esdumptool"
backupdir="${espath}/data_uploaded"
batchidscript="getbatchid.sh"
dumpscript="esdump.py"
queryfile="query_dump.json"
renamescript="rename.sh"
uploadscript="upload_S3.sh"
dateStamp=`date +%Y%m%d`

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
        echo "$1 <URL> <data_dir> [-a] [-f fields_silo] [-s fields_su] [-b batch_id] [-q]"
    } >&2
    exit 1
}

## upload the dumped files to S3 storage
function upload_S3()
{
    info_log "START UPLOAD"
    if [ "$allfields" == "Y" ]
    then
        dumpmode="full"
    else
        dumpmode="id"
    fi
    for node in ${nodes[*]} 
    do
        scp $ssh_opt -i $keypath $uploadscript ${awsuser}@${node}:${toolpath}
        cmd_upload="sh ${toolpath}/${uploadscript} $dumpmode $data_dir $batchid"
        ssh $ssh_opt -i $keypath ${awsuser}@${node} $cmd_upload &
    done
    wait
    info_log "END UPLOAD"
}

echo $0
echo $*

test ! -z "$1" || usage $0
test ! -z "$2" || usage $0

URL="$1"
data_dir="${espath}/$2"
shift 2

while getopts 'af:s:b:q:n:' OPTION
do
    case $OPTION in
    a)
        allfields="Y"
        ;;
    f)
        fields_silo="$OPTARG"
        ;;
    s)
        fields_su="$OPTARG"
        ;;
    b)
        batchid="$OPTARG"
        ;;
    q)
	    echo "-q: $OPTARG"
        query_dsl="$OPTARG"
        ;;
    n)
        binnum_arr="$OPTARG"
        ;;
	esac
done

if [ "$allfields" == "Y" ] && [ "$fields_silo" != "" ]
then
    echo "Invalid options! '-a' and '-f' are not supposed to be used together!"
    exit 1
fi

if [ "$allfields" == "Y" ] && [ "$fields_su" != "" ]
then
    echo "Invalid options! '-a' and '-s' are not supposed to be used together!"
    exit 1
fi

if [ "$allfields" == "Y" ]
then
    fields_silo="all"
    fields_su="all"
fi

nodes_number=`sh dumpnodes.sh -l|wc -l`
nodes=(`sh dumpnodes.sh -l`)

## get the latest batch ID from s3://tr-search-data/1.5/dev/incrementals/ if not provided as an argument
if [ "$batchid" == "" ]
then
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[0]} "[[ -d $toolpath ]] || mkdir -p $toolpath"
    scp $ssh_opt -i $keypath $batchidscript ${awsuser}@${nodes[0]}:${toolpath}
    batchid=$(ssh $ssh_opt -i $keypath ${awsuser}@${nodes[0]} "sh ${toolpath}/${batchidscript}")
fi
datapath="${data_dir}/${batchid}"

echo "query_dsl: $query_dsl"
## if a specific query is provided, write it to a file, otherwise delete this file in case previously created
if [[ "$query_dsl" != "" ]]
then
    echo "Creating query file ..."
    echo $query_dsl > $queryfile
    querypath="${toolpath}/${queryfile}"
else
    echo "Removing query file ..."
    [ -f $queryfile ] && rm -f $queryfile
fi

echo "Calculating the binnum pairs ..."
## calculate the binnum pairs required by the python dump script, if not provided as an argument
if [ "$binnum_arr" == "" ]
then
    binnum_arr=(`python binnumchunk.py -n$nodes_number`)
else
    bstart=$(echo $binnum_arr | cut -d',' -f1)
    bend=$(echo $binnum_arr | cut -d',' -f2)
    binnum_arr=(`python binnumchunk.py -s$bstart -e$bend -n$nodes_number`)
fi

echo "Uploading scripts to each node ..."
## deploy necessary directories and scripts to each node
for ((n=0; n<$nodes_number; n++))
do
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $toolpath ]] && rm -rf $toolpath ; mkdir -p $toolpath"
    scp $ssh_opt -i $keypath $dumpscript ${awsuser}@${nodes[$n]}:${toolpath}
    if [ -f $queryfile ]
    then
        scp $ssh_opt -i $keypath $queryfile ${awsuser}@${nodes[$n]}:${toolpath}
    fi
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $datapath ]] || mkdir -p $datapath"
    #ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"binnum_range: ${binnum_arr[$n]}\" > $espath/dump_${dateStamp}_${n}.log"
    ## Eileen modify 2016-12-08:
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"host: ${nodes[$n]}\" > $espath/dump_${dateStamp}_${n}.log"
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"binnum_range: ${binnum_arr[$n]}\" >> $espath/dump_${dateStamp}_${n}.log"
done

## execute the dump operation on the specified nodes concurrently, index by index
for index in `sh dumpindices.sh -l`
do
    info_log "START DUMP $index"
    i=0
    if [ "$index" != "superunif" ]
    then
        fields="$fields_silo"
    else
        fields="$fields_su"
    fi
    for node in ${nodes[*]}
    do
        if [ "$query_dsl" != "" ]
        then
            cmd_dump="echo ${binnum_arr[$i]} | python ${toolpath}/${dumpscript} -u $URL -i $index -p $datapath -f $fields -q $querypath >> $espath/dump_${dateStamp}_${i}.log"
        else
            cmd_dump="echo ${binnum_arr[$i]} | python ${toolpath}/${dumpscript} -u $URL -i $index -p $datapath -f $fields >> $espath/dump_${dateStamp}_${i}.log"
        fi
        ssh $ssh_opt -i $keypath ${awsuser}@${node} $cmd_dump &
        i=`expr $i + 1`
    done
    wait
    info_log "END DUMP $index"
done

## Eileen add 2016-12-08:
## fetch log files back to admin node and generate a report for the extraction process
dir_name=`dirname $0`
log_path="${dir_name}/logs"
[[ -d $log_path ]] && rm -rf $log_path
mkdir $log_path
for node in ${nodes[*]}
do
    scp $ssh_opt -i $keypath ${awsuser}@${node}:$espath/dump_${dateStamp}_*.log $log_path
done
cat $log_path/dump_${dateStamp}_*.log | python ./dumpreport.py -n $nodes_number -u $URL 

## rename the dumped files to make them consecutive across nodes
info_log "START RENAME"
for ((m=0; m<$nodes_number; m++))
do
    scp $ssh_opt -i $keypath $renamescript ${awsuser}@${nodes[$m]}:${toolpath}
    cmd_rename="sh ${toolpath}/${renamescript} $datapath $m"
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$m]} $cmd_rename &
done
wait
info_log "END RENAME"

## do backup job: clear previous backups and make backup for files dumped this time;
## remove query file on each node
for node in ${nodes[*]}
do
    ssh $ssh_opt -i $keypath ${awsuser}@${node} "[[ -d $backupdir ]] && rm -rf $backupdir"
    ssh $ssh_opt -i $keypath ${awsuser}@${node} "mv $data_dir $backupdir"
#    ssh $ssh_opt -i $keypath ${awsuser}@${node} "[[ -f $querypath ]] && rm -f $querypath"
done
