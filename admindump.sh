#!/bin/bash
#
# $1 - cluster_URL:port
# $2 - name of data directory
# $3 - number of nodes
#
# Last updated: Dec 12, 2016
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


test ! -z "$1" || usage $0
test ! -z "$2" || usage $0

URL="$1"
data_dir="${espath}/$2"
shift 2

#if [[ -z "$@" ]] || [[ "$@" == "all" ]]
#then
#    fields="all"
#else
#    fields="$@"
#fi

while getopts 'af:s:b:q' OPTION
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
        hasquery="Y"
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
binnum_arr=(`python binnumchunk.py -n$nodes_number`)

## get the latest batch ID from s3://tr-search-data/1.5/dev/incrementals/ if not provided as an argument
if [ "$batchid" == "" ]
then
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[0]} "[[ -d $toolpath ]] || mkdir -p $toolpath"
    scp $ssh_opt -i $keypath $batchidscript ${awsuser}@${nodes[0]}:${toolpath}
    batchid=$(ssh $ssh_opt -i $keypath ${awsuser}@${nodes[0]} "sh ${toolpath}/${batchidscript}")
fi
datapath="${data_dir}/${batchid}"

## deploy necessary directories and scripts to each node
for ((n=0; n<$nodes_number; n++))
do
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $toolpath ]] || mkdir -p $toolpath"
    scp $ssh_opt -i $keypath $dumpscript ${awsuser}@${nodes[$n]}:${toolpath}
    if [ "$hasquery" == "Y" ]
    then
        scp $ssh_opt -i $keypath $queryfile ${awsuser}@${nodes[$n]}:${toolpath}
    fi
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $datapath ]] || mkdir -p $datapath"
    #ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"binnum_range: ${binnum_arr[$n]}\" > $espath/dump_${dateStamp}_${n}.log"
    ## Eileen modify 2016-12-08:
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"host: ${nodes[$n]}\"; echo \"binnum_range: ${binnum_arr[$n]}\" > $espath/dump_${dateStamp}_${n}.log"
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
        cmd_dump="echo ${binnum_arr[$i]} | python ${toolpath}/${dumpscript} -u $URL -i $index -p $datapath -f $fields -q $queryfile >> $espath/dump_${dateStamp}_${i}.log"
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

## upload the dumped files to S3 storage
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

## do backup job: make backup for dumped files and remove 'data' folder;
## remove query file on each node
for node in ${nodes[*]}
do
    ssh $ssh_opt -i $keypath ${awsuser}@${node} "[[ -d $backupdir ]] || mkdir -p $backupdir"
    cmd_backup="mv $data_dir/* $backupdir && rm -rf $data_dir"
    cmd_rmquery="[[ -f ${toolpath}/${queryfile} ]] && rm -f ${toolpath}/${queryfile}"
    ssh $ssh_opt -i $keypath ${awsuser}@${node} $cmd_backup
    ssh $ssh_opt -i $keypath ${awsuser}@${node} $cmd_rmquery
done
