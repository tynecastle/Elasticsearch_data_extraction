#!/bin/bash
#
# $1 - cluster_URL:port
# $2 - name of data directory
# $3 - number of nodes

ssh_opt="-o LogLevel=quiet -o StrictHostKeyChecking=no"
keypath="KP_infrastructure.pem"
awsuser="ec2-user"
espath="/opt/reuters/data/elasticsearch"
toolpath="${espath}/esdumptool"
dumpscript="esdump.py"
renamescript="rename.sh"
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
        echo "$1 <URL> <data_dir> <no. of nodes>"
    } >&2
    exit 1
}


test ! -z "$1" || usage $0
test ! -z "$2" || usage $0
test ! -z "$3" || usage $0

URL="$1"
datapath="${espath}/$2"
nfn=$3
shift 3

if [[ -z "$@" ]] || [[ "$@" == "all" ]]
then
    fields="all"
else
    fields="$@"
fi

nodes_number=`sh dumpnodes.sh -l|wc -l`
if [ $nodes_number -ne $nfn ]
then
    echo "The number of nodes you provided does not match the list size in .nodes !"
    echo "Please double check and provide the correct nodes list!"
    exit 1
fi

nodes=(`sh dumpnodes.sh -l`)
binnum_arr=(`python binnumchunk.py -n$nodes_number`)

for ((n=0; n<$nodes_number; n++))
do
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $toolpath ]] || mkdir $toolpath"
    scp $ssh_opt -i $keypath $dumpscript ${awsuser}@${nodes[$n]}:${toolpath}
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "[[ -d $datapath ]] || mkdir $datapath"
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$n]} "echo \"binnum_range: ${binnum_arr[$n]}\" > $espath/dump_${dateStamp}_${n}.log"
done

for index in `sh dumpindices.sh -l`
do
    info_log "START DUMP $index"
    i=0
    for node in ${nodes[*]}
    do
        cmd_dump="echo ${binnum_arr[$i]} | python ${toolpath}/${dumpscript} -u $URL -i $index -p $datapath -f $fields >> $espath/dump_${dateStamp}_${i}.log"
        ssh $ssh_opt -i $keypath ${awsuser}@${node} $cmd_dump &
        i=`expr $i + 1`
    done
    wait
    info_log "END DUMP $index"
done

info_log "START RENAME"
for ((m=0; m<$nodes_number; m++))
do
    scp $ssh_opt -i $keypath $renamescript ${awsuser}@${nodes[$m]}:${toolpath}
    cmd_rename="sh ${toolpath}/${renamescript} $datapath $m"
    ssh $ssh_opt -i $keypath ${awsuser}@${nodes[$m]} $cmd_rename &
done
wait
info_log "END RENAME"
