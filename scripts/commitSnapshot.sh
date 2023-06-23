#!/bin/bash

if [[ $# -lt 2 ]];
then 
    echo "you must provide the table id & snapeshot-id as follow"
    echo "commitSnapshot.sh \"<table uuid>\" \"<snapshot-id>\""
    exit 1
fi

TABLEID=$1
SNAPID=$2
# for the first commit, parent if is -1
PARENTID=-1
MANIFESTID="0"
SNAPSHOTID=null

# in this logic we consider parent id as the previous number
if [[ $SNAPID -gt 1 ]];
then 
    PARENTID="$(($SNAPID - 1))"
    MANIFESTID="$(($SNAPID - 1))"
    SNAPSHOTID=$PARENTID
fi

TIMESTAMP=`echo '('\`date +"%s.%N"\` ' * 1000)/1' | bc`
FILESIZE=476
TOTALFILESSIZE=$((${FILESIZE} * ${SNAPID}))

echo "added-files-size:" ${FILESIZE}
echo "total-records:" ${SNAPID}
echo "total-files-size:" ${TOTALFILESSIZE}
echo "total-data-files:" ${SNAPID}
echo "---"
echo "TABLEID=" $TABLEID
echo "SNAPID=" $SNAPID
echo "PARENTID=" $PARENTID
echo "MANIFESTID=" $MANIFESTID
echo "SNAPSHOTID=" $SNAPSHOTID
echo "TIMESTAMP=" $TIMESTAMP
echo "FILESIZE=" $FILESIZE
echo "TOTALFILESSIZE=" $TOTALFILESSIZE


# Commit the first snapshot 0.avro / snap-0.avro / gotest_upld_0.parquet files
# in namespace "nstest" / table "test"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces/nstest/tables/test' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "requirements": [
    {
        "type": "assert-table-uuid", 
        "uuid": "'${TABLEID}'"
    },
    {
        "type": "assert-ref-snapshot-id",
        "ref": "main",
        "snapshot-id": '${SNAPSHOTID}'
    }
    ],
    "updates": [
    {
        "action": "add-snapshot",        
        "snapshot": {
            "snapshot-id": '${SNAPID}',
            "parent-snapshot-id": '${PARENTID}',
            "timestamp-ms": '${TIMESTAMP}',
            "manifest-list": "s3://cs-tmp/ylebras/nstest/metadata/snap-'${MANIFESTID}'.avro",
            "schema-id": 0,
            "summary": {
                "operation": "append",
                "added-data-files": "1",
                "added-records": "10",
                "added-files-size": "'${FILESIZE}'",
                "changed-partition-count": "0",
                "total-records": "'${SNAPID}'",
                "total-files-size": "'${TOTALFILESSIZE}'",
                "total-data-files": "'${SNAPID}'",
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0"
            }
        }
    },
    {
        "action": "set-snapshot-ref",
        "type": "branch",
        "snapshot-id": '${SNAPID}',
        "ref-name": "main"
    }
    ]
}'