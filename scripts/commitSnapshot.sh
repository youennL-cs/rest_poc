#!/bin/bash

if [[ $# -lt 4 ]];
then 
    echo "you must provide the table id & snapeshot-id as follow"
    echo "commitSnapshot.sh \"<table name>\" \"<table uuid>\" \"<table number>\" \"<snapshot-id>\""
    echo "    - \"table number\": 0 without partition / 1 with partition"
    exit 1
fi
TABLE_NAME=$1
TABLE_ID=$2
TABLE_PART=$3
SNAP_ID=$4
# for the first commit, parent if is -1
PARENT_ID=-1
MANIFEST_ID=${TABLE_PART}"0"
SNAPSHOT_ID=null

# in this logic we consider parent id as the previous number
if [[ $SNAP_ID -gt 1 ]];
then 
    PARENT_ID="$(($SNAP_ID - 1))"
    MANIFEST_ID=${TABLE_PART}"$(($SNAP_ID - 1))"
    SNAPSHOT_ID=$PARENT_ID
fi

TIMESTAMP=`echo '('\`date +"%s.%N"\` ' * 1000)/1' | bc`
FILE_SIZE=476
TOTAL_FILES_SIZE=$((${FILE_SIZE} * ${SNAP_ID}))

echo "added-files-size:" ${FILE_SIZE}
echo "total-records:" ${SNAP_ID}
echo "total-files-size:" ${TOTAL_FILES_SIZE}
echo "total-data-files:" ${SNAP_ID}
echo "---"
echo "TABLE_NAME=" $TABLE_NAME
echo "TABLE_ID=" $TABLE_ID
echo "TABLE_PART=" $TABLE_PART
echo "SNAP_ID=" $SNAP_ID
echo "PARENT_ID=" $PARENT_ID
echo "MANIFEST_ID=" $MANIFEST_ID
echo "SNAPSHOT_ID=" $SNAPSHOT_ID
echo "TIMESTAMP=" $TIMESTAMP
echo "FILE_SIZE=" $FILE_SIZE
echo "TOTAL_FILES_SIZE=" $TOTAL_FILES_SIZE


# Commit the first snapshot ${TABLE_PART}0.avro / snap-${TABLE_PART}0.avro / gotest_upld_0.parquet files
# in namespace "nstest" / table "test"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces/nstest/tables/'$TABLE_NAME'' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "requirements": [
    {
        "type": "assert-table-uuid", 
        "uuid": "'${TABLE_ID}'"
    },
    {
        "type": "assert-ref-snapshot-id",
        "ref": "main",
        "snapshot-id": '${SNAPSHOT_ID}'
    }
    ],
    "updates": [
    {
        "action": "add-snapshot",        
        "snapshot": {
            "snapshot-id": '${SNAP_ID}',
            "parent-snapshot-id": '${PARENT_ID}',
            "timestamp-ms": '${TIMESTAMP}',
            "manifest-list": "s3://cs-tmp/ylebras/nstest/'${TABLE_NAME}'/metadata/snap-'${MANIFEST_ID}'.avro",
            "schema-id": 0,
            "summary": {
                "operation": "append",
                "added-data-files": "1",
                "added-records": "10",
                "added-files-size": "'${FILE_SIZE}'",
                "changed-partition-count": "0",
                "total-records": "'${SNAP_ID}'",
                "total-files-size": "'${TOTAL_FILES_SIZE}'",
                "total-data-files": "'${SNAP_ID}'",
                "total-delete-files": "0",
                "total-position-deletes": "0",
                "total-equality-deletes": "0"
            }
        }
    },
    {
        "action": "set-snapshot-ref",
        "type": "branch",
        "snapshot-id": '${SNAP_ID}',
        "ref-name": "main"
    }
    ]
}'