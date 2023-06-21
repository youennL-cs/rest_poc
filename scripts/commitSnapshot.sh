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
PARENTID="-1"
MANIFESTID="0"

# in this logic we consider parent id as the previous number
if [[ $SNAPID -gt 1 ]];
then 
    PARENTID="$(($SNAPID - 1))"
    MANIFESTID="$(($SNAPID - 1))"
fi

echo $PARENTID "\n"

TIMESTAMP=`echo '('\`date +"%s.%N"\` ' * 1000)/1' | bc`

# Commit the first snapshot 0.avro / snap-0.avro / gotest_upld_0.parquet files
# in namespace "gotest" / table "test"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces/gotest/tables/test' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "requirements": [
        {
        "type": "assert-table-uuid", 
        "ref": "main",
        "uuid": "'${TABLEID}'",
        "snapshot-id": '${SNAPID}', 
        "last-assigned-field-id": 1,
        "current-schema-id": 0,
        "last-assigned-partition-id": 0,
        "default-spec-id": 0,
        "default-sort-order-id": 0
        }
    ],
    "updates": [
    {
        "action": "add-snapshot",        
        "snapshot": {
        "snapshot-id": '${SNAPID}',
        "parent-snapshot-id": -1,
        "timestamp-ms": '${TIMESTAMP}',
        "manifest-list": "s3://cs-tmp/ylebras/gotest/metadata/snap-'${MANIFESTID}'.avro",
        "summary": {
            "operation": "append"
        }
        }
    },
    {
        "action": "set-snapshot-ref",
        "type": "branch",
        "snapshot-id": '${SNAPID}',
        "max-ref-age-ms": 3000000000000,
        "max-snapshot-age-ms": 3000000000000,
        "min-snapshots-to-keep": 10,
        "ref-name": "main"
    }
    ]}'