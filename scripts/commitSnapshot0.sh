#!/bin/bash

if [[ $# -lt 2 ]];
then 
    echo "you must provide the table id and the timestamp"
    return
fi

# Commit the first snapshot 0.avro / snap-0.avro / gotest_upld_0.parquet files
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces/gotest/tables/test' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "requirements": [
        {
        "type": "assert-table-uuid",
        "ref": "main",
        "uuid": "$1",
        "snapshot-id": 1, 
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
        "snapshot-id": 1,
        "parent-snapshot-id": -1,
        "timestamp-ms": $2,
        "manifest-list": "s3://cs-tmp/ylebras/gotest/metadata/snap-0.avro",
        "summary": {
            "operation": "append"
        }
        }
    },
    {
        "action": "set-snapshot-ref",
        "type": "branch",
        "snapshot-id": 1,
        "max-ref-age-ms": 3000000000000,
        "max-snapshot-age-ms": 3000000000000,
        "min-snapshots-to-keep": 10,
        "ref-name": "main"
    }
    ]}'