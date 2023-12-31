#!/bin/bash

TABLENAME="test"
NAMESPACE="nstest"
AWSBUCKET="cs-tmp/ylebras"

# create a table "test" where data/ & metadata/ are located in "s3://cs-tmp/ylebras/nstest"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces/'${NAMESPACE}'/tables' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
		"name": "'$TABLENAME'",
		"location": "s3://'${AWSBUCKET}'/'${NAMESPACE}'",
		"schema": {
			"type": "struct",
			"schema-id": 0,
			"fields": [
				{
                    "id": 1,
                    "field-id":1000,
                    "name": "id",
                    "type": "int",
                    "required": true,
                    "doc": "This is an ID - what did you expect"
                }
				]
			},
			"stage-create": false,
			"properties": {
			  "owner": "root"
			}
		}'