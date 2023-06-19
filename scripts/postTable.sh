#!/bin/bash

# create a table "test" where data/ & metadata/ are located in "s3://cs-tmp/ylebras/gotest"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
		"name": "test",
		"location": "s3://cs-tmp/ylebras/gotest",
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
			  "owner": "Hank Bendickson"
			}
		}'