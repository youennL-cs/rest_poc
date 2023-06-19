#!/bin/bash

# Create a namespace "gotest" own by "Hank Bendickson"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "namespace": [
        "gotest"
    ],
    "properties": {
        "owner": "Hank Bendickson"
    }
    }'