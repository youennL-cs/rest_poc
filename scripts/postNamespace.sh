#!/bin/bash

# Create a namespace "nstest" own by "root"
curl -X 'POST' \
    'http://localhost:8181/v1/namespaces' \
    -H 'accept: application/json' \
    -H 'Content-Type: application/json' \
    -d '{
    "namespace": [
        "nstest"
    ],
    "properties": {
        "owner": "root"
    }
    }'