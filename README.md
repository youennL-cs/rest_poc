# rest_poc

## Intro
This repos gather all you need to test the REST API.

## To test it you can run the following command
$ cd docker-compose
$ docker-compose up -d
$ cd ../scripts/
$ python3 toAvro.py
$ go run .
// take the table uid and use it in the next command
$ ./commitSnapshot.sh "<table-uuid>" <snapeshot-id>

$ docker exec -it clickhouse clickhouse client
:) create table test (`id` UInt32) ENGINE = Iceberg('https://cs-tmp.s3.eu-west-1.amazonaws.com/ylebras/gotest')
...
:) select * from test
