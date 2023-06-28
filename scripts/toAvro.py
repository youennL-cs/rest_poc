import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter
from os import stat
import argparse
import calendar
import time

def manifestFile(table, partitioned, idx):
    part = "1" if (partitioned) else "0"

    print("Create "+str(idx)+".avro")
    schema = avro.schema.parse(open("manifestFile.avsc", "rb").read())

    writer = DataFileWriter(open("./output/"+part+str(idx)+".avro", "wb"), DatumWriter(), schema)
    dataFileName = "test_"+part+str(idx)+".parquet"
    file_stats = stat("./output/"+dataFileName)

    writer.append({
        "data_file": {
            "content": 1,
            "file_path": "s3://cs-tmp/ylebras/nstest/"+table+"/data/" + dataFileName,
            "file_format": "PARQUET",
            "partition": {
                "source-id": 1,
                "field-id": 2,
                "name": "date",
                "transform": "day"
            },
            "file_size_in_bytes": file_stats.st_size,
            "record_count": 10,
        },
        "snapshot_id": idx+1,
        "status": 1
    })

    writer.close()

    reader = DataFileReader(open("./output/"+part+str(idx)+".avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()

def manifestList(table, partitioned, idx):
    part = "1" if (partitioned) else "0"

    print("Create snap-"+part+str(idx)+".avro")
    schema = avro.schema.parse(open("manifestList.avsc", "rb").read())

    writer = DataFileWriter(open("./output/snap-"+part+str(idx)+".avro", "wb"), DatumWriter(), schema)
    for j in range(idx+1):
        manifestFile = part+str(j)+".avro"
        file_stats = stat("./output/"+manifestFile)

        writer.append({
            "manifest_path": "s3://cs-tmp/ylebras/nstest/"+table+"/metadata/" + manifestFile,
            "manifest_length": file_stats.st_size,
            "partition_spec_id": 0,
            "added_snapshot_id": j+1,
            "added_data_files_count": 1,
            "existing_data_files_count": j,
            "deleted_data_files_count": 0,
            "partitions": [{"contains_null": False, "contains_nan": False, "lower_bound": bytes(str(calendar.timegm(time.gmtime())), 'utf-8'), "upper_bound": bytes(str(calendar.timegm(time.gmtime())), 'utf-8')}],
            # "content": 0,
            "added_data_rows_count": 10,
            "existing_rows_count": j*10,
            "deleted_rows_count": 0,
        })

    writer.close()

    reader = DataFileReader(open("./output/snap-"+part+str(idx)+".avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()

def main():
    parser = argparse.ArgumentParser(
                    prog='toArvo',
                    description='Create a manifest file and list according to ')
    parser.add_argument('--partitioned', action=argparse.BooleanOptionalAction)
    parser.add_argument('--id', type=int, default=0)
    parser.add_argument('--table', type=str, default=0)
    parser.set_defaults(partitioned=False)

    args = parser.parse_args()

    manifestFile(args.table, args.partitioned, args.id)
    manifestList(args.table, args.partitioned, args.id)

if __name__ == "__main__":
    main()