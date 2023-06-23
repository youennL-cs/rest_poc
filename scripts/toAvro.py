import avro.schema
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def manifestFile(idx):
    print("Create "+str(idx)+".avro")
    schema = avro.schema.parse(open("manifestFile.avsc", "rb").read())

    writer = DataFileWriter(open("./output/"+str(idx)+".avro", "wb"), DatumWriter(), schema)
    writer.append({
        "data_file": {
            "content": 1,
            "file_path": "s3://cs-tmp/ylebras/nstest/data/gotest_upld_"+str(idx)+".parquet",
            "file_format": "PARQUET",
            "file_size_in_bytes": 476,
            # "partition": {},
            "record_count": 10,
        },
        "snapshot_id": idx+1,
        "status": 1
    })

    writer.close()

    reader = DataFileReader(open("./output/"+str(idx)+".avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()

def manifestList(idx):
    print("Create snap-"+str(idx)+".avro")
    schema = avro.schema.parse(open("manifestList.avsc", "rb").read())

    writer = DataFileWriter(open("./output/snap-"+str(idx)+".avro", "wb"), DatumWriter(), schema)
    writer.append({
        "manifest_path": "s3://cs-tmp/ylebras/nstest/metadata/"+str(idx)+".avro",
        "manifest_length": 544,
        # "partition_spec_id": 0,
        "added_snapshot_id": idx+1,
        "added_data_files_count": 1,
        "existing_data_files_count": idx,
        "deleted_data_files_count": 0,
        # "content": 0,
        "added_data_rows_count": 10,
        "existing_rows_count": idx*10,
        "deleted_rows_count": 0,
    })

    writer.close()

    reader = DataFileReader(open("./output/snap-"+str(idx)+".avro", "rb"), DatumReader())
    for user in reader:
        print(user)
    reader.close()


def main():
    manifestFile(0)
    manifestFile(1)
    manifestList(0)
    manifestList(1)

if __name__ == "__main__":
    main()