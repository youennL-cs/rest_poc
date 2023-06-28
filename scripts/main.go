package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	// awscreds "github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/parquet"
	"github.com/xitongsys/parquet-go/writer"
)

var aws_bucket string = "cs-tmp/ylebras/"

type Fields struct {
	Id   int32 `parquet:"name=id, type=INT32, encoding=PLAIN"`
	Date int32 `parquet:"name=date, type=INT32, convertedtype=DATE"`
}

type PostTableResponse struct {
	MetadataLocation string `json:metadata-location`
	Metadata         struct {
		TableUuid string                 `json:table-uuid`
		Location  string                 `json:location`
		X         map[string]interface{} `json:"-"` // Rest of the fields should go here.
	}
}

type Table struct {
	Name      string
	Namespace string
	Location  string
	Uid       bytes.Buffer
	Fields    string
}

func (t *Table) postNamespace(ns string) {
	// TODO: add a get first to look if table doesn't already exists
	log.Printf("=== Create Namespace %q \n", t.Namespace)
	client := &http.Client{}
	var data = strings.NewReader(fmt.Sprintf(`{
		"namespace": [
			"%s"
		],
		"properties": {
			"owner": "root"
		}
	}`, t.Namespace))

	req, err := http.NewRequest("POST", "http://localhost:8181/v1/namespaces", data)
	if err != nil {
		log.Fatal(err)
	}
	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
}

func (t *Table) postTable() {
	// TODO: add a get first to look if table doesn't already exists
	client := &http.Client{}
	var data = strings.NewReader(fmt.Sprintf(`{
		"name": "%s",
		"location": "%s",
		"schema": {
			"type": "struct",
			"schema-id": 0,
			"fields": [
				%s
				]
			},
			"stage-create": false,
			"properties": {
			  "owner": "root"
			}
		  }`,
		t.Name, t.Location, t.Fields))

	req, err := http.NewRequest("POST", "http://localhost:8181/v1/namespaces/"+t.Namespace+"/tables", data)
	if err != nil {
		log.Fatal("POST Error:", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Do Error:", err)
	}

	log.Println("Create table response: ", string(bodyText))
	// extract table-uuid from
	var tableInfo interface{}
	if err = json.Unmarshal(bodyText, &tableInfo); err != nil {
		log.Fatal("Cannot unmarshal JSON", err)
	}
	t.Uid.WriteString(tableInfo.(map[string]interface{})["metadata"].(map[string]interface{})["table-uuid"].(string))
}

// Generate 10 lines of Fields in path/pqFile
func generateParquetEntry(path, pqFile string, startAt int, part bool) {
	log.Println("=== Generate Parquet Entries from", startAt, "to", startAt+9)
	var err error
	fw, err := local.NewLocalFileWriter(path + pqFile)
	if err != nil {
		log.Fatal("Can't create local file", err)
	}
	defer fw.Close()

	//write
	pw, err := writer.NewParquetWriter(fw, new(Fields), 4)
	if err != nil {
		log.Fatal("Can't create parquet writer", err)
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.PageSize = 8 * 1024              //8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := startAt + 10
	for i := startAt; i < num; i++ {
		if part {
			flds := Fields{
				Id:   int32(i),
				Date: int32(time.Now().Unix()),
			}
			if err = pw.Write(flds); err != nil {
				log.Fatal("Write error", err)
			}
		} else {
			flds := Fields{
				Id: int32(i),
			}
			if err = pw.Write(flds); err != nil {
				log.Fatal("Write error", err)
			}
		}

	}
	if err = pw.WriteStop(); err != nil {
		log.Fatal("WriteStop error", err)
		return
	}
}

func uploadFileOnS3(fromPath string, fromFName string, destPath string, destFName string) {
	log.Println("Upload files on S3")
	file, err := os.Open(fromPath + fromFName)
	if err != nil {
		log.Fatalf("Unable to open file %q, %v", fromFName, err)
	}
	defer file.Close()

	sess, err := session.NewSession(
		&aws.Config{
			Region: aws.String("eu-west-1")},
	)
	if err != nil {
		log.Fatalf("Unable to create a new session %v", err)
	}

	uploader := s3manager.NewUploader(sess)

	_, err = uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(aws_bucket),
		Key:    aws.String(destPath + destFName),
		Body:   file,
	})
	if err != nil {
		log.Fatalf("Unable to upload %q to %q, %v", fromFName, aws_bucket, err)
	}

	log.Printf("Successfully uploaded %q to %q as %q\n", fromFName, aws_bucket, destFName)
}

// Simple tests manifests are X.avro and snap-X.avro
// and data files are in ./output/
func unpartitionTest(nbCommits int) {
	stest := Table{
		"simpletest",
		"nstest",
		"s3://" + aws_bucket + "nstest/" + "simpletest",
		bytes.Buffer{},
		`{ 
			"id": 1, 
			"field-id":1000, 
			"name": "id",
			"type": "int", 
			"required": true, 
			"doc": "This is an ID - what did you expect"
		}`,
	}

	// Step 1 - creation of the namespace 'nstest',
	// can be commented if exists.
	stest.postNamespace("nstest")

	// Step 2 - creation of the table 'test' in the namespace 'nstest'
	// can be commented if exists
	log.Println("Create \"simpletest\" table...")
	stest.postTable()

	log.Println("Create & post data & manifests on s3...")
	for i := 0; i < nbCommits; i++ {
		// Step 3 - build parquet files with 10 rows of data
		// can be commented if already existing
		generateParquetEntry("./output/", "test_0"+strconv.Itoa(i)+".parquet", i*10, false)

		// Call python script to generate corresponding manifests
		log.Println("Create manifest with python: python3 toAvro.py --table", stest.Name, "--id", strconv.Itoa(i))
		cmd := exec.Command("python3", "toAvro.py", "--table", stest.Name, "--id", strconv.Itoa(i))
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}

		// Step 4 - Upload parquet files on S3
		uploadFileOnS3("./output/", "test_0"+strconv.Itoa(i)+".parquet",
			stest.Namespace+"/"+stest.Name+"/data/", "test_0"+strconv.Itoa(i)+".parquet")

		// Step 4 - Upload manifest Files on S3
		uploadFileOnS3("./output/", "0"+strconv.Itoa(i)+".avro",
			stest.Namespace+"/"+stest.Name+"/metadata/", "0"+strconv.Itoa(i)+".avro")

		// Step 4 - Upload manifest Lists on S3
		uploadFileOnS3("./output/", "snap-0"+strconv.Itoa(i)+".avro",
			stest.Namespace+"/"+stest.Name+"/metadata/", "snap-0"+strconv.Itoa(i)+".avro")

		// Step 5 - post commit
		log.Println("Commit the " + strconv.Itoa(i) + "e change")
		cmd = exec.Command("./commitSnapshot.sh", stest.Name, stest.Uid.String(), "0", strconv.Itoa(i))
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
	}
}

// part tests manifests are 1X.avro and snap-1X.avro
// and data files are at ./output/<data file>
func partitionedTest(nbCommits int) {
	partTest := Table{
		"parttest",
		"nstest",
		"s3://" + aws_bucket + "nstest/" + "parttest",
		bytes.Buffer{},
		`{
			"id": 1,
			"field-id":1000,
			"name": "id",
			"type": "int",
			"required": true,
			"doc": "This is an ID - what did you expect"
		},{
			"id": 2,
			"field-id":1001,
			"name": "date",
			"type": "date",
			"required": true,
			"doc": "Date to partition"
		}
		`,
	}

	// Step 1 - creation of the namespace 'nstest',
	// can be commented if exists.
	partTest.postNamespace("nstest")

	// Step 2 - creation of the table 'test' in the namesapce 'nstest'
	// can be commented if exists
	log.Println("Step 2")
	partTest.postTable()

	for i := 0; i < nbCommits; i++ {
		// Step 3 - build parquet files with 10 rows of data
		// can be commented if already existing
		log.Println("Step 3")
		generateParquetEntry("./output/", "test_1"+strconv.Itoa(i)+".parquet", i*10+100, true)

		// Call python script to generate corresponding manifests
		log.Println("Create manifest with python")
		cmd := exec.Command("python3", "toAvro.py", "--table", partTest.Name, "--partitioned", "--id", strconv.Itoa(i))
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}

		log.Println("Step 4")
		// Step 4 - Upload data files (parqll ou	uet) on S3
		uploadFileOnS3("./output/", "test_1"+strconv.Itoa(i)+".parquet",
			partTest.Namespace+"/"+partTest.Name+"/data/", "test_1"+strconv.Itoa(i)+".parquet")

		// Step 4 - Upload manifest Files on S3
		uploadFileOnS3("./output/", "1"+strconv.Itoa(i)+".avro",
			partTest.Namespace+"/"+partTest.Name+"/metadata/", "1"+strconv.Itoa(i)+".avro")

		// Step 4 - Upload manifest Lists on S3
		uploadFileOnS3("./output/", "snap-1"+strconv.Itoa(i)+".avro",
			partTest.Namespace+"/"+partTest.Name+"/metadata/", "snap-1"+strconv.Itoa(i)+".avro")

		// Step 5 - post commit
		log.Println("Commit the " + strconv.Itoa(i) + "e change")
		cmd = exec.Command("./commitSnapshot.sh", partTest.Name, partTest.Uid.String(), "1", strconv.Itoa(i))
		if err := cmd.Run(); err != nil {
			log.Fatal(err)
		}
	}
}

func main() {
	log.Println("=== Run simple test...")
	// Run a simple end-to-end process where we create a namespace then a simple table with a unique field `id`.
	// Generate data file in parquet with ten elements and corresponding manifests list & file before to send everything on s3 and commit
	unpartitionTest(2)
	log.Println("=== Run partitionned test...")
	// Run an end-to-end process where we create a namespace; then a partitioned table with two fields `id` and `date` .
	// Generate data file in parquet with ten elements and corresponding manifests list & file before to send everything on s3 and commit
	partitionedTest(2)

	log.Println("=== Work done.")
}
