package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"

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
	Id int32 `parquet:"name=id, type=INT32, encoding=PLAIN"`
}

func createNamespace(ns string) {
	client := &http.Client{}
	var data = strings.NewReader(fmt.Sprintf(`{
		"namespace": [
			"%s"
		],
		"properties": {
			"owner": "Hank Bendickson"
		}
	}`, ns))

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
	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("%s\n", bodyText)
	fmt.Println("=== createNamespace Done")
}

func createTable(name string, ns string, fields string) {
	client := &http.Client{}
	var data = strings.NewReader(fmt.Sprintf(`{
		"name": "%s",
		"location": "s3://%s%s",
		"schema": {
			"type": "struct",
			"schema-id": 0,
			"fields": [
				%s
				]
			},
			"stage-create": false,
			"properties": {
			  "owner": "Hank Bendickson"
			}
		  }`,
		name, aws_bucket, ns, fields))
	// fmt.Println("=== DATA ====\n", data)
	req, err := http.NewRequest("POST", "http://localhost:8181/v1/namespaces/"+ns+"/tables", data)
	if err != nil {
		log.Fatal("POST Error:", err)
	}

	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	log.Println("------------------")
	log.Println(req)
	log.Println("------------------")

	resp, err := client.Do(req)
	if err != nil {
		log.Fatal(err)
	}
	defer resp.Body.Close()
	bodyText, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		log.Fatal("Do Error:", err)
	}
	fmt.Printf("Response: %s\n", bodyText)
	fmt.Println("=== createTable Done")
}

// generate 10 lines of Fields in path/pqFile
func generateParquetEntry(path, pqFile string, startAt int) {
	var err error
	fw, err := local.NewLocalFileWriter(path + pqFile)
	if err != nil {
		log.Println("Can't create local file", err)
		return
	}
	defer fw.Close()

	//write
	pw, err := writer.NewParquetWriter(fw, new(Fields), 4)
	if err != nil {
		log.Println("Can't create parquet writer", err)
		return
	}

	pw.RowGroupSize = 128 * 1024 * 1024 //128M
	pw.PageSize = 8 * 1024              //8K
	pw.CompressionType = parquet.CompressionCodec_SNAPPY
	num := startAt + 10
	for i := startAt; i < num; i++ {
		flds := Fields{
			Id: int32(i),
		}
		if err = pw.Write(flds); err != nil {
			log.Println("Write error", err)
		}
	}
	if err = pw.WriteStop(); err != nil {
		log.Println("WriteStop error", err)
		return
	}

	fmt.Println("=== generateParquetEntry Done")
}

func uploadFileOnS3(fromPath string, fromFName string, destPath string, destFName string) {
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

	fmt.Printf("Successfully uploaded %q to %q\n", fromFName, aws_bucket)
}

func main() {
	// Step 1 - creation of the namespace 'gotest',
	// can be commented if exists.
	createNamespace("gotest")

	// Step 2 - creation of the table 'test' in the namesapce 'gotest'
	// can be commented if exists
	createTable(
		"test",
		"gotest",
		`{
			"id": 1,
			"field-id":1000,
			"name": "id",
			"type": "int",
			"required": true,
			"doc": "This is an ID - what did you expect"
		}`,
	)

	nbCommits := 1
	for i := 0; i < nbCommits; i++ {
		// Step 3 - build parquet files with 10 rows of data
		// can be commented if already existing
		generateParquetEntry("./output/", "gotest_"+strconv.Itoa(i)+".parquet", i*10)

		//TODO
		// Call python script to generate corresponding manifests

		// Step 4 - Upload parquet files on S3
		uploadFileOnS3("./output/", "gotest_"+strconv.Itoa(i)+".parquet", "gotest/data/", "gotest_upld_"+strconv.Itoa(i)+".parquet")

		// Step 4 - Upload manifest Files on S3
		uploadFileOnS3("./output/", strconv.Itoa(i)+".avro", "gotest/metadata/", strconv.Itoa(i)+".avro")

		// Step 4 - Upload manifest Lists on S3
		uploadFileOnS3("./output/", "snap-"+strconv.Itoa(i)+".avro", "gotest/metadata/", "snap-"+strconv.Itoa(i)+".avro")

		// TODO
		// Step 5 - post commit
		// 1. when curl to create table -> get table uuid (write it somewhere and keep it for next commits)
		// 2. curl post each commit
	}

	fmt.Println("=== Main Done")
}
