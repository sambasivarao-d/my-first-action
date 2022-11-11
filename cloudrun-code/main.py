import os
import logging
from googleapiclient.discovery import build
from google.cloud import storage
from google.cloud import bigquery
from fastapi import FastAPI, Request
app = FastAPI()
@app.post("/")
def root(request: Request):
    startdataprocess(request)

def startdataprocess(request):
    '''
    Method: 
        startDataflowProcess: Triggers the Dataflow job once the file is available in landing bucket.
    '''
    #params:
    #1. metadata: dict: Contains metadata for the file itself.
    #2. context: (google.cloud.functions.Context): Metadata for the event.

    # Extract enviroment variables to configure & trigger DF job:
    project_id = os.environ.get('projectid', None)
    bq_table_name = os.environ.get('tablename', None )
    destination_bucketname = os.environ.get('destinationbucketname', None)
    failed_bcktname = os.environ.get('failedbucketname', None)
    source_fileprefix = os.environ.get("sourcefileprefix", None)
    bq_location = os.environ.get("bq_region", None)


    try:
        # extract source filename prefix to validate with file prefix provided to trigger:
        print("request:" + str(request))
        source_filename = str(request.headers.get('ce-subject')).split('/')[-1]
        print("SOURCE FILE NAME", source_filename)
        source_prefix = (source_filename.split('_')[0])
        logging.info("SOURCE_FILE_NAME PREFIX" + source_prefix)
        print("SOURCE_FILE_NAME PREFIX" + source_prefix)
        logging.info("PARAMETER_FILE_NAME PREFIX" + source_fileprefix)
        print("PARAMETER_FILE_NAME PREFIX" + source_fileprefix)
        landing_bucket_name = str(request.headers.get('ce-bucket'))

        validate_vars(project_id,bq_table_name , destination_bucketname,failed_bcktname,source_fileprefix,bq_location)

        if source_prefix == source_fileprefix:
            
            inputfilepath = "gs://" + landing_bucket_name +"/"+ source_filename
            logging.info("inputFile:" + inputfilepath)
            print("inputFile:" + inputfilepath)
            # user defined parameters to pass to the dataflow pipeline job
            # init BQ client and provide job configs
            client = bigquery.Client()
            job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,)

            # load data to bq dataset
            load_job = client.load_table_from_uri(inputfilepath, bq_table_name, job_config=job_config, location=bq_location)
            result = load_job.result()
            logging.info("BQ LOAD JOB: %s .", result)
            print("BQ LOAD JOB: %s ."+ str(result))

            """
            Moving file after bqload to staging bucket.
            """
            destination_filename = "v1/"+source_filename           
            move_blob(landing_bucket_name, source_filename, destination_bucketname, destination_filename, failed_bcktname)

        else:
            move_blob(landing_bucket_name, source_filename, failed_bcktname, source_filename , failed_bcktname)

    except Exception as e:
        logging.info(str(e))
        print(str(e))

'''
Method: validate_vars: Validate the env variables: whether the values present.
'''

def validate_vars(project_id: str, bq_table_name: str,
                  destination_bucketname: str, failed_bcktname: str,source_fileprefix: str,bq_location: str) -> None:
    '''
    Validates presence of the runtime environment vars
    '''

    if project_id is None:
        raise RuntimeError('project_id not provided in env vars.')
    if bq_table_name is None:
        raise RuntimeError('bq_table_name not provided in env vars.')
    if destination_bucketname is None:
        raise RuntimeError('destination_bucketname not provided in env vars.')
    if failed_bcktname is None:
        raise RuntimeError('failed_bcktname not provided in env vars.')
    if source_fileprefix is None:
        raise RuntimeError('source_fileprefix not provided in env vars.')
    if bq_location is None:
        raise RuntimeError('bq_location not provided in env vars.')


def move_blob(bucket_name, blob_name, destination_bucket_name, destination_blob_name, failed_bcktname):
    """Moves a blob from one bucket to another"""
    # The ID of your GCS bucket
    # bucket_name = "your-source-bucket-name"
    # The ID of your GCS object
    # blob_name = "your-object-name"
    # The ID of the bucket to move the object to
    # destination_bucket_name = "destination-bucket-name"
    # The ID of your new GCS object (optional)
    # destination_blob_name = "destination-object-name"
    try:
        logging.info("File Movement function triggerd")
        print("File Movement function triggerd")
        storage_client = storage.Client()

        source_bucket = storage_client.bucket(bucket_name)
        logging.info("source_bucket" + str(source_bucket))
        print("source_bucket:" + str(source_bucket))
        source_blob = source_bucket.blob(blob_name)
        destination_bucket = storage_client.bucket(destination_bucket_name)
        logging.info("destination_bucket" + str(destination_bucket))
        print("destination_bucket : " + str(destination_bucket))

        blob_copy = source_bucket.copy_blob(
            source_blob, destination_bucket, destination_blob_name
        )
        source_bucket.delete_blob(blob_name)

        logging.info(
            "Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                source_blob.name,
                source_bucket.name,
                blob_copy.name,
                destination_bucket.name,
            )
        )
        print("Blob {} in bucket {} moved to blob {} in bucket {}.".format(
                str(source_blob.name),
                str(source_bucket.name),
                str(blob_copy.name),
                str(destination_bucket.name),
            ))
    except Exception as e:
        logging.info("error" + str(e))
        print("error" + str(e))
        blob_copy = source_bucket.copy_blob(
            source_blob, failed_bcktname, destination_blob_name
        )
        source_bucket.delete_blob(blob_name)
        logging.info("File Moved to Failed bucket")
        print("File Moved to Failed bucket")