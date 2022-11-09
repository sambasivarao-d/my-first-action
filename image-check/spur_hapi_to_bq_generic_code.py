import os
from flask import Flask,request
import requests
import json
import time, datetime
import http.client
from google.cloud import storage, bigquery
import hapi_bq_schema_validation
from io import StringIO
import logging
import google_crc32c
from google.cloud import secretmanager

# environment variables
project_id = os.environ.get("project_id" ,None)
hapi_endpoint_url =  os.environ.get("hapi_endpoint_url" , None)
auth_server_url = os.environ.get("auth_server_url" , None)
grant_type = os.environ.get("grant_type" , None)
client_id = os.environ.get("client_id" , None) 
#client_secret = os.environ.get("client_secret" , None) 
scope = os.environ.get("scope" , None)
hapi_ad_url = os.environ.get("hapi_ad_url" , None)
#api_subscription_key = os.environ.get("api_subscription_key" , None)
spur_gcs__data_bucket = os.environ.get("spur_gcs__data_bucket" , None)
gcs_dir = os.environ.get("gcs_dir" , None)
bq_table = os.environ.get("bq_table" , None)
failed_record_table = os.environ.get("failed_record_table" ,None)
schema_validation_switch = os.environ.get("schema_validation_switch" ,None)
file_name_prefix = os.environ.get("file_name_prefix" ,None)
integration_name = os.environ.get("integration_name" ,None)
schema_bucket = os.environ.get("schema_bucket" , None)
bq_schema_file = os.environ.get("bq_schema_file" , None)
validation_schema_file = os.environ.get("validation_schema_file" , None)
#print("ENV variables block")
#HapiToBqFunc function load the data received from HAPI test portal to Bigquery and GCS file.

app = Flask(__name__)
@app.route('/hapiTobqBatchPoc', methods=['GET'])
def HapiToBqFunc():
    print("inside code function")
    try: 
        print("inside try block")
        client_secret = access_secret_version(project_id, "spur_client_secret","1")
        api_subscription_key = access_secret_version(project_id, "spur_api_subscription_key","1")       
        #Get AUTH token for HAPI Endpoint.
        print("secret value taken")
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        token_req_payload = {'grant_type': grant_type,'client_id' :client_id,
            'client_secret':client_secret,'scope':scope}
        token_response = requests.post(auth_server_url,data=token_req_payload,headers=headers)
        tokens = json.loads(token_response.text)
        logging.info(tokens['access_token'])
        token = tokens['access_token']
        print("received token")
        #Set up connection with generic HAPI url.    
        conn = http.client.HTTPSConnection(hapi_ad_url)
        payload = ''
        api_call_headers = {'Authorization': 'Bearer '+token , 'Ocp-Apim-Subscription-Key':api_subscription_key}
        conn.request("GET", hapi_endpoint_url, payload, api_call_headers)

        #Get HAPI response and load it to json format.
        res = conn.getresponse()
        data = res.read()
        print("received HAPI response")
        date_time = datetime.datetime.now()
        json_data = json.loads(data)
        json_records = json_data["value"]
        print("load json value type      -- : " , type(json_records))
        jsonData = [json.dumps(record) for record in json_data["value"]]
        if (schema_validation_switch.lower() == 'true'):
            print(" switch type >>>>>>>>" , type(schema_validation_switch))
            print("I AM IN SCHEMA VALIDATION CODE")
            schema_validate_stat = hapi_bq_schema_validation.validate(json_records,schema_bucket,validation_schema_file)
            print("I am schema status :    " , schema_validate_stat)
            if(schema_validate_stat == True):
				#Storage Client for JSON to GCS File Load.
                response_body = HAPI_to_GCS(jsonData)
                logging.info(response_body)
				#Bigquery Client for JSON records to BQ Load.
                response = GCS_to_BQ(json_records)
            else:
                logging.info("JSON Payload is not correct")
                json_string = str(json_records)
                data = json_string
                response = failedRecordsInsert(data)
        else:
            print("I AM NOT IN SCHEMA VALIDATION CODE")
            #Storage Client for JSON to GCS File Load.
            response_body = HAPI_to_GCS(jsonData)
            logging.info(response_body)
            #Bigquery Client for JSON records to BQ Load.
            response = GCS_to_BQ(json_records)

    except Exception as e:
        print(str(e))
        logging.info(str(e))
        response = "I am in Exception block   :    " + str(e)

    return response
def access_secret_version(project_id, secret_id, version_id):
    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_id}/versions/{version_id}"
        response = client.access_secret_version(request={"name": name})
        crc32c = google_crc32c.Checksum()
        crc32c.update(response.payload.data)
        if response.payload.data_crc32c != int(crc32c.hexdigest(), 16):
            print("Data corruption detected.")
            return response
        payload = response.payload.data.decode("UTF-8")
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        payload = "I am in access_secret_version exception block   :    " + str(e)
    return payload

def HAPI_to_GCS(jsonData):
    try:
        #Storage Client for JSON to GCS File Load.
        storage_client = storage.Client()
        bucket = storage_client.get_bucket(spur_gcs__data_bucket)
        timestr = time.strftime("%Y-%m-%d")
        blob = bucket.blob(gcs_dir + "/" + timestr +"/" + file_name_prefix + "_" + timestr + ".json")
        result = jsonData
        blob.upload_from_string(
            data= '\n'.join(result),
            content_type='application/json'
            )
        date_time = datetime.datetime.now()
        result = 'SPUR_HAPI_batch_Certificates data upload complete'
        response_body  =  {"message" : result, 
                            "Date&Time": date_time}
        logging.info(response_body)
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        response_body = "I am in HAPI_to_GCS exception block   :    " + str(e)
    return response_body
	
def GCS_to_BQ(json_records):
    try:
        #Bigquery Client for JSON records to BQ Load.
        client = bigquery.Client()
        table_id = bq_table
        bq_schema=getBQSchemaFunc()
        job_config = bigquery.LoadJobConfig(
            autodetect=False,
            schema=bq_schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
            )
        bq_ts = datetime.datetime.now().isoformat() + "Z"
        for dict in json_records:
            dict['bqTimestamp'] = bq_ts
        load_job = client.load_table_from_json(json_records, table_id, job_config = job_config)
        load_job.result()  # Waits for the job to complete.
        destination_table = client.get_table(table_id)
        response = "Loaded {} rows.".format(destination_table.num_rows)
        logging.info(response)
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        response = "I am in GCS_to_BQ exception block   :    " + str(e)
    return response

#Bigquery Load for Failed records.
def failedRecordsInsert(data):
    try:
        client = bigquery.Client()
        current_ts = time.strftime("%Y-%m-%d %H:%M:%S")
        rows_to_insert = [{"projectName": project_id, "gcsUri": "gs://" + spur_gcs__data_bucket + "/" + gcs_dir , "ipName" : integration_name  ,"failedPayload":data, "errorMsg" : "SUPUCertificate JSON is not correct" , "ingestionTimestamp": current_ts},]
        BQ_status = client.insert_rows_json(failed_record_table, rows_to_insert)  # Make an API request.
        response = ""
        if BQ_status == []:
            response = "New rows have been added to BQ Failed Records table."
        else:
            response = "Encountered errors while inserting rows: {}".format(BQ_status)
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        response = "I am in failedRecordsInsert exception block   :    " + str(e)
    return response

def getBQSchemaFunc():
    try:
        #bucket_name = "bucket-supucertificate-supplier-spur-system-d"
        #filename = "schema/v1/bq_schema.json"
        read_storage_client = storage.Client()
        bucket = read_storage_client.get_bucket(schema_bucket)
        blob = bucket.get_blob(bq_schema_file)
        json_data_string = blob.download_as_string()
        bq_schema_json = json.loads(json_data_string)
        print("bq_schema json >>>>" , bq_schema_json)
        bigquerySchema = []
        for col in bq_schema_json:
            bigquerySchema.append(bigquery.SchemaField(col['name'], col['type'] , col['mode']))
        response = bigquerySchema
    except Exception as e:
        print(str(e))
        logging.info(str(e))
        response = "I am in getBQSchemaFunc exception block   :    " + str(e)
    return response

if __name__ == "__main__":
    print("I am in flask MAIN code block")
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))