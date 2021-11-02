from google.oauth2 import service_account
from google.cloud import bigquery as bq
import os
import log
import datetime
import pytz
def export_js_to_bq(js,tableid,key_path,dataset_id,LOG_FILE):
    loger=log.get_logger(__name__,LOG_FILE)
    table_id = tableid
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    bigquery_client = bq.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    job_config = bq.LoadJobConfig()
    # Schema autodetection enabled
    job_config.autodetect = True
    # Skipping first row which correspnds to the field names
    # Format of the data in GCS
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    loger.info('Starting uploading data. Record count {}'.format(len(js)))
    job = bigquery_client.load_table_from_json(js, table_ref, job_config=job_config)
    # load_job = bigquery_client.load_table_from_file(csvfile,table_ref,job_config)
    loger.info(f'Starting job {job.job_id}')
    loger.info(f'Loading data into the BQ table {table_id}')
    job.result()  # Waits for table load to complete.

def CreateDataSet(dataset_id,key_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bigquery_client = bq.Client()
    dataset = bq.Dataset('{}.{}'.format(credentials.project_id,dataset_id))

    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = bigquery_client.create_dataset(dataset)  # Make an API request.
    print("Created dataset {}.{}".format(bigquery_client.project, dataset.dataset_id))

def DeleteTable(table_id,dataset_id,key_path):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

    bigquery_client = bq.Client()
    #dataset = bq.Dataset('{}.{}'.format(credentials.project_id,dataset_id))
    fulltableid=f'{credentials.project_id}.{dataset_id}.{table_id}'
    # Send the dataset to the API for creation.
    # Raises google.api_core.exceptions.Conflict if the Dataset already
    # exists within the project.
    dataset = bigquery_client.delete_table(fulltableid)  # Make an API request.
    print(f"Delete table {fulltableid}")

def GetMaxRecord(table_id,dataset_id,key_path,fieldname,ozonid):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f'{credentials.project_id}.{dataset_id}.{table_id}'
    bigquery_client = bq.Client()
    try:
        query = f'SELECT Max({fieldname}) as Max{fieldname}  FROM `{fulltableid}` where ozon_id={ozonid}'
        job_query = bigquery_client.query(query, project=credentials.project_id)
        results=job_query.result()
        maxdate=datetime.date(1,1,1)
        for row in results:
            maxdate=row['Max'+fieldname]
            break
    except Exception as e:
        maxdate = datetime.datetime(1, 1, 1)
    maxdate = maxdate.replace(tzinfo=pytz.UTC)
    return maxdate

def DeleteOldReport(DateImport,dataset_id,key_path,fieldname,table_id,ozonid):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = key_path
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    fulltableid = f'{credentials.project_id}.{dataset_id}.{table_id}'
    bigquery_client = bq.Client()
    results=0
    try:
        query = f'Delete FROM `{fulltableid}` where {fieldname}>="{DateImport.strftime("%Y-%m-%d")}" and ozon_id="{ozonid}"'
        job_query = bigquery_client.query(query, project=credentials.project_id)
        results=job_query.result()
    except Exception as e:
        print(e)
    return results

