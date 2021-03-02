import pandas as pd

def fetch_data(source_type='file',
               source_name='ospc_churn_dev.features'
               ):
    '''
    Run query on BQ, or load table name or csv file
    INPUTS:
       -- source_type
          type: str
          desc: ['table','query','file'] Tells the source type
       -- source_name
          type: str
          desc: ['bq_dataset.bq_table' if source_type = 'table',
                  BQ query if source_type = query,
                  'filename<.csv, .json, .pkl>' if source_type = 'file' ]
    RETURNS:
      -- dataframe
         type: pandas df
    '''
    import google.auth
    from google.cloud import bigquery_storage_v1beta1
    credentials, my_project_id = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    bqstorageclient = bigquery_storage_v1beta1.BigQueryStorageClient(credentials=credentials)
    
    if source_type == 'table':

        table = bigquery_storage_v1beta1.types.TableReference()
        table.project_id = my_project_id
        table.dataset_id = source_name.split('.')[0]#"ospc_churn_dev"
        table.table_id = source_name.split('.')[1]#"features"
        
        # Select columns to read with read options. If no read options are
        # specified, the whole table is read.
        read_options = bigquery_storage_v1beta1.types.TableReadOptions()
        #read_options.selected_fields.append("species_common_name")
        #read_options.selected_fields.append("fall_color")

        parent = "projects/{}".format(my_project_id)
        session = bqstorageclient.create_read_session(
            table,
            parent,
            read_options=read_options,
            # This API can also deliver data serialized in Apache Avro format.
            # This example leverages Apache Arrow.
            format_=bigquery_storage_v1beta1.enums.DataFormat.ARROW,
            # We use a LIQUID strategy in this example because we only read from a
            # single stream. Consider BALANCED if you're consuming multiple streams
            # concurrently and want more consistent stream sizes.
            sharding_strategy=(bigquery_storage_v1beta1.enums.ShardingStrategy.LIQUID),)
        
        # This example reads from only a single stream. Read from multiple streams
        # to fetch data faster. Note that the session may not contain any streams
        # if there are no rows to read.
        stream = session.streams[0]
        position = bigquery_storage_v1beta1.types.StreamPosition(stream=stream)
        reader = bqstorageclient.read_rows(position)
    
        # Parse all Avro blocks and create a dataframe. This call requires a
        # session, because the session contains the schema for the row blocks.
        dataframe = reader.to_dataframe(session)
        
    elif source_type == 'query':
        from google.cloud import bigquery
        bqclient = bigquery.Client(credentials=credentials, project=my_project_id)
        dataframe = bqclient.query(source_name).result().to_dataframe(bqstorage_client=bqstorageclient)

    elif source_type == 'file':
        if '.csv' in source_name:
            dataframe = pd.read_csv(source_name)
        elif '.pkl' in source_name:
            dataframe = pd.read_pickle(source_name)
        elif '.json' in source_name:
            dataframe = pd.read_json(source_name)
        else:
            print(f'{source_name} is not a valid filetype. valid file types: csv, pkl, json')
            return
        
    #logger.debug('Done fetching data')
    print('done fetching data')
    return dataframe


def upload2gcs(pd_data,
               bucket_name='rax-datascience-dev',
               path='cmueller/test',
               filename='myfile.pkl'
           ):
    '''
    Uploads data to GCS
    param pd_data: - the data to be uploaded
    type pd_data: pandas dataframe
    param bucket_name: the GCS bucket where the file will be uploaded
    type bucket_name: str
    param path: the path to the file (DO NOT add leading or trailing '/')
    type path: str
    param filename: the file name (with extension csv, pkl, json)
    type filename: str
    '''
    from datetime import datetime
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
    
    persist_time = str(datetime.utcnow())
    
    print('saving file locally...')
    fn = f'f{persist_time}_{filename}'.replace(' ','_').replace(':','_')
    
    if '.csv' in filename:
        pd_data.to_csv(fn,compression='gzip')
    elif '.pkl' in filename:
        pd_data.to_pickle(fn)
    elif '.json' in filename:
        pd_data.to_json(fn)
    else:
        print('Invalid file type. Valid file types: [csv, pkl, json]')
        return
  
    blb = f'{path}/{fn}'
    gcs = storage.Client()
    
    print(f'getting bucket: {bucket_name}')
    bucket = gcs.get_bucket(bucket_name)
    blob = bucket.blob(blb)
    print('uploading blob....')
    blob.upload_from_filename(fn)
    print(f'uploaded file: {blob.public_url}')
    return


def upload2bq(dataset=None,
              table=None,
              df_in=None):
    '''
    Upload model predictions to BigQuery table
    
    param dataset: bq dataset where table will be uploaded
    type dataset: string
    
    param table: bq table name where results will be uploaded
    type table: string
    
    param df_in: input data that will be uploaded
    type df_in: pandas dataframe
    
    RETURNS: None
    '''
    from datetime import datetime
    from google.cloud import bigquery
        
    client = bigquery.Client()
    table_id = '{}.{}'.format(dataset,table)

    upload_df = df_in.copy(deep=True)
    exec_time = str(datetime.utcnow())
    upload_df['updated_on'] = exec_time
    upload_df['updated_by'] = 'char8060'
        
    # Since string columns use the "object" dtype, pass in a (partial) schema
    # to ensure the correct BigQuery data type.
    schema_=[]
    for col_name,col_dtype in zip(upload_df.columns,upload_df.dtypes):
        if col_dtype == 'object':
            schema_.append(bigquery.SchemaField(col_name, "STRING"))
    job_config = bigquery.LoadJobConfig(shema=schema_)
        
    job = client.load_table_from_dataframe(upload_df, table_id, job_config=job_config)
    print(job.result())
    return