import pandas as pd

def fetch_data(source_type='table', #or 'csv', or 'query'
               source_name='ospc_churn_dev.features'
               ):
    '''
    Run query on BQ, or load table name or csv file
    INPUTS:
       -- source_type
          type: str
          desc: ['table','query','csv'] Tells the source type
       -- source_name
          type: str
          desc: ['bq_dataset.bq_table' if source_type = 'table',
                  BQ query if source_type = query,
                  'filename.csv' if source_type = 'csv ]
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

    elif source_type == 'csv':
        dataframe = pd.read_csv(source_name)

        
    #logger.debug('Done fetching data')
    print('done fetching data')
    return dataframe


def upload2gcs(pd_data,
               bucket_name='rax-datascience-dev',
               path='cmueller/test',
               filename='myfile'
           ):
    '''
    Uploads data to GCS
    param pd_data: - the data to be uploaded
    type pd_data: pandas dataframe
    param bucket_name: the GCS bucket where the file will be uploaded
    type bucket_name: str
    param path: the path to the file (DO NOT add leading or trailing '/')
    type path: str
    param filename: the file name (without extension)
    type filename: str
    '''
    from datetime import datetime
    from google.cloud import storage
    from google.cloud.storage.blob import Blob
    
    persist_time = str(datetime.utcnow())
    fn = f'{filename}_{persist_time}.csv.gz'.replace(' ','_')
    
    print('saving file locally...')
    pd_data.to_csv(fn,compression='gzip')

    blb = f'{path}/{fn}'
    gcs = storage.Client()
    
    print(f'getting bucket: {bucket_name}')
    bucket = gcs.get_bucket(bucket_name)
    blob = bucket.blob(blb)
    print('uploading blob....')
    blob.upload_from_filename(fn)
    print(f'uploaded file: {blob.public_url}')
    return