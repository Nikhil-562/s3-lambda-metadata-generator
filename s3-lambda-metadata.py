import json
import boto3
import os
from datetime import datetime
from botocore.exceptions import ClientError


s3 = boto3.client('s3')

def lambda_handler(event, context):

    record = event['Records'][0]
    bucket = record['s3']['bucket']['name']
    key =    record['s3']['object']['key']

    metadata_file =  "Metadata.json"


    if key.endswith('Metadata.json'):
        print("Metadat file detected! skipping.....")
        return 

    response = s3.head_object(Bucket=bucket,Key=key)

    file_name =  os.path.basename(key)
    file_extension = os.path.splitext(file_name)[1]
    file_size = response['ContentLength']
    upload_time = response['LastModified'].isoformat()
    folder_path = os.path.dirname(key)

    new_entry = {
        'file_name': file_name,
        'file_extension': file_extension,
        'file_size': file_size,
        'upload_time': upload_time,
        'folder_path': folder_path,
        "bucket_name": bucket,
        "s3_key": key,
        "metadata_created" : datetime.utcnow().isoformat()  
    }

    metadata_key = f"{folder_path}/{metadata_file}"

    try:
        response = s3.get_object(Bucket=bucket,Key=metadata_key)
        content = json.load(response['Body'].read())
        files = content.get('files',[])
        print("Metadata file found! appending.....")

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            print('No file exist. Creating New')
            files = []
        else:
            raise e

    files.append(new_entry)

    updated_metadata = {
        "files": files}

    s3.put_object(
        Bucket=bucket,
        Key=metadata_key,
        Body=json.dumps(updated_metadata,indent = 4),
        ContentType='application/json'
    )

    print("Metadata file updated successfully:", metadata_key)