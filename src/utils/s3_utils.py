from io import StringIO
import pandas as pd
import os


def pd2s3(df, bucket, key, s3_resource):
    """Save pd.DataFrame in S3 as csv"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer)
    s3_resource.Object(bucket, key).put(Body=csv_buffer.getvalue())
    print(f"File saved in S3 {bucket}/{key}")


def upload_files_to_s3(s3_client, dictionary_paths, bucket, prefix_censo="raw-data"):
    """
    Upload the files in a dictionary to s3

    Parameters:
    -----------
    s3_client : botocore.client.S3
        Boto3 s3 client
    dictionary_paths : dict
        Dictionary of paths for each department to use
    bucket : str
        Bucket name
    prefix_censo : str
        Prefix to add in s3 after the bucket name
    """
    for key, val in dictionary_paths.items():
        for table, filepath in val.items():
            paths = filepath.split("/")
            file_name_s3 = os.path.join(prefix_censo, *paths[-4:])
            print(file_name_s3)
            try:
                response = s3_client.head_object(Bucket=bucket,
                                    Key=file_name_s3)
            except Exception as e:
                if e.response["Error"]["Message"] == "Not Found":
                    response = s3_client.upload_file(Bucket=bucket,
                                                    Key=file_name_s3,
                                                    Filename=filepath)
    print("Files uploaded to S3")
