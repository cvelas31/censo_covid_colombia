from io import StringIO
import pandas as pd
import os


def pd2s3(df, bucket, key, s3_resource):
    """Save pd.DataFrame in S3 as csv"""
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
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

def get_censo_paths(bucket_s3, directory_key):
    """
    Get dictionary of census data for each department
    
    Parameters:
    -----------
    bucket_s3 : s3.Bucket
        Boto3 Bucket object
    directory_key : path
        Directory key in S3
    
    Return:
    -------
    dict_paths_departments : dict
        Dictionary with the data path for each departtment
    """
    dict_paths_departments = {}
    for object_summary in bucket_s3.objects.filter(Prefix=directory_key):
        name = object_summary.key
        if name.endswith(".CSV"):
            list_paths = name.split("/")
            department = list_paths[2].split("_")[1]
            if "MGN" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update({"MGN": os.path.join(f"s3a://{bucket_s3.name}", name)})                
            elif "FALL" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update({"FALL": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "HOG" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update({"HOG": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "VIV" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update({"VIV": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "PER" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update({"PER": os.path.join(f"s3a://{bucket_s3.name}", name)})
    return dict_paths_departments