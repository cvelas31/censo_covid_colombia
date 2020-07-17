from io import StringIO
import pandas as pd
import os
from .general_utils import create_dir


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
                dict_paths_departments[department].update(
                    {"MGN": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "FALL" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update(
                    {"FALL": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "HOG" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update(
                    {"HOG": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "VIV" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update(
                    {"VIV": os.path.join(f"s3a://{bucket_s3.name}", name)})
            elif "PER" in list_paths[-1]:
                if not(department in dict_paths_departments):
                    dict_paths_departments[department] = {}
                dict_paths_departments[department].update(
                    {"PER": os.path.join(f"s3a://{bucket_s3.name}", name)})
    return dict_paths_departments


def download_files_from_s3(curr_dir, s3Bucket, prefix):
    """
    Download files insiide s3 'directory', keeping the same folder structure.

    Parameters:
    -----------
    curr_dir : os.path
        Root directory
    s3Bucket : boto3.Bucket
        Bucket where the prefix is located
    prefix : os.path
        s3 directory of s3Bucket to download 
    
    Return:
    -------
    None
    """
    if os.path.isfile(curr_dir):
        print("Is a file")
    elif len(os.listdir(curr_dir)) > 1:
        print(f"Already downloaded {prefix}") 
    else:
        for s3_object in s3Bucket.objects.filter(Prefix=prefix).all():
            path, filename = os.path.split(s3_object.key)
            partial_dir = mk_partitioned_dir(curr_dir, path)
            if filename:
                print("Downloading...", filename)
                s3Bucket.download_file(s3_object.key, os.path.join(partial_dir, filename))
            print("Downloaded: ",os.path.join(partial_dir, filename))


def mk_partitioned_dir(curr_dir, path):
    """
    Recursive function to create same folder structure of partitioned dataset in s3

    Parameters:
    -----------
    curr_dir : os.path
        Current directory
    path : str
        Path of stored partitioned data. Ex: final-data/aggregates_personas/dpto=META

    Return:
    -------
    curr_dir : os.path
        Current local directory to store the file 
    """
    root = path.split("/")[0]
    if len(root) == 0:
        return curr_dir
    elif "=" in root:
        curr_dir = os.path.join(curr_dir, root)
        create_dir(curr_dir)
        return mk_partitioned_dir(curr_dir, path[len(root)+1:])
    else:
        return mk_partitioned_dir(curr_dir, path[len(root)+1:])


def identified_partitioned_dir(path, key_val=None):
    """
    Recursive identifier of a partitioned directory extracting the key values in folder names

    Parameters:
    -----------
    path : os.path
        Path with metadata in folder names. Ex: final-data/aggregates_personas/dpto=META
    key_val : None or dict
        Dictionary where the the key values are stored
    
    Return:
    key_val : dict
        Key value dict. Ex: {dpto: META}
    """
    root = path.split("/")[0]
    if len(root) == 0 and len(path.split("/"))<=1:
        return key_val
    elif "=" in root:
        key = root.split("=")[0]
        val = root.split("=")[1]
        if key_val is None:
            key_val = {key: val}
        else:
            key_val.update({key: val})
        return identified_partitioned_dir(path[len(root)+1:], key_val=key_val)
    else:
        return identified_partitioned_dir(path[len(root)+1:], key_val=key_val)


def read_multiple_csv(selected_dir, to_concat=[], list_of_files=[], header=0, n_files=None):
    """
    Recursive read partitioned csv dataset, adding follder name columns

    Parameters:
    -----------
    selected_dir : os.path
        Directory to search on for csvs
    to_concat : list
        List to store the pd.DataFrames read
    header : int
        Same as pd header on read_csv
    n_files : int or None
        Maximum number of files to read

    Return:
    -------
    to_concat : list
        List with all pd.DataFrames read from csvs
    """
    if n_files is not None:
        if len(to_concat) == n_files:
            return to_concat
    for path in os.listdir(selected_dir):
        if path.endswith(".csv"):
            aux = pd.read_csv(os.path.join(selected_dir, path), header=header)
            dict_key_val = identified_partitioned_dir(selected_dir)
            if dict_key_val is not None:
                for key, val in dict_key_val.items():
                    aux[key] = val
            to_concat.append(aux)
            continue
        elif os.path.isdir(os.path.join(selected_dir, path)):
            list_of_files.append(os.path.join(selected_dir, path))
            to_concat, list_of_files = read_multiple_csv(os.path.join(selected_dir, path), to_concat=to_concat,
                                          header=header, n_files=n_files)
    return to_concat, list_of_files
