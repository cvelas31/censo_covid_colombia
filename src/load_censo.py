import os
import boto3
import configparser
from utils.s3_utils import upload_files_to_s3
from utils.general_utils import create_dict_files


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('aws.cfg')
    aws = config["AWS"]
    bucket = "censo-covid"
    assert aws['AWS_ACCESS_KEY_ID'] is not None, "AWS ACCESS NOT FOUND"

    s3_client = boto3.client('s3',
                             aws_access_key_id=aws['AWS_ACCESS_KEY_ID'],
                             aws_secret_access_key=aws['AWS_SECRET_ACCESS_KEY']
                             )
    data_path = os.path.join(os.getcwd(), "data")
    censo_data_path = os.path.join(data_path, "censo")
    dict_paths_departments = create_dict_files(censo_data_path)
    upload_files_to_s3(s3_client, dict_paths_departments, bucket, prefix_censo="raw-data")
