import pandas as pd
import numpy as np
from sodapy import Socrata
import os
import boto3
import configparser
from utils.s3_utils import pd2s3

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('aws.cfg')
    aws = config["AWS"]

    # Paths and Inputs
    filename = "covid-tests.csv"
    covid_tests_file_path = os.path.join(os.getcwd(), "data", filename)
    bucket = 'censo-covid'
    s3_resource = boto3.resource('s3',
                                 aws_access_key_id=aws['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=aws['AWS_SECRET_ACCESS_KEY'])
    s3_key_covid_tests = os.path.join("raw-data", filename)

    client = Socrata("www.datos.gov.co", None)
    results = client.get("8835-5baf", limit=100000)

    # Convert to pandas DataFrame
    df_muestras = pd.DataFrame.from_records(results)
    df_muestras = df_muestras[1:]

    for column in df_muestras.columns:
        if column=="fecha":
            df_muestras[column] = df_muestras[column].astype(np.datetime64)
        else:
            if column=="positividad_acumulada":
                df_muestras[column] = df_muestras[column].str.replace(",", ".")
            df_muestras[column] = df_muestras[column].astype(np.float64)
    df_muestras.to_csv(covid_tests_file_path, index=False)
    print("File saved as ", covid_tests_file_path)
    pd2s3(df_muestras, bucket, s3_key_covid_tests, s3_resource)
    