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
    filename = "covid.csv"
    covid_file_path = os.path.join(os.getcwd(), "data", filename)
    bucket = 'censo-covid'
    s3_resource = boto3.resource('s3', 
                                 aws_access_key_id=aws['AWS_ACCESS_KEY_ID'],
                                 aws_secret_access_key=aws['AWS_SECRET_ACCESS_KEY'])
    s3_key_covid = os.path.join("raw-data", filename)

    client = Socrata("www.datos.gov.co", None)
    results = client.get("gt2j-8ykr", limit=10000000)
    df = pd.DataFrame.from_records(results)

    df = df.set_index("id_de_caso", drop=True)
    df["Asintomatico"] = (df["fis"] == "Asintomático").astype(int)
    df.loc[df["fis"] == "Asintomático", "fis"] = np.nan
    df = df.astype({'fecha_de_notificaci_n': np.datetime64,
                    'c_digo_divipola': str,
                    'ciudad_de_ubicaci_n': str,
                    'departamento': str,
                    'atenci_n': str,
                    'edad': np.int64,
                    'sexo': str,
                    'tipo': str,
                    'estado': str,
                    'pa_s_de_procedencia': str,
                    'fis': np.datetime64,
                    'fecha_diagnostico': np.datetime64,
                    'fecha_recuperado': np.datetime64,
                    'fecha_reporte_web': np.datetime64,
                    'tipo_recuperaci_n': str,
                    "codigo_departamento": str,
                    "codigo_pais": str,
                    "pertenencia_etnica": str,
                    'fecha_de_muerte': np.datetime64})
    df["divipola_dpto"] = df["c_digo_divipola"].str[:2].astype(int)
    df["divipola_mpio"] = df["c_digo_divipola"].str[2:].astype(int)
    df["sexo"] = df["sexo"].map({"F": "F", "f": "F", "m": "M", "M": "M"})
    df["edad_q"] = pd.cut(df["edad"], np.append(np.arange(-1, 100, 5),df["edad"].max()))
    map_edad_q = {}
    for idx, val in enumerate(df["edad_q"].unique()):
        map_edad_q[val] = idx+1
    df["edad_q"] = df["edad_q"].map(map_edad_q).astype(int)
    df["muerto"] = ~df["fecha_de_muerte"].isnull()
    df["edad_muerto"] = df["muerto"]*df["edad"]
    df.to_csv(covid_file_path, index=False)
    print("File saved as ", covid_file_path)
    pd2s3(df, bucket, s3_key_covid, s3_resource)