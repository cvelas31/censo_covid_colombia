import pandas as pd
from sodapy import Socrata
import os
import boto3
from utils.s3_utils import pd2s3


if __name__ == "__main__":
    # Paths and Inputs
    filename = "divipola.csv"
    divipola_file_path = os.path.join(os.getcwd(), "..", "data", filename)
    bucket = 'censo-covid'
    s3_resource = boto3.resource('s3')
    s3_key_divipola = os.path.join("raw-data", filename)

    client = Socrata("www.datos.gov.co", None)
    results = client.get("gdxc-w37w", limit=100000)

    # Convert to pandas DataFrame
    df_divipola = pd.DataFrame.from_records(results)
    df_divipola = df_divipola[:-5]  # Clean last 5 unuseful rows
    df_divipola["cod_mpio"] = df_divipola["cod_mpio"].str[2:]  # Split code
    df_divipola = df_divipola.astype({'cod_depto': np.int64,
                                      'cod_mpio': np.int64,
                                      'dpto': np.dtype('O'),
                                      'nom_mpio': np.dtype('O'),
                                      'tipo_municipio': np.dtype('O')})
    df_divipola.to_csv(divipola_file_path)
    print("File saved as ", divipola_file_path)
    pd2s3(df_divipola, bucket, s3_key_divipola, s3_resource)
