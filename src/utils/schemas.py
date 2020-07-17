def build_schema_census(source="vivienda"):
    """
    Build schema for different sources

    Parameters:
    -----------
    source : str
        Table source may be: "VIV", "PER", "HOG", "FALL", "MGN"

    Return:
    -------
    schema : spark.schema
        Spark schema for loading source table
    """
    if source == "VIV":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("UVA_ESTATER", IntegerType()),
                             StructField("UVA1_TIPOTER", DoubleType()),
                             StructField("UVA2_CODTER", DoubleType()),
                             StructField("UVA_ESTA_AREAPROT", IntegerType()),
                             StructField("UVA1_COD_AREAPROT", DoubleType()),
                             StructField("UVA_USO_UNIDAD", IntegerType()),
                             StructField("V_TIPO_VIV", DoubleType()),
                             StructField("V_CON_OCUP", DoubleType()),
                             StructField("V_TOT_HOG", DoubleType()),
                             StructField("V_MAT_PARED", DoubleType()),
                             StructField("V_MAT_PISO", DoubleType()),
                             StructField("VA_EE", DoubleType()),
                             StructField("VA1_ESTRATO", DoubleType()),
                             StructField("VB_ACU", DoubleType()),
                             StructField("VC_ALC", DoubleType()),
                             StructField("VD_GAS", DoubleType()),
                             StructField("VE_RECBAS", DoubleType()),
                             StructField("VE1_QSEM", DoubleType()),
                             StructField("VF_INTERNET", DoubleType()),
                             StructField("V_TIPO_SERSA", DoubleType()),
                             StructField("L_TIPO_INST", DoubleType()),
                             StructField("L_EXISTEHOG", DoubleType()),
                             StructField("L_TOT_PERL", DoubleType())
                             ])
    elif source == "HOG":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("H_NROHOG", DoubleType()),
                             StructField("H_NRO_CUARTOS", DoubleType()),
                             StructField("H_NRO_DORMIT", DoubleType()),
                             StructField("H_DONDE_PREPALIM", DoubleType()),
                             StructField("H_AGUA_COCIN", DoubleType()),
                             StructField("HA_NRO_FALL", DoubleType()),
                             StructField("HA_TOT_PER", DoubleType())
                             ])
    elif source == "PER":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("P_NROHOG", DoubleType()),
                             StructField("P_NRO_PER", IntegerType()),
                             StructField("P_SEXO", IntegerType()),
                             StructField("P_EDADR", IntegerType()),
                             StructField("P_PARENTESCOR", DoubleType()),
                             StructField("PA1_GRP_ETNIC", IntegerType()),
                             StructField("PA11_COD_ETNIA", DoubleType()),
                             StructField("PA12_CLAN", DoubleType()),
                             StructField("PA21_COD_VITSA", DoubleType()),
                             StructField("PA22_COD_KUMPA", DoubleType()),
                             StructField("PA_HABLA_LENG", DoubleType()),
                             StructField("PA1_ENTIENDE", DoubleType()),
                             StructField("PB_OTRAS_LENG", DoubleType()),
                             StructField("PB1_QOTRAS_LENG", DoubleType()),
                             StructField("PA_LUG_NAC", IntegerType()),
                             StructField("PA_VIVIA_5ANOS", DoubleType()),
                             StructField("PA_VIVIA_1ANO", DoubleType()),
                             StructField("P_ENFERMO", DoubleType()),
                             StructField("P_QUEHIZO_PPAL", DoubleType()),
                             StructField("PA_LO_ATENDIERON", DoubleType()),
                             StructField("PA1_CALIDAD_SERV", DoubleType()),
                             StructField("CONDICION_FISICA", DoubleType()),
                             StructField("P_ALFABETA", DoubleType()),
                             StructField("PA_ASISTENCIA", DoubleType()),
                             StructField("P_NIVEL_ANOSR", DoubleType()),
                             StructField("P_TRABAJO", DoubleType()),
                             StructField("P_EST_CIVIL", DoubleType()),
                             StructField("PA_HNV", DoubleType()),
                             StructField("PA1_THNV", DoubleType()),
                             StructField("PA2_HNVH", DoubleType()),
                             StructField("PA3_HNVM", DoubleType()),
                             StructField("PA_HNVS", DoubleType()),
                             StructField("PA1_THSV", DoubleType()),
                             StructField("PA2_HSVH", DoubleType()),
                             StructField("PA3_HSVM", DoubleType()),
                             StructField("PA_HFC", DoubleType()),
                             StructField("PA1_THFC", DoubleType()),
                             StructField("PA2_HFCH", DoubleType()),
                             StructField("PA3_HFCM", DoubleType()),
                             StructField("PA_UHNV", DoubleType()),
                             StructField("PA1_MES_UHNV", DoubleType()),
                             StructField("PA2_ANO_UHNV", DoubleType())
                             ])
    elif source == "FALL":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("F_NROHOG", IntegerType()),
                             StructField("FA1_NRO_FALL", IntegerType()),
                             StructField("FA2_SEXO_FALL", IntegerType()),
                             StructField("FA3_EDAD_FALL", IntegerType()),
                             StructField("FA4_CERT_DEFUN", IntegerType())
                             ])
    elif source == "MGN":
        schema = StructType([StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("UA1_LOCALIDAD", IntegerType()),
                             StructField("U_SECT_RUR", IntegerType()),
                             StructField("U_SECC_RUR", IntegerType()),
                             StructField("UA2_CPOB", IntegerType()),
                             StructField("U_SECT_URB", IntegerType()),
                             StructField("U_SECC_URB", IntegerType()),
                             StructField("U_MZA", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType())
                             ])
    else:
        print("Source not valid. Enter one of the following sources: VIV, PER, HOG, FALL, MGN")
    return schema


def build_schema_covid(source="covid"):
    """
    Build schema for different covid sources

    Parameters:
    -----------
    source : str
        Table source may be: "covid", "tests"

    Return:
    -------
    schema : spark.schema
        Spark schema for loading source table
    """
    if source == "covid":
        schema = StructType([StructField("fecha_de_notificaci_n", DateType()),
                             StructField("c_digo_divipola", StringType()),
                             StructField("ciudad_de_ubicaci_n", StringType()),
                             StructField("departamento", StringType()),
                             StructField("atenci_n", StringType()),
                             StructField("edad", IntegerType()),
                             StructField("sexo", StringType()),
                             StructField("tipo", StringType()),
                             StructField("estado", StringType()),
                             StructField("pa_s_de_procedencia", StringType()),
                             StructField("fis", DateType()),
                             StructField("fecha_diagnostico", DateType()),
                             StructField("fecha_recuperado", DateType()),
                             StructField("fecha_reporte_web", DateType()),
                             StructField("tipo_recuperaci_n", StringType()),
                             StructField("codigo_departamento", StringType()),
                             StructField("codigo_pais", StringType()),
                             StructField("pertenencia_etnica", StringType()),
                             StructField("nombre_grupo_etnico", StringType()),
                             StructField("fecha_de_muerte", DateType()),
                             StructField("Asintomatico", IntegerType()),
                             StructField("divipola_dpto", IntegerType()),
                             StructField("divipola_mpio", IntegerType()),
                             StructField("edad_q", IntegerType()),
                             StructField("muerto", BooleanType()),
                             StructField("edad_muerto", IntegerType()),
                             ])
    elif source == "tests":
        schema = StructType([StructField("fecha", DateType()),
                             StructField("acumuladas", DoubleType()),
                             StructField("amazonas", DoubleType()),
                             StructField("antioquia", DoubleType()),
                             StructField("arauca", DoubleType()),
                             StructField("atlantico", DoubleType()),
                             StructField("bogota", DoubleType()),
                             StructField("bolivar", DoubleType()),
                             StructField("boyaca", DoubleType()),
                             StructField("caldas", DoubleType()),
                             StructField("caqueta", DoubleType()),
                             StructField("casanare", DoubleType()),
                             StructField("cauca", DoubleType()),
                             StructField("cesar", DoubleType()),
                             StructField("choco", DoubleType()),
                             StructField("cordoba", DoubleType()),
                             StructField("cundinamarca", DoubleType()),
                             StructField("guainia", DoubleType()),
                             StructField("guajira", DoubleType()),
                             StructField("guaviare", DoubleType()),
                             StructField("huila", DoubleType()),
                             StructField("magdalena", DoubleType()),
                             StructField("meta", DoubleType()),
                             StructField("narino", DoubleType()),
                             StructField("norte_de_santander", DoubleType()),
                             StructField("putumayo", DoubleType()),
                             StructField("quindio", DoubleType()),
                             StructField("risaralda", DoubleType()),
                             StructField("san_andres", DoubleType()),
                             StructField("santander", DoubleType()),
                             StructField("sucre", DoubleType()),
                             StructField("tolima", DoubleType()),
                             StructField("valle_del_cauca", DoubleType()),
                             StructField("vaupes", DoubleType()),
                             StructField("vichada", DoubleType()),
                             StructField("procedencia_desconocida", DoubleType()),
                             StructField("positivas_acumuladas", DoubleType()),
                             StructField("negativas_acumuladas", DoubleType()),
                             StructField("positividad_acumulada", DoubleType()),
                             StructField("indeterminadas", DoubleType()),
                             StructField("barranquilla", DoubleType()),
                             StructField("cartagena", DoubleType()),
                             StructField("santa_marta", DoubleType())
                             ])
    else:
        print("Source not valid. Enter one of the following sources: 'covid', 'tests'")
    return schema
              
def build_schema_divipola(source="divipola"):
    """
    Build schema for different covid sources

    Parameters:
    -----------
    source : str
        Table source may be: "covid", "tests"

    Return:
    -------
    schema : spark.schema
        Spark schema for loading source table
    """
    if source == "divipola":
        schema = StructType([StructField("cod_depto", IntegerType()),
                             StructField("cod_mpio", IntegerType()),
                             StructField("dpto", StringType()),
                             StructField("nom_mpio", StringType()),
                             StructField("tipo_municipio", StringType())
                             ])
    else:
        print("Source not valid. Enter one of the following sources: 'covid', 'tests'")
    return schema

def build_schema_complete(source="vivienda"):
    """
    Build schema for different sources

    Parameters:
    -----------
    source : str
        Table source may be: "VIV", "PER", "HOG", "FALL", "MGN"

    Return:
    -------
    schema : spark.schema
        Spark schema for loading source table
    """
    if source == "fallecidos":
        schema = StructType([StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("F_NROHOG", IntegerType()),
                             StructField("FA1_NRO_FALL", IntegerType()),
                             StructField("FA2_SEXO_FALL", IntegerType()),
                             StructField("FA3_EDAD_FALL", IntegerType()),
                             StructField("FA4_CERT_DEFUN", IntegerType()),
                             StructField("UVA_USO_UNIDAD", IntegerType()),
                             StructField("V_TIPO_VIV", DoubleType()),
                             StructField("V_CON_OCUP", DoubleType()),
                             StructField("V_TOT_HOG", DoubleType()),
                             StructField("V_MAT_PARED", DoubleType()),
                             StructField("V_MAT_PISO", DoubleType()),
                             StructField("VA_EE", DoubleType()),
                             StructField("VA1_ESTRATO", DoubleType()),
                             StructField("VB_ACU", DoubleType()),
                             StructField("VC_ALC", DoubleType()),
                             StructField("VD_GAS", DoubleType()),
                             StructField("VE_RECBAS", DoubleType()),
                             StructField("VE1_QSEM", DoubleType()),
                             StructField("VF_INTERNET", DoubleType()),
                             StructField("V_TIPO_SERSA", DoubleType()),
                             StructField("L_TIPO_INST", DoubleType()),
                             StructField("L_EXISTEHOG", DoubleType()),
                             StructField("L_TOT_PERL", DoubleType()),
                             StructField("H_NRO_CUARTOS_H", DoubleType()),
                             StructField("H_NRO_DORMIT_H", DoubleType()),
                             StructField("H_DONDE_PREPALIM_H", DoubleType()),
                             StructField("H_AGUA_COCIN_H", DoubleType()),
                             StructField("HA_NRO_FALL_H", DoubleType()),
                             StructField("HA_TOT_PER_H", DoubleType()),
                             StructField("UA1_LOCALIDAD", IntegerType()),
                             StructField("U_SECT_RUR", IntegerType()),
                             StructField("U_SECC_RUR", IntegerType()),
                             StructField("UA2_CPOB", IntegerType()),
                             StructField("U_SECT_URB", IntegerType()),
                             StructField("U_SECC_URB", IntegerType()),
                             StructField("U_MZA", IntegerType()),
                             StructField("dpto", StringType()),
                             StructField("nom_mpio", StringType()),
                             StructField("tipo_municipio", StringType())])
    elif source == "personas":
        schema = StructType([StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("P_NROHOG", IntegerType()),
                             StructField("P_NRO_PER", IntegerType()),
                             StructField("P_SEXO", IntegerType()),
                             StructField("P_EDADR", IntegerType()),
                             StructField("P_PARENTESCOR", DoubleType()),
                             StructField("PA_LUG_NAC", IntegerType()),
                             StructField("PA_VIVIA_5ANOS", DoubleType()),
                             StructField("PA_VIVIA_1ANO", DoubleType()),
                             StructField("P_ENFERMO", DoubleType()),
                             StructField("P_QUEHIZO_PPAL", DoubleType()),
                             StructField("PA_LO_ATENDIERON", DoubleType()),
                             StructField("PA1_CALIDAD_SERV", DoubleType()),
                             StructField("CONDICION_FISICA", DoubleType()),
                             StructField("P_ALFABETA", DoubleType()),
                             StructField("PA_ASISTENCIA", DoubleType()),
                             StructField("P_NIVEL_ANOSR", DoubleType()),
                             StructField("P_TRABAJO", DoubleType()),
                             StructField("P_EST_CIVIL", DoubleType()),
                             StructField("PA_HNV", DoubleType()),
                             StructField("PA1_THNV", DoubleType()),
                             StructField("PA2_HNVH", DoubleType()),
                             StructField("PA3_HNVM", DoubleType()),
                             StructField("PA_HNVS", DoubleType()),
                             StructField("PA1_THSV", DoubleType()),
                             StructField("PA2_HSVH", DoubleType()),
                             StructField("PA3_HSVM", DoubleType()),
                             StructField("PA_HFC", DoubleType()),
                             StructField("PA1_THFC", DoubleType()),
                             StructField("PA2_HFCH", DoubleType()),
                             StructField("PA3_HFCM", DoubleType()),
                             StructField("UVA_USO_UNIDAD", IntegerType()),
                             StructField("V_TIPO_VIV", DoubleType()),
                             StructField("V_CON_OCUP", DoubleType()),
                             StructField("V_TOT_HOG", DoubleType()),
                             StructField("V_MAT_PARED", DoubleType()),
                             StructField("V_MAT_PISO", DoubleType()),
                             StructField("VA_EE", DoubleType()),
                             StructField("VA1_ESTRATO", DoubleType()),
                             StructField("VB_ACU", DoubleType()),
                             StructField("VC_ALC", DoubleType()),
                             StructField("VD_GAS", DoubleType()),
                             StructField("VE_RECBAS", DoubleType()),
                             StructField("VE1_QSEM", DoubleType()),
                             StructField("VF_INTERNET", DoubleType()),
                             StructField("V_TIPO_SERSA", DoubleType()),
                             StructField("L_TIPO_INST", DoubleType()),
                             StructField("L_EXISTEHOG", DoubleType()),
                             StructField("L_TOT_PERL", DoubleType()),
                             StructField("H_NRO_CUARTOS_H", DoubleType()),
                             StructField("H_NRO_DORMIT_H", DoubleType()),
                             StructField("H_DONDE_PREPALIM_H", DoubleType()),
                             StructField("H_AGUA_COCIN_H", DoubleType()),
                             StructField("HA_NRO_FALL_H", DoubleType()),
                             StructField("HA_TOT_PER_H", DoubleType()),
                             StructField("UA1_LOCALIDAD", IntegerType()),
                             StructField("U_SECT_RUR", IntegerType()),
                             StructField("U_SECC_RUR", IntegerType()),
                             StructField("UA2_CPOB", IntegerType()),
                             StructField("U_SECT_URB", IntegerType()),
                             StructField("U_SECC_URB", IntegerType()),
                             StructField("U_MZA", IntegerType()),
                             StructField("dpto", StringType()),
                             StructField("nom_mpio", StringType()),
                             StructField("tipo_municipio", StringType())])
    else:
        print("Source not valid. Enter one of the following sources: fallecidos, personas")
    return schema
              
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