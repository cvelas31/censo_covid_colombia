def build_schema_census(source="vivienda"):
    """
    Build schema for different sources

    Parameters:
    -----------
    source : str
        Table source may be: "vivienda", "personas", "hogar", "fallecidos", "georeferenciacion"

    Return:
    -------
    schema : spark.schema
        Spark schema for loading source table
    """
    if source == "vivienda":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("UVA_ESTATER", IntegerType()),
                             StructField("UVA1_TIPOTER", LongType()),
                             StructField("UVA2_CODTER", LongType()),
                             StructField("UVA_ESTA_AREAPROT", IntegerType()),
                             StructField("UVA1_COD_AREAPROT", LongType()),
                             StructField("UVA_USO_UNIDAD", IntegerType()),
                             StructField("V_TIPO_VIV", LongType()),
                             StructField("V_CON_OCUP", LongType()),
                             StructField("V_TOT_HOG", LongType()),
                             StructField("V_MAT_PARED", LongType()),
                             StructField("V_MAT_PISO", LongType()),
                             StructField("VA_EE", LongType()),
                             StructField("VA1_ESTRATO", LongType()),
                             StructField("VB_ACU", LongType()),
                             StructField("VC_ALC", LongType()),
                             StructField("VD_GAS", LongType()),
                             StructField("VE_RECBAS", LongType()),
                             StructField("VE1_QSEM", LongType()),
                             StructField("VF_INTERNET", LongType()),
                             StructField("V_TIPO_SERSA", LongType()),
                             StructField("L_TIPO_INST", LongType()),
                             StructField("L_EXISTEHOG", LongType()),
                             StructField("L_TOT_PERL", LongType())
                             ])
    elif source == "hogar":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("H_NROHOG", LongType()),
                             StructField("H_NRO_CUARTOS", LongType()),
                             StructField("H_NRO_DORMIT", LongType()),
                             StructField("H_DONDE_PREPALIM", LongType()),
                             StructField("H_AGUA_COCIN", LongType()),
                             StructField("HA_NRO_FALL", LongType()),
                             StructField("HA_TOT_PER", LongType())
                             ])
    elif source == "personas":
        schema = StructType([StructField("TIPO_REG", IntegerType()),
                             StructField("U_DPTO", IntegerType()),
                             StructField("U_MPIO", IntegerType()),
                             StructField("UA_CLASE", IntegerType()),
                             StructField("U_EDIFICA", IntegerType()),
                             StructField("COD_ENCUESTAS", IntegerType()),
                             StructField("U_VIVIENDA", IntegerType()),
                             StructField("P_NROHOG", LongType()),
                             StructField("P_NRO_PER", IntegerType()),
                             StructField("P_SEXO", IntegerType()),
                             StructField("P_EDADR", IntegerType()),
                             StructField("P_PARENTESCOR", LongType()),
                             StructField("PA1_GRP_ETNIC", IntegerType()),
                             StructField("PA11_COD_ETNIA", LongType()),
                             StructField("PA12_CLAN", LongType()),
                             StructField("PA21_COD_VITSA", LongType()),
                             StructField("PA22_COD_KUMPA", LongType()),
                             StructField("PA_HABLA_LENG", LongType()),
                             StructField("PA1_ENTIENDE", LongType()),
                             StructField("PB_OTRAS_LENG", LongType()),
                             StructField("PB1_QOTRAS_LENG", LongType()),
                             StructField("PA_LUG_NAC", IntegerType()),
                             StructField("PA_VIVIA_5ANOS", LongType()),
                             StructField("PA_VIVIA_1ANO", LongType()),
                             StructField("P_ENFERMO", LongType()),
                             StructField("P_QUEHIZO_PPAL", LongType()),
                             StructField("PA_LO_ATENDIERON", LongType()),
                             StructField("PA1_CALIDAD_SERV", LongType()),
                             StructField("CONDICION_FISICA", LongType()),
                             StructField("P_ALFABETA", LongType()),
                             StructField("PA_ASISTENCIA", LongType()),
                             StructField("P_NIVEL_ANOSR", LongType()),
                             StructField("P_TRABAJO", LongType()),
                             StructField("P_EST_CIVIL", LongType()),
                             StructField("PA_HNV", LongType()),
                             StructField("PA1_THNV", LongType()),
                             StructField("PA2_HNVH", LongType()),
                             StructField("PA3_HNVM", LongType()),
                             StructField("PA_HNVS", LongType()),
                             StructField("PA1_THSV", LongType()),
                             StructField("PA2_HSVH", LongType()),
                             StructField("PA3_HSVM", LongType()),
                             StructField("PA_HFC", LongType()),
                             StructField("PA1_THFC", LongType()),
                             StructField("PA2_HFCH", LongType()),
                             StructField("PA3_HFCM", LongType()),
                             StructField("PA_UHNV", LongType()),
                             StructField("PA1_MES_UHNV", LongType()),
                             StructField("PA2_ANO_UHNV", LongType())
                             ])
    elif source == "fallecidos":
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
    elif source == "georeferenciacion":
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
        print("Source not valid. Enter one of the following sources: 'vivienda', 'personas', 'hogar', 'fallecidos', 'georeferenciacion')
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
                             StructField("divipola_mpio", IntegerType())
                             ])
    elif source == "tests":
        schema = StructType([StructField("fecha", DateType()),
                             StructField("acumuladas", LongType()),
                             StructField("amazonas", LongType()),
                             StructField("antioquia", LongType()),
                             StructField("arauca", LongType()),
                             StructField("atlantico", LongType()),
                             StructField("bogota", LongType()),
                             StructField("bolivar", LongType()),
                             StructField("boyaca", LongType()),
                             StructField("caldas", LongType()),
                             StructField("caqueta", LongType()),
                             StructField("casanare", LongType()),
                             StructField("cauca", LongType()),
                             StructField("cesar", LongType()),
                             StructField("choco", LongType()),
                             StructField("cordoba", LongType()),
                             StructField("cundinamarca", LongType()),
                             StructField("guainia", LongType()),
                             StructField("guajira", LongType()),
                             StructField("guaviare", LongType()),
                             StructField("huila", LongType()),
                             StructField("magdalena", LongType()),
                             StructField("meta", LongType()),
                             StructField("narino", LongType()),
                             StructField("norte_de_santander", LongType()),
                             StructField("putumayo", LongType()),
                             StructField("quindio", LongType()),
                             StructField("risaralda", LongType()),
                             StructField("san_andres", LongType()),
                             StructField("santander", LongType()),
                             StructField("sucre", LongType()),
                             StructField("tolima", LongType()),
                             StructField("valle_del_cauca", LongType()),
                             StructField("vaupes", LongType()),
                             StructField("vichada", LongType()),
                             StructField("procedencia_desconocida", LongType()),
                             StructField("positivas_acumuladas", LongType()),
                             StructField("negativas_acumuladas", LongType()),
                             StructField("positividad_acumulada", LongType()),
                             StructField("indeterminadas", LongType()),
                             StructField("barranquilla", LongType()),
                             StructField("cartagena", LongType()),
                             StructField("santa_marta", LongType())
                             ])
    else:
        print("Source not valid. Enter one of the following sources: 'covid', 'tests'")
    return schema
