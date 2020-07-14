import os

metadata = {"CENSO": {"VIVIENDA": {"useful_columns": ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'U_EDIFICA',
                                                      'COD_ENCUESTAS', 'U_VIVIENDA', 'UVA_USO_UNIDAD',
                                                      'V_TIPO_VIV', 'V_CON_OCUP', 'V_TOT_HOG',
                                                      'V_MAT_PARED', 'V_MAT_PISO', 'VA_EE', 'VA1_ESTRATO', 'VB_ACU', 'VC_ALC',
                                                      'VD_GAS', 'VE_RECBAS', 'VE1_QSEM', 'VF_INTERNET', 'V_TIPO_SERSA',
                                                      'L_TIPO_INST', 'L_EXISTEHOG', 'L_TOT_PERL']
                                   },
                      "HOGAR": {"useful_columns": ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'COD_ENCUESTAS',
                                                   'U_VIVIENDA', 'H_NROHOG', 'H_NRO_CUARTOS', 'H_NRO_DORMIT',
                                                   'H_DONDE_PREPALIM', 'H_AGUA_COCIN', 'HA_NRO_FALL', 'HA_TOT_PER']},
                      "PERSONAS": {"useful_columns": ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'U_EDIFICA',
                                                      'COD_ENCUESTAS', 'U_VIVIENDA', 'P_NROHOG', 'P_NRO_PER', 'P_SEXO',
                                                      'P_EDADR', 'P_PARENTESCOR', 'PA_LUG_NAC',
                                                      'PA_VIVIA_5ANOS', 'PA_VIVIA_1ANO', 'P_ENFERMO', 'P_QUEHIZO_PPAL',
                                                      'PA_LO_ATENDIERON', 'PA1_CALIDAD_SERV', 'CONDICION_FISICA',
                                                      'P_ALFABETA', 'PA_ASISTENCIA', 'P_NIVEL_ANOSR', 'P_TRABAJO',
                                                      'P_EST_CIVIL', 'PA_HNV', 'PA1_THNV', 'PA2_HNVH', 'PA3_HNVM', 'PA_HNVS',
                                                      'PA1_THSV', 'PA2_HSVH', 'PA3_HSVM', 'PA_HFC', 'PA1_THFC', 'PA2_HFCH',
                                                      'PA3_HFCM']},
                      "FALLECIDOS": {"useful_columns": ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'COD_ENCUESTAS',
                                                        'U_VIVIENDA', 'F_NROHOG', 'FA1_NRO_FALL', 'FA2_SEXO_FALL',
                                                        'FA3_EDAD_FALL', 'FA4_CERT_DEFUN']},
                      "GEOREFERENCIACION": {"useful_columns": ['U_DPTO', 'U_MPIO', 'UA_CLASE', 'UA1_LOCALIDAD', 'U_SECT_RUR',
                                                               'U_SECC_RUR', 'UA2_CPOB', 'U_SECT_URB', 'U_SECC_URB', 'U_MZA',
                                                               'U_EDIFICA', 'COD_ENCUESTAS', 'U_VIVIENDA']},
                      "DIVIPOLA": {"useful_columns": ['cod_depto', 'cod_mpio', 'dpto', 'nom_mpio', 'tipo_municipio']}
                      },
            }