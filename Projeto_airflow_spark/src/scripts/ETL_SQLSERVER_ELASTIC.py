import pandas as pd
from elasticsearch import Elasticsearch, helpers
import pandas as pd
import sqlalchemy as sa
from dotenv import load_dotenv
import os
from datetime import datetime
import pyodbc
import json
import logging

# Configurar o log
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')

#load_dotenv('/u01/etl/mdfe/.env')
load_dotenv('/u01/gepro/imagens/airflow/scripts/cargas/carga-efd/.env')

server = os.environ.get("ETL_SQL_SERVER_COPAF_URL")
database = os.environ.get("ETL_SQL_SERVER_COPAF_DATABASE")
username = os.environ.get("ETL_SQL_SERVER_COPAF_USERNAME")
password = os.environ.get("ETL_SQL_SERVER_COPAF_PASSWORD") 

# SQLAlchemy Connection String
connection_string = 'DRIVER={ODBC Driver 18 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password+';TrustServerCertificate=yes'
engine_sql = sa.create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")

def get_es_client(producao = False):
    if producao:
        es_client = Elasticsearch(
        hosts=["https://10.2.4.33:9200"],  
        ssl_assert_fingerprint="c443610c762e3b26117f0479966f185ca4bae10a581d7363306aa691f269c090",  
        api_key="R2ZoOXpvOEI3MmszMW8zMVpYMTA6RXlHUW8yN3NUZ1dGakM0YUd1cUZoUQ=="  
    )
    else:        
        es_client = Elasticsearch(
             hosts=["https://10.2.4.41:9200", "https://10.2.4.42:9200", "https://10.2.4.43:9200"],
            ssl_assert_fingerprint="68521ce70c87834a0ef73ac384d3e44520026f37d2ac45535310ee5d37fa6863",
             api_key="VkpQNVBZOEJXUFk4ZDMzakhJOWM6Y25JVXB0YmxUVHVONFZJRDBwSlFSUQ=="
        )
    return es_client
def main():
    es = get_es_client(producao=True)

    # Nome do Indice
    INDEX_NAME = "efd_ajuste_2"

    # Apagar index porque a carga vai ser full
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        logging.info(f"Deleted existing index: {INDEX_NAME}")
        
    # Recriar o indice com o mapping
    mapping_path = './efd_ajuste_mapping.json'
    with open(mapping_path, 'r') as file:
        body = json.load(file)
    es.indices.create(index=INDEX_NAME, body=body)

    # Calcular data inicial da carga
    last_yyyymm =(pd.to_datetime(datetime.now()) - pd.DateOffset(years=6, months=1)).strftime('%Y%m')

    # Query no SQL Server
    QUERY_AJUSTE = f"""
    SELECT 
        CONCAT(dc.PK_IE, dc.DIGITO_IE) as IE, 
        RIGHT('00000000000000' + CAST(dc.CNPJ_CPF AS VARCHAR(14)), 14) AS CNPJ_CPF,
        RIGHT('00000000' + CAST(dc.RAIZ_CNPJ AS VARCHAR(8)), 8) AS RAIZ_CNPJ,
        dc.RAZAO_SOCIAL, 
        CAST(CAST(dc.TIPO_PESSOA as int) AS VARCHAR) AS TIPO_PESSOA,
        dc2.Subclasse_Desc AS DESC_CNAE, 
        dc2.Grupo_Ativ_Desc AS GRUPO_ATIVIDADE_DESC,
        dccd.[Categoria Cadastral] AS CATEGORIA_CADASTRAL,
        vfaa.FK_PERIODO PERIODO,
        CONVERT(DATE, CAST(vfaa.FK_PERIODO as varchar) + '01') AS PERIODO_DATA, 
        dt.DESCRICAO AS DESC_TIPO_DECLARACAO,
        CAST(vfed.FINALIDADE as varchar) AS FINALIDADE, 
        vfed.PERFIL, 
        vfed.DESC_BENEFICIO,
        CASE 
            WHEN EXISTS (
                SELECT 1 
                FROM DWRECEITA.dbo.VW_DM_CADASTRO_SIMPLES S
                WHERE S.FK_IE = vfaa.FK_IE_DTE
                AND vfaa.FK_PERIODO BETWEEN S.PERIODO_INI AND S.PERIODO_FIN
            ) THEN '1' 
            ELSE '0' 
        END AS SIMPLES_NACIONAL,
        dat.DESCRICAO AS DESC_APURACAO_TIPO, 
        vdaa.COD_AJUSTE, 
        vdaa.DESCRICAO AS DESCRICAO_AJUSTE, 
        vfaa.VL_AJUSTE AS VALOR_AJUSTE
    FROM DWRECEITA.efd.VW_FT_APURACAO_AJUSTE vfaa 
    INNER JOIN DWRECEITA.efd.VW_DM_APURACAO_AJUSTE vdaa 
        ON vdaa.PK_AJUSTE = vfaa.FK_AJUSTE 
    INNER JOIN DWRECEITA.efd.DM_APURACAO_TIPO dat 
        ON dat.PK_APUR_TIPO = vfaa.FK_APUR_TIPO 
    INNER JOIN DWRECEITA.efd.DM_TIPODECLARACAO dt 
        ON dt.PK_TIPODECLARACAO = vfaa.TIPO_ARQUIVO 
    INNER JOIN DWRECEITA.efd.VW_FT_EFD_DECLARACAO vfed 
        ON vfed.ID_ARQ = vfaa.ID_ARQ 
    INNER JOIN DWRECEITA.dbo.DM_CONTRIBUINTE dc 
        ON dc.PK_IE = vfaa.FK_IE_DTE 
    INNER JOIN DWRECEITA.dbo.DM_CNAE dc2 
        ON dc2.PK_CNAE = dc.FK_CNAE_ECON 
    INNER JOIN DWRECEITA.dbo.DD_CAT_CADASTRAL_DIEF dccd 
        ON dccd.CAT_CADASTRAL = dc.CAT_CADASTRAL 
    WHERE
        vfaa.FK_PERIODO >= {last_yyyymm}
    """

    def fetchdata():
        """
        Busca dados de um banco de dados SQL Server e os retorna em lotes de 1000 linhas.
        Se ocorrer um erro durante a conexão com o banco de dados ou a busca de dados, loga uma mensagem de erro.
        Retorna (yield):
            dict: Um dicionário representando uma linha de dados do banco de dados SQL Server.    
        """
        connection_string = (
            f"DRIVER={{ODBC Driver 18 for SQL Server}};"
            f"SERVER={server};DATABASE={database};UID={username};PWD={password};"
            f"TrustServerCertificate=yes"
        )
        cursor = None  # Inicializa a variável para evitar referência antes da atribuição
        try:
            conn = pyodbc.connect(connection_string)
            cursor = conn.cursor()
            cursor.execute(QUERY_AJUSTE)
            while True:
                rows = cursor.fetchmany(1000)
                if not rows:
                    break
                yield from (
                    {
                        'IE': row[0],
                        'CNPJ_CPF': row[1],
                        'RAIZ_CNPJ': row[2],
                        'RAZAO_SOCIAL': row[3],
                        'TIPO_PESSOA': row[4],
                        'DESC_CNAE': row[5],
                        'GRUPO_ATIVIDADE_DESC': row[6],
                        'CATEGORIA_CADASTRAL': row[7],
                        'PERIODO': row[8],
                        'PERIODO_DATA': row[9],
                        'DESC_TIPO_DECLARACAO': row[10],
                        'FINALIDADE': row[11],
                        'PERFIL': row[12],
                        'DESC_BENEFICIO_DECLARACAO': row[13],
                        'SIMPLES_NACIONAL': row[14],
                        'DESC_APURACAO_TIPO': row[15],
                        'COD_AJUSTE': row[16],
                        'DESCRICAO_AJUSTE': row[17],
                        'VALOR_AJUSTE': row[18]
                    }             
                    for row in rows
                )
        except Exception as e:
            print(f"Error fetching data from SQL Server: {e}")
        finally:
            cursor.close()
            conn.close()

    # Bulk index usando parallel_bulk
    results = helpers.parallel_bulk(
        client=es,
        index=INDEX_NAME,
        actions=fetchdata(),
        chunk_size=500,     
        request_timeout=60,
        raise_on_error=False,
        raise_on_exception=False,
        thread_count=4     
    )
    # Processar results e contar documentos inseridos com sucesso -- Carga Full
    success_count = 0
    for success, info in results:
        if success:
            success_count += 1
        else:
            print("Erro no documento:", info)

    logging.info(f'Número de documentos inseridos: {success_count}')
    logging.info("Bulk indexing completado.")

if __name__ =="__main__":
    main()
