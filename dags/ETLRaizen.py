from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

default_args = {
    'owner': 'Guilherme Ragonesi',
}
### 
def dowload_file():
    from urllib import request
    file_url = 'https://github.com/raizen-analytics/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls'
    file = './Data/Raw/vendas-combustiveis-m3.xls'
    request.urlretrieve(file_url , file )


def convert_file ():
    import subprocess

    bashCommand = """libreoffice --headless --invisible --convert-to xls ./Data/Raw/vendas-combustiveis-m3.xls
    --outdir ./Data/Staging/vendas-combustiveis-m3.xls """
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    process.communicate()

def transform():
    import pandas as pd
    from datetime import datetime

    file="./Data/Staging/vendas-combustiveis-m3.xls/vendas-combustiveis-m3.xls"
    df_petroleo=pd.read_excel(file,sheet_name=1)
    df_diesel=pd.read_excel(file,sheet_name=2)
    df = pd.concat([df_petroleo,df_diesel],ignore_index=True)
    new_coluns= ['Combustível', 'Ano', 'Região', 'UF','1','2','3','4','5','6','7','8','9','10','11','12','total']
    df.columns=new_coluns
    df_tratado=df.melt(id_vars=df.columns[:4],value_vars=df.columns[4:16],var_name="Month",value_name="volume")
    df_tratado['year_month'] = df_tratado['Ano'].astype(str) + '-' + df_tratado['Month']
    df_tratado['year_month']=pd.to_datetime(df_tratado['year_month'])
    df_tratado=df_tratado.rename(columns={'Ano':'ano'})
    df_tratado=df_tratado.rename(columns={'UF':'uf'})
    df_tratado['product'] = df['Combustível'].str[:-5]
    df_tratado['unit']='m3'
    df_tratado=df_tratado.groupby(['year_month','uf','product','unit'])[['volume']].sum().reset_index()
    data=datetime.now()
    df_tratado['created_at']=data
    df_tratado.to_parquet("./Data/Output/ETL_Raizen")

#### não finalizado
#def corrigir_error()
 #   import numpy as np
  #  import pandas as pd
   # df_temp=pd.DataFrame(df,columns=df.columns[4:17])
    #maxvalues=df_temp.max(axis=1)
  #  idmax=df_temp.idxmax(axis=1)
   # df_temp["Total_2"]=maxvalues
   # df_temp.head()
 




def postgres():
    import pandas as pd 
    import psycopg2
    from sqlalchemy import create_engine
    
    df_tratado=pd.read_parquet("./Data/Output/ETL_Raizen")
    engine= create_engine('postgresql://airflow:airflow@postgres:5432/airflow')
    df_tratado.to_sql('output', engine, if_exists='replace', index=False)

    #with engine.connect() as con:
            #con.execute('Create TABLE output ALTER COLUMN year_month TYPE date USING(year_month::date)')
            #con.execute('ALTER TABLE output ALTER COLUMN year_month TYPE date USING(year_month::date)')
            #con.execute('ALTER TABLE output ALTER COLUMN created_at TYPE timestamp USING(created_at::timestamp)')
            #con.execute('CREATE INDEX output_idex ON diesel_sales (year_month, uf, product);')

with DAG(
    'ETL_Raizen',
    default_args=default_args,

    #### reescrever
    description='values dag',
    schedule_interval=None,
    start_date=datetime(2022,5,8),
    tags=[],
) as dag:


    dowload_file = PythonOperator(
        task_id='dowload_file',
        python_callable=dowload_file,
    )

    convert_file = PythonOperator(
        task_id='convert_file',
        python_callable=convert_file,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform,
    )

    postgres = PythonOperator(
        task_id='postgres',
        python_callable=postgres,
    )


dowload_file>>convert_file>>transform>>postgres