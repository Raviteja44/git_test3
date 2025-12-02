# Copyright(c) 2022 KPI Partners, Inc. All Rights Reserved.
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#
# author: KPI Partners, Inc.
# version: 2022.06
# description: This script represents to build Airflow DAG to perform full and incremental loads of DW tables. Stage, ODS, Dimesnions and Facts tables are loaded in the order.
# File Version: KPI v2.0
# Updated made to v2.0 : Added logic for checking ascp_dms_flag and trigerring ASCP/WIP VAR based on the flag content and archiving the flag data
# Updates made to v3.0: Added logic to trigger the custom dag for Quicksight spice datasets refresh

from airflow import DAG
from Airflow_custom_functions_bi_dashboard import *
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import ShortCircuitOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import *
from airflow.operators.email_operator import EmailOperator
from redshift_utils import redshift_connection as rs
import pprint
from datetime import datetime, date, timedelta
import calendar
import boto3
import json
import qs_refresh

#Adding details of QS_refresh code partition
1234
sdfg
89nhmk


# STATIC VARIABLES
# =====================================================


PWD = Variable.get('password')
USERNAME = Variable.get('username')
DATABASE = Variable.get('database')
HOST = Variable.get('host')
AWS_REGION = Variable.get('aws_region')
ENV = Variable.get('environment')
DATE = Variable.get('date')

DMS_BUCKET = Variable.get("dms_flag_bucket")
DMS_KEY = Variable.get("dms_flag_key")

PORT = int(Variable.get('port'))
SCHEDULE_INTERVAL = Variable.get('schedule_interval_LoadCommon')
SCHEDULE_INTERVAL = None if SCHEDULE_INTERVAL == 'None' else SCHEDULE_INTERVAL
EXECUTION_TIMEOUT = int(Variable.get('task_execution_timeout_min_LoadCommon'))
DAG_EXECUTION_TIMEOUT = int(Variable.get('dag_execution_timeout_min_LoadCommon'))
year = DATE[0:4]
month = DATE[5:7]
day = DATE[8:10]
YEAR, MONTH, DAY = int(year), int(month), int(day)
MAIL_ID = Variable.get('mail_id')
rsconn = rs()
pp = pprint.PrettyPrinter(indent=4)
START_HOUR = int(Variable.get('start_hour'))
END_HOUR = int(Variable.get('end_hour'))
# RUN_DAY1=Variable.get('run_day1')
# RUN_DAY2=Variable.get('run_day2')
BATCH_NAME = 'common'

# each Workflow/DAG must have a unique text identifier
WORKFLOW_DAG_ID = 'LOAD_COMMON'

# start/end times are datetime objects
WORKFLOW_START_DATE = datetime(YEAR, MONTH, DAY)

# schedule/retry intervals are timedelta objects
# here we execute the DAGs tasks every 20 minutes
WORKFLOW_SCHEDULE_INTERVAL = SCHEDULE_INTERVAL
DWH_CONNECTION_INFO = {"username": USERNAME, "password": PWD, "host": HOST, "port": PORT, "database": DATABASE}

# PASSING CONNECTION PARAMETERS TO AIRFLOW FUNCTIONS

# default arguments are applied by default to all tasks
# in the DAG
WORKFLOW_DEFAULT_ARGS = {
    'owner': 'BE-EDP',
    'depends_on_past': False,
    'start_date': WORKFLOW_START_DATE,
    'email': MAIL_ID,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=1)
}

# Querying from the tables
# =========================================================

# Querying the batch_variable_info table to get the variable list

sql_variables = "SELECT * FROM bec_etl_ctrl.batch_variable_info order by seq_num"
variable_df = rsconn.get_data(DWH_CONNECTION_INFO, AWS_REGION, sql_variables)
variable_list = variable_df.values.tolist()
Bucket_Name = variable_list[0][2]
stage_schema_name = variable_list[2][2]
ods_schema_name = variable_list[3][2]
analytics_schema_name = variable_list[4][2]
etl_script_folder_path = variable_list[5][2]

concurrency_variable = "SELECT * FROM bec_etl_ctrl.batch_variable_info where variable_key='common'"
concurrency_df = rsconn.get_data(DWH_CONNECTION_INFO, AWS_REGION, concurrency_variable)
concurrency_list = concurrency_df.values.tolist()
LoadCommon_concurrency = concurrency_list[0][2]

CONCURRENCY = 10
if LoadCommon_concurrency is not None:
    CONCURRENCY = int(LoadCommon_concurrency)

# print("&&&&& ",Bucket_Name, ' ',analytics_schema_name,' ',stage_schema_name)


# initializing the ODS, DIM, FACT and RT tables variables list
ods_table_list = ''

# Query batch_ods_info table to get all ODS table names


sql_ods = "select ods_table_name from bec_etl_ctrl.batch_ods_info where refresh_frequency = 'Daily' and disable_flag = false and batch_name in ('common')";

records_df = rsconn.get_data(DWH_CONNECTION_INFO, AWS_REGION, sql_ods)
records = records_df.values.tolist()
# print("&&&&& ",records)
for o_table in records:
    ods_table_list = ods_table_list + o_table[0] + ','
ods_table_list = ods_table_list[:-1]
# print (ods_table_list)

'''init_parameters'''

def CheckDateTime():
    x = datetime.now()
    print(x)
    hour = (x.hour)
    print(hour)
    date1 = date.today()
    weekday1 = calendar.day_name[date.weekday(date1)]
    print(weekday1)

    if hour >= START_HOUR and hour < END_HOUR:
        # and (weekday1==RUN_DAY1 or weekday1==RUN_DAY2):
        return 'ASCP_WIP_VAR_Trigger'
    else:
        return 'Branch_Complete'


def check_dms_flag_for_branch():
    s3 = boto3.client('s3')
    print(f"Checking DMS flag at s3://{DMS_BUCKET}/{DMS_KEY}")
    try:
        obj = s3.get_object(Bucket=DMS_BUCKET, Key=DMS_KEY)
        content = obj['Body'].read().decode('utf-8')
        print(f"Raw flag content: {content}") 
        flag_data = json.loads(content)
        flag_value = flag_data.get('flag', '').lower()
        print(f"DMS Flag value for branching: {flag_value}")
        if flag_value == 'yes':
            print("Proceeding with ASCP/WIP_VAR trigger")
            return 'ASCP_WIP_VAR_Trigger'
        else:
            print("Skipping ASCP/WIP_VAR trigger") 
            return 'Branch_Complete'
    except Exception as e:
        print(f"Error checking DMS flag for branching: {e}")
        return 'Branch_Complete'


def convert_pst():
    max_batch_id = "select max(id)  from bec_etl_ctrl.bec_etl_summary where dag_name ='LOAD_COMMON'"
    load_max_batch_id_df = rsconn.get_data(DWH_CONNECTION_INFO, AWS_REGION, max_batch_id)
    load_max_batch_id_list = load_max_batch_id_df.values.tolist()
    load_max_batch_id_list = int(load_max_batch_id_list[0][0])
    batch_id=load_max_batch_id_list
    #batch_id = rsconn.get_data(DWH_CONNECTION_INFO, AWS_REGION, batch_id)
    print("batch_id", batch_id)
    sql_batch_id = "update bec_etl_ctrl.bec_etl_summary set dag_end_time = (select max(last_refreshed) from bec_etl_ctrl.bec_etl_summary where current_status = 'Complete' and batch_id = (select max(batch_id) from bec_etl_ctrl.bec_etl_summary where dag_name = 'LOAD_COMMON') and dag_name not in  ('ASCP_AIRFLOW_ETL_LOAD','WIP_VAR_AIRFLOW_ETL_LOAD','LOAD_COMMON'))where current_status = 'Complete' and batch_id = '{}' and dag_name='LOAD_COMMON'".format(batch_id)
    sql_batch_id = rsconn.execute_query(DWH_CONNECTION_INFO, AWS_REGION, sql_batch_id)
    print("batch_id", batch_id)

def archive_and_reset_dms_flag(bucket_name, key, archive_prefix='dms_status/archive/'):
    s3 = boto3.client('s3')
    print(f"Starting archive process for {bucket_name}/{key}")

    try:
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        content = obj['Body'].read().decode('utf-8')
        timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        archive_key = f"{archive_prefix}ascp_dms_flag_{timestamp}.json"
        
        print(f"Archiving to {archive_key}") 
        s3.copy_object(
            Bucket=bucket_name,
            CopySource={'Bucket': bucket_name, 'Key': key},
            Key=archive_key
        )
        new_flag_data = {
            "flag": "No",
            "date": datetime.utcnow().strftime('%Y-%m-%dT%H-%M-%S')
        }
        print(f"Resetting flag to: {new_flag_data}")
        s3.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=json.dumps(new_flag_data),
            ContentType='application/json'
        )
        print("Archive and reset completed successfully")
    except Exception as e:
        print(f"Error archiving and resetting DMS flag: {e}")



# initialize the DAG
# ======================================================================

dag = DAG(
    dag_id=WORKFLOW_DAG_ID,
    start_date=WORKFLOW_START_DATE,
    schedule_interval=WORKFLOW_SCHEDULE_INTERVAL,
    default_args=WORKFLOW_DEFAULT_ARGS,
    concurrency=CONCURRENCY,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=DAG_EXECUTION_TIMEOUT)
)



# TASKS DEFINITION
# ======================================================================

# ITERATING THE ODS_TABLE_LIST AND CALL THE RUN_STAGE_QUERIES FUNCTION TO PULL THE DATA FROM EXTERNAL SCHEMA TO STAGE


if len(ods_table_list) > 0:
    init_parameters = PythonOperator(task_id='Load_base_tables', python_callable=dag_start, dag=dag)
    stage_Load_Success = DummyOperator(task_id='Level_1_Load_Success', dag=dag)


    for tableName in ods_table_list.split(','):
        load_base_stg = PythonOperator(
            task_id=tableName + '_stage',
            op_kwargs={'table_name': tableName},
            op_args=[Bucket_Name, etl_script_folder_path],
            python_callable=run_stage_queries,
            provide_context=True,
            execution_timeout=timedelta(minutes=EXECUTION_TIMEOUT),
            dag=dag)
        init_parameters >> load_base_stg
        load_base_stg >> stage_Load_Success

# ITERATING THE ODS_TABLE_LIST AND CALL THE RUN_ODS_QUERIES FUNCTION TO PULL THE DATA FROM STAGE SCHEMA TO ODS

if len(ods_table_list) > 0:
    # ods_success = BranchPythonOperator(task_id='Level_2_Load_success',python_callable=check_dms_flag_for_branch,dag=dag)
    ods_success = DummyOperator(task_id='Level_2_Load_Success', dag=dag)

    for tableName in ods_table_list.split(','):
        load_base_ods = PythonOperator(
            task_id=tableName + '_ods',
            op_kwargs={'table_name': tableName},
            op_args=[Bucket_Name, etl_script_folder_path],
            python_callable=run_ods_queries,
            provide_context=True,
            execution_timeout=timedelta(minutes=EXECUTION_TIMEOUT),
            dag=dag)
        stage_Load_Success >> load_base_ods
        load_base_ods >> ods_success


    #Declaring required Python operators

    start_branch = BranchPythonOperator(task_id='Start_Branch', python_callable=check_dms_flag_for_branch, dag=dag)
    branch = DummyOperator(task_id='Branch_Complete', dag=dag)
    load_success = PythonOperator(task_id='Common_Load_completed', op_args=[BATCH_NAME], python_callable=dag_success,dag=dag)
 
    
    update_to_pst = PythonOperator(task_id="EBS_Load_complete", python_callable=convert_pst)
    load_error = PythonOperator(task_id='Load_error', python_callable=dag_error, dag=dag, trigger_rule='one_failed')

   

    trigger_ext_dag_AP = TriggerDagRunOperator(task_id="Trigger_Dag_AP",
                                               trigger_dag_id="AP_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )
                                               
    # branch>>trigger_ext_dag_AP
    load_success >> trigger_ext_dag_AP

    trigger_ext_dag_AR = TriggerDagRunOperator(task_id="Trigger_Dag_AR",
                                               trigger_dag_id="AR_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )
    # branch >> trigger_ext_dag_AR
    load_success >> trigger_ext_dag_AR

    trigger_ext_dag_PO = TriggerDagRunOperator(task_id="Trigger_Dag_PO",
                                               trigger_dag_id="PO_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )

    # branch >> trigger_ext_dag_PO
    load_success >> trigger_ext_dag_PO

    trigger_ext_dag_GL = TriggerDagRunOperator(task_id="Trigger_Dag_GL",
                                               trigger_dag_id="GL_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )

    # branch >> trigger_ext_dag_GL
    load_success >> trigger_ext_dag_GL

    trigger_ext_dag_INV_COSTING_WIP = TriggerDagRunOperator(task_id="Trigger_Dag_INV",
                                                           trigger_dag_id="INV_AIRFLOW_ETL_LOAD",
                                                            wait_for_completion=True,
                                                            )

    # branch >> trigger_ext_dag_INV_COSTING_WIP
    load_success >> trigger_ext_dag_INV_COSTING_WIP

    trigger_ext_dag_FA = TriggerDagRunOperator(task_id="Trigger_Dag_FA",
                                               trigger_dag_id="FA_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )

    # branch >> trigger_ext_dag_FA
    load_success >> trigger_ext_dag_FA

    trigger_ext_dag_OM = TriggerDagRunOperator(task_id="Trigger_Dag_OM",
                                               trigger_dag_id="OM_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )

    # branch >> trigger_ext_dag_OM
    load_success >> trigger_ext_dag_OM

    trigger_ext_dag_SC = TriggerDagRunOperator(task_id="Trigger_Dag_SC",
                                               trigger_dag_id="SC_AIRFLOW_ETL_LOAD",
                                               wait_for_completion=True,
                                               )

    # branch >> trigger_ext_dag_SC
    load_success >> trigger_ext_dag_SC


    trigger_ext_dag_ASCP = TriggerDagRunOperator(
        task_id="Trigger_Dag_ASCP",
        trigger_dag_id="ASCP_AIRFLOW_ETL_LOAD",
        wait_for_completion=True, 
        
        )

    trigger_ext_dag_WIP_VAR = TriggerDagRunOperator(
        task_id="Trigger_Dag_WIP_VAR",
        trigger_dag_id="WIP_VAR_AIRFLOW_ETL_LOAD",
        wait_for_completion=True,        
        )

    update_to_pst >> start_branch
    start_branch >> branch

    dag_trigger_for_daily_once_for_a_day = DummyOperator(task_id='ASCP_WIP_VAR_Trigger', dag=dag)
    start_branch >> dag_trigger_for_daily_once_for_a_day

    dag_trigger_for_daily_once_for_a_day >> trigger_ext_dag_ASCP
    dag_trigger_for_daily_once_for_a_day >> trigger_ext_dag_WIP_VAR

    archive_and_reset_task = PythonOperator(
        task_id='Archive_And_Reset_DMS_Flag',
        python_callable=archive_and_reset_dms_flag,
        op_args=[DMS_BUCKET, DMS_KEY],
        dag=dag
    	)
     
    dag_trigger_for_daily_once_for_a_day >> archive_and_reset_task


    #trigger_ext_dag_SECURITY = TriggerDagRunOperator(task_id="Trigger_Dag_SECURITY",
    #                                                 trigger_dag_id="AIRFLOW_SECURITY_TABLES",
    #                                                 wait_for_completion=True,
    #                                                 )
    # branch >> trigger_ext_dag_SECURITY
    #load_success >> trigger_ext_dag_SECURITY

    trigger_ext_dag_COSTING = TriggerDagRunOperator(task_id="Trigger_Dag_COSTING",
                                                    trigger_dag_id="COSTING_AIRFLOW_ETL_LOAD",
                                                    wait_for_completion=True,
                                                    )
    # branch >> trigger_ext_dag_COSTING
    load_success >> trigger_ext_dag_COSTING

    trigger_ext_dag_WIP = TriggerDagRunOperator(task_id="WIP_Dag_WIP",
                                                trigger_dag_id="WIP_AIRFLOW_ETL_LOAD",
                                                wait_for_completion=True,
                                               )
    # branch >> trigger_ext_dag_WIP
    load_success >> trigger_ext_dag_WIP
    
    trigger_refresh_all_quicksight_datasets = TriggerDagRunOperator(task_id="Trigger_QS_Refresh",
                                              trigger_dag_id="REFRESH_ALL_QUICKSIGHT_DATASETS", 
                                              wait_for_completion=False, 
                                              dag=dag
                                                )  

    [trigger_ext_dag_AP, trigger_ext_dag_AR, trigger_ext_dag_PO, trigger_ext_dag_INV_COSTING_WIP, trigger_ext_dag_FA,
     trigger_ext_dag_OM, trigger_ext_dag_GL, 
     #trigger_ext_dag_SECURITY, 
     trigger_ext_dag_SC, trigger_ext_dag_COSTING,
     trigger_ext_dag_WIP] >> update_to_pst >> trigger_refresh_all_quicksight_datasets

    trigger_ext_dag_SFC = TriggerDagRunOperator(task_id="Trigger_Dag_SFC",
                                                trigger_dag_id="SFC_AIRFLOW_ETL_LOAD",
                                                wait_for_completion=False,
                                                )

    update_to_pst >> trigger_ext_dag_SFC

ods_success >> load_success
ods_success >> load_error