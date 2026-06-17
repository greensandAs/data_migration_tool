# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 15:10:59 2024

@author: DINESH_MALLIKARJUNAN
"""
import os
import subprocess
import datetime
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import threading
import snowflake.connector
#import teradatasql
import pandas as pd
#from cloud_utils import cloud_upload
from sf_utils_ing import create_target_table,auditupdate,sfcount,src_cnt,create_file_format,copy_ingestion,ingestion,insert_audit_batch
from logger_ing import batch_create,log_update
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from threading import Lock
import multiprocessing
import shutil
import json
import argparse
from datetime import datetime


thread_local_data = threading.local()
lock = multiprocessing.Lock()
#lock = threading.Lock()
#lock = Lock()



def dataingest(task):
    time.sleep(1)
    
    rc_sum=0
    thread_local_data.job=task
    job = thread_local_data.job
    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9]
    field_delimiter = job[10]
    field_optionally_enclosed_by = job[11]
    escape_character = job[12]
    skip_header = job[13]
    additional_file_format_options = job[14]
    additional_copy_into_options = job[15]
    table_exists = job[16]

        
    print(job_id, batch_id, file_pattern, cloud_path, sf_database_name, sf_schema_name, sf_table_name, warehouse_name, load_mode, file_type, field_delimiter, field_optionally_enclosed_by, escape_character, skip_header, additional_file_format_options)
    
    
    returncode,file_format_obj_name,file_format_obj_stmt ,file_format_obj_log, file_format_obj_status = create_file_format(job)
    rc_sum=rc_sum+returncode
    log_update('create_file_format',[returncode,file_format_obj_stmt ,file_format_obj_log, file_format_obj_status],batch_id,job_id)
    
    if returncode != 0:
        return sf_table_name 
    

    returncode,src_count,src_info,act_path = src_cnt(file_pattern,cloud_path,file_format_obj_name)
    rc_sum=rc_sum+returncode
    print(returncode,src_count,src_info)

    log_update('src_cnt',[returncode,src_count,src_info],batch_id,job_id)

    if returncode != 0:
        return sf_table_name 
    

    returncode,create_target_table_log = create_target_table(job,file_format_obj_name,act_path)
    rc_sum=rc_sum+returncode
    print(returncode,create_target_table_log)

    log_update('create_target_table',[returncode,create_target_table_log],batch_id,job_id)

    if returncode != 0:
        return sf_table_name


    returncode,ingestion_stmt,ingestion_log,ingestion_cnt = copy_ingestion(job,act_path,file_format_obj_name)
    rc_sum=rc_sum+returncode
    print(returncode,ingestion_stmt,ingestion_log,ingestion_cnt)

    log_update('copy_ingestion',[returncode,ingestion_stmt,ingestion_log,ingestion_cnt],batch_id,job_id)

    if returncode != 0:
        return sf_table_name
    

    returncode_final=rc_sum
    log_update('final_status',[returncode_final],batch_id,job_id)
    return sf_table_name


     

if __name__ == "__main__":

    start=time.time()
    #'/media/ssd/python/credentials.json'

    #C:\Users\dines\Downloads\GCP_VM\python\credentials.json
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    job_parallelism = int(cred['job_parallelism'])
    sf_config_table = cred['sf_config_table']
    sf_log_table = cred['sf_log_table']
    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema": sf_schema
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('param', nargs='*')
    args = parser.parse_args()
    job_id = ""
    count = len(args.param)
    if count == 0:
        print("No parameters:", args.param)
        path_ext = "''"
        where_condition = "(1=1)"
    elif count == 1:
    
        print("one parameters:", args.param)
    
        job_id = args.param[0]
        path_ext = "''"

        where_condition = f" JOB_ID = '{args.param[0]}' "
    
    elif count == 2:
        
        print("Two parameters:", args.param)
        
        job_id = f"'{args.param[0]}'"
        path_ext = f"'{args.param[1]}'"

        if path_ext == "'CURR_DATE'":
            current_date = datetime.now()
            path_ext = str(current_date.strftime("%Y%m%d"))
            path_ext = f"'{path_ext}/'"

        where_condition = f" JOB_ID = '{args.param[0]}'  "
    
    else:
        print("Invalid number of parameters")
    
    print(where_condition)

    query=f"""SELECT JOB_ID, (SELECT COALESCE((SELECT MAX(CAST(BATCH_ID AS INT)) FROM {sf_log_table})+1,10000)) AS BATCH_ID, FILE_PATTERN, CONCAT(CLOUD_PATH,{path_ext}), SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, WAREHOUSE_NAME, LOAD_MODE, FILE_TYPE, FIELD_DELIMITER, FIELD_OPTIONALLY_ENCLOSED_BY, ESCAPE_CHARACTER, SKIP_HEADER, ADDITIONAL_FILE_FORMAT_OPTIONS, ADDITIONAL_COPY_INTO_OPTIONS, TABLE_EXISTS FROM {sf_config_table} WHERE{where_condition};"""
    
    print(query)
    spsession=Session.builder.configs(spcon).create()
    batch=spsession.sql(query)

    
    config=batch.collect()
    configtable=list(config)
    print(configtable)
    if(len(configtable)==0):
        print("Please Provide Correct Input/Congifs")
        exit()
    try:
        batch_create(where_condition,path_ext)
    except Exception as e:
        print(e)
        print("NOT ABLE TO CREATE LOG")
        exit()

    


    
    with ThreadPoolExecutor(max_workers=job_parallelism) as executor:
        status_code_tpt_scr_gen = {executor.submit(dataingest, task): task for task in configtable}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(),"Return Code")
    
    return_code=insert_audit_batch(configtable[0][1])

    print("Extraction completed")
    #print(tpt_jobs)
    end=time.time()
    print(end-start)
    