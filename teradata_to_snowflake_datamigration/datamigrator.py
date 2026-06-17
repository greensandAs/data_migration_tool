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
import teradatasql
import pandas as pd
from tpt_utils import tpt_script_generator,tptexport
from td_utils import getcolumninfo,tdquery,tdcount
from cloud_utils import cloud_upload,az_archive
from sf_utils import create_table,create_stage,copycommand,getcdcdates,mergecommand,auditupdate,sfcount,sfquery
from logger import batch_create,log_update
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session
from threading import Lock
import multiprocessing
import shutil
import json

thread_local_data = threading.local()
lock = multiprocessing.Lock()
#lock = threading.Lock()
#lock = Lock()




def datamigration(task):
    time.sleep(1)
    job=task

    execution_mode=job[18]
    if execution_mode == 'FULL':
        return full_execution(task)
    
    elif execution_mode == 'EXPORT':
        return export_only(task)
    
    elif execution_mode == 'INGEST':
        return ingest_only(task)
    


def export_only(task):

    job=task
    rc_sum=0
    tddbname=job[0]
    tdtablename=job[1]
    cloud_path=job[14]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    loadtype=job[7]
    batch_id=job[15]
    job_id=job[16]
    custom_sql=job[17]
    print(tddbname,tdtablename,loadtype,custom_sql)
    

 

    returncode,errormsg,tptfilename,tptcontent,exportfilename,extract_start_dttm,extract_end_dttm,colstr=tpt_script_generator(job)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tpt_script_generator',[returncode,errormsg,tptfilename,tptcontent],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename    



    print(returncode,errormsg)

    
    returncode,tpt_cmd,stdout=tptexport(tptfilename,exportfilename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tptexport',[returncode,tpt_cmd,stdout,exportfilename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    returncode,tdcnt=tdcount(stdout)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum,tdcnt)
    log_update('tdcount',[returncode,tdcnt],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename
    

 
    uploadfilename=exportfilename.replace('.csv','')
    print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")

    
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename
    
    print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")
    
    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)


    print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("AUDIT UPDATE STARTED FOR :",sftablename)

def ingest_only(task):

    job=task
    rc_sum=0
    tddbname=job[0]
    tdtablename=job[1]
    cloud_path=job[14]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    loadtype=job[7]
    batch_id=job[15]
    job_id=job[16]
    custom_sql=job[17]
    
    log_update('start_time_update',[0],batch_id,job_id)

    print(tddbname,tdtablename,loadtype,custom_sql)
    
    extract_start_dttm = 'NULL'
    extract_end_dttm = 'NULL'
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print(result)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename    

    print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename)
    


    print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    print("CREAT STAGE COMPLETED FOR:",sftablename)

    print("VARADHA")
    print("COPY COMMAND STARTED FOR :",sftablename)
    
    query_ingest_only = f"""SELECT * FROM AUDIT_TABLE WHERE INGESTION_COMPLETED = 'NO' AND TD_TABLE_NAME = '{tdtablename}' AND TD_DATABASE_NAME = '{tddbname}' AND EXECUTION_MODE = 'EXPORT' AND FINAL_STATUS = 'SUCCESS' ORDER BY BATCH_ID ASC; """

    list_of_files = sfquery(query_ingest_only)
    rc_code = 0
    cum_copystmnt = ''
    cum_merstmnt = ''
    cum_result_copystmnt = ''
    cum_result_merstmnt = ''
    print(list_of_files,'MAHAVISHNU')
    for file in list_of_files:
        print(file)
        uploadfilename = file[27]
        for j in file:
            print(j,'FILE DETAILS KRISHNA')
        print(uploadfilename,'UPLOAD FILENAME KESAVA')
        returncode,copystmnt,result_copystmnt=copycommand(stagename,job,uploadfilename)
        rc_sum=rc_sum+returncode
        print("RC_SUM",rc_sum)
        print(returncode,copystmnt,result)

        cum_copystmnt = cum_copystmnt + '\n' + copystmnt
        cum_result_copystmnt = cum_result_copystmnt + '\n' + result_copystmnt

        log_update('copycommand',[returncode,cum_copystmnt,cum_result_copystmnt],batch_id,job_id)
        
        if returncode != 0:
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
            rc_code= rc_code + returncode

        print("COPY COMMAND COMPLETED FOR :",sftablename)

        

        print("MERGE STATEMENT STARTED FOR :",sftablename)
        returncode,merstmnt,result_merstmnt=mergecommand(job,uploadfilename)
        print("RC_SUM",rc_sum)
        print("MERGE STATEMENT COMPLETED FOR :",sftablename)
        rc_sum=rc_sum+returncode

        cum_merstmnt = cum_merstmnt + '\n' + merstmnt
        cum_result_merstmnt = cum_result_merstmnt + '\n' + result_merstmnt 

        log_update('mergecommand',[returncode,cum_merstmnt,cum_result_merstmnt],batch_id,job_id)
        
        if returncode != 0:
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
            rc_code= rc_code + returncode

    if rc_code != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename 


    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)


    print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    
    #log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    #if returncode != 0:
    #    return sftablename
    returncode,archive_log = az_archive(cloud_path,uploadfilename,batch_id)
    print("AZURE ARCHIVE RC",returncode,archive_log)
    return sftablename




def full_execution(task):
    #time.sleep(1)
    job=task
    rc_sum=0
    tddbname=job[0]
    tdtablename=job[1]
    cloud_path=job[14]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    loadtype=job[7]
    batch_id=job[15]
    job_id=job[16]
    custom_sql=job[17]
    print(tddbname,tdtablename,loadtype,custom_sql)
    
    tim0=time.time()
    log_update('initial_start',['RUNNING'],batch_id,job_id)

    returncode,errormsg,tptfilename,tptcontent,exportfilename,extract_start_dttm,extract_end_dttm,colstr=tpt_script_generator(job)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tpt_script_generator',[returncode,errormsg,tptfilename,tptcontent],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename    



    print(returncode,errormsg)

    
    returncode,tpt_cmd,stdout=tptexport(tptfilename,exportfilename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('tptexport',[returncode,tpt_cmd,stdout,exportfilename],batch_id,job_id)

    tim1=time.time()
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    returncode,tdcnt=tdcount(stdout)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum,tdcnt)
    log_update('tdcount',[returncode,tdcnt],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename
    


    uploadfilename=exportfilename.replace('.csv','')
    print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")

    
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)
    
    tim2=time.time()
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename
    
    print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")

    
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print(result)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename    

    print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)
    


    print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    print("CREAT STAGE COMPLETED FOR:",sftablename)


    print("COPY COMMAND STARTED FOR :",sftablename)
    
    returncode,copystmnt,result=copycommand(stagename,job,uploadfilename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print(returncode,copystmnt,result)


    log_update('copycommand',[returncode,copystmnt,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    print("COPY COMMAND COMPLETED FOR :",sftablename)

    print("MERGE STATEMENT STARTED FOR :",sftablename)
    returncode,merstmnt,result=mergecommand(job,uploadfilename)
    print("RC_SUM",rc_sum)
    print("MERGE STATEMENT COMPLETED FOR :",sftablename)
    rc_sum=rc_sum+returncode
    log_update('mergecommand',[returncode,merstmnt,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    tim3=time.time()

    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    


    print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
    rc_sum=rc_sum+returncode
    print("RC_SUM",rc_sum)
    print("AUDIT UPDATE STARTED FOR :",sftablename)
    
    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    returncode,archive_log = az_archive(cloud_path,uploadfilename,batch_id)
    print("AZURE ARCHIVE RC",returncode,archive_log)

    #if returncode != 0:
    #    return sftablename
    print(tim0)
    print(tim1)
    print(tim2)
    print(tim3)
    return sftablename

if __name__ == "__main__":

    start=time.time()

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    job_parallelism = int(cred['job_parallelism'])

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

    #query="""SELECT * FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE;"""
    query="""SELECT TD_DATABASE_NAME,TD_TABLE_NAME,SF_DATABASE_NAME,SF_SCHEMA_NAME,SF_TABLE_NAME,WAREHOUSE_NAME,SCD_TYPE,LOAD_TYPE,CDC_COLUMNS,PRIMARY_KEY,DELIMITER,FILTER_CONDITION,
            TRIM,ENCRYPTION_COLUMNS,S3_PATH,(SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000)) AS BATCH_ID,JOB_ID,CUSTOM_SQL,EXECUTION_MODE,CDC_TYPE FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE WHERE ENABLED = 'Y';"""
    
    spsession=Session.builder.configs(spcon).create()
    batch=spsession.sql(query)

    
     

    #print(list(test.collect()))
    config=batch.collect()
    configtable=list(config)

    try:
        batch_create()
    except Exception as e:
        print(e)
        print("NOT ABLE TO CREATE LOG")
        exit()


    '''
    config=pd.read_sql(query, sfcon)
    configtable=config.values.tolist()
    '''
    
    #tptlogdir=r"/media/ssd/tptlog"
        
    #tpt_jobs=[]
    
    '''
    with ThreadPoolExecutor() as executor:
        for job in configtable:
            sts=executor.submit(tpt_script_generator,job)
            #print(sts.result(),"Return Code")
        #print("s")
    '''
    timb=time.time()
    print(timb)
    with ProcessPoolExecutor(max_workers=job_parallelism) as executor:
        status_code_tpt_scr_gen = {executor.submit(datamigration, task): task for task in configtable}
        for return_code in as_completed(status_code_tpt_scr_gen):
            print(return_code.result(),"Return Code")
    

    '''
    for i in configtable:
        res=datamigration(i)
        print(res)

    '''

    #status=tpt_script_generator(configtable)
    #print("Krishna")
    #print(status)
    
    '''
    for tptscrptnm in tpt_jobs:
        print(tptscrptnm)
        #cmd=f"tbuild -f {tptscrptnm} -C"
        cmd = ["tbuild", "-f", tptscrptnm, "-C"]
        #t=subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)
        print("Damodhara")
        print(cmd)
        t=subprocess.run(cmd, capture_output=True, text=True)
        
        print(t.returncode)
        
        print("SriRanga")
        print(t.stdout)
        #print(t.stdout)
    '''
    #EXPORT FILES FROM TERADATA
    
    '''
    with ProcessPoolExecutor() as executor:
        print("Kanna")
        status_code_tpt={executor.submit(tptexport,tptscptnm_filename[0],tptscptnm_filename[1],tptscptnm_filename[2]): tptscptnm_filename for tptscptnm_filename in tpt_jobs}
    '''
    
    '''
    for tptscrptnm in tpt_jobs:
        print("Export started for :" ,tptscrptnm)
        tptexport(tptscrptnm)
        print("Export Completed for :",tptscrptnm)
    '''

    print("Extraction completed")
    #print(tpt_jobs)
    end=time.time()
    print(end-start)
