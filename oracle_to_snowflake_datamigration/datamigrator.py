# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 15:10:59 2024

@author: DINESH_MALLIKARJUNAN
"""
import os
import argparse
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
from oracle_utils import oracle_query,oracle_count,oracle_export #getcolumninfo
from cloud_utils import cloud_upload,az_archive
from sf_utils import create_table,create_stage,copycommand,mergecommand,auditupdate,sfcount,sfquery
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

##print("kasava") 



def datamigration(task):
    ##print("KODANDAPANI")
    job=task

    execution_mode=job[18]
    oracletablename=job[1]
    print(f"Migration started for {oracletablename} with execution mode as {execution_mode}")
    if execution_mode == 'FULL':
        return full_execution(task)
    
    elif execution_mode == 'EXPORT':
        return export_only(task)
    
    elif execution_mode == 'INGEST':
        return ingest_only(task)
    


def export_only(task):
    # #print("Srirama")
    job=task
    rc_sum=0
    job_id=job[16]
    batch_id=job[15]
    oracleschemaname=job[0]
    oracletablename=job[1]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    sfwarehouse=job[5]
    scdtype=job[6]
    loadtype=job[7]
    cdccolumns=job[8]
    primarykey=job[9]
    delimiter=job[10]
    filtercondition=job[11]
    trim=job[12]
    encryptioncolumns=job[13]
    cloud_path=job[14]
    # customsql=job[16]
    # executionmode=job[17]
    # enabled=job[18]
    # #print(f"schema : {oracleschemaname} , table : {oracletablename}")
    # returncode,oraclecount=oracle_count(oracleschemaname,oracletablename)
    # rc_sum=rc_sum+returncode

    # #print("RC_SUM",rc_sum)

    # log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)
    
    # if returncode != 0:
    #     return sftablename
    print(f"Export started for {oracletablename}")
    
    returncode,returnmessage,exportedfilenames_list,exportedfilename,extract_start_dttm,extract_end_dttm,cdc_id=oracle_export(oracleschemaname,oracletablename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    
    # #print("RC_SUM",rc_sum)
    #print(f"exportedfilenames,exportedfilename: {exportedfilenames_list,exportedfilename}")
    log_update('oracleexport',[returncode,returnmessage,exportedfilenames_list,exportedfilename],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    

    ##print("MADHAVA") 
    uploadfilename=exportedfilename.replace('.csv','')
    #print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")
    ##print("GOVINDA")
    
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)

    print(f"Cloud Upload completed for {oracletablename}")
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    
    #print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    ##print("Srirama")
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    #print(f"RUN DETAILS : {job,extract_start_dttm,extract_end_dttm,batch_id,job_id}")
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    # print(f"Log and Audit update complted for {oracletablename} in Export Only Mode")
    print(f"Export completed for {oracletablename}")
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE STARTED FOR :",sftablename)

def ingest_only(task):
    ##print("Srirama")
    job=task
    rc_sum=0
    job_id=job[16]
    batch_id=job[15]
    oracleschemaname=job[0]
    oracletablename=job[1]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    sfwarehouse=job[5]
    scdtype=job[6]
    loadtype=job[7]
    cdccolumns=job[8]
    primarykey=job[9]
    delimiter=job[10]
    filtercondition=job[11]
    trim=job[12]
    encryptioncolumns=job[13]
    cloud_path=job[14]
    customsql=job[17]
    # executionmode=job[17]
    # enabled=job[18]
    #print(f"schema : {oracleschemaname} , table : {oracletablename}")
    # returncode,oraclecount=oracle_count(oracleschemaname,oracletablename)
    # rc_sum=rc_sum+returncode

    # #print("RC_SUM",rc_sum)

    # log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)
    
    # if returncode != 0:
    #     return sftablename
    print(f"Ingestion Started for {oracletablename}")
    log_update('start_time_update',[0],batch_id,job_id)
    ##print("ranga",tddbname,tdtablename,loadtype,custom_sql)
    
    extract_start_dttm = 'NULL'
    extract_end_dttm = 'NULL'
    cdc_id = 'NULL'
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    # #print("MADHAVA","MADHAVA",result)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename    

    #print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename)
    
    ##print("KODANDAPANI")

    #print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    #print("CREAT STAGE COMPLETED FOR:",sftablename)

    ##print("VARADHA")
    #print("COPY COMMAND STARTED FOR :",sftablename)
    
    query_ingest_only = f"""SELECT * FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE INGESTION_COMPLETED = 'NO' AND ORACLE_TABLE_NAME = '{oracletablename}' AND ORACLE_SCHEMA_NAME = '{oracleschemaname}' AND EXECUTION_MODE = 'EXPORT' AND FINAL_STATUS = 'SUCCESS' ORDER BY BATCH_ID ASC; """

    list_of_files = sfquery(query_ingest_only)
    # print(f"LIST OF FILES FOR INGESTION : {list_of_files}")
    rc_code = 0
    cum_copystmnt = ''
    cum_merstmnt = ''
    cum_result_copystmnt = ''
    cum_result_merstmnt = ''

    if len(list_of_files) == 0:
        cum_copystmnt = 'NO FILES TO COPY'
        cum_result_copystmnt = 'NO FILES TO COPY'
        cum_merstmnt = 'NO FILES TO MERGE'
        cum_result_merstmnt = 'NO FILES TO MERGE' 
        log_update('copycommand',[returncode,cum_copystmnt,cum_result_copystmnt],batch_id,job_id)
        log_update('mergecommand',[returncode,cum_merstmnt,cum_result_merstmnt],batch_id,job_id)

    for file in list_of_files:
        ##print('VARADHARAJA',file)
        uploadfilename = file[26]
        returncode,copystmnt,result_copystmnt=copycommand(stagename,job,uploadfilename)
        rc_sum=rc_sum+returncode
        #print("RC_SUM",rc_sum)
        # #print(returncode,copystmnt,result)

        cum_copystmnt = cum_copystmnt + '\n' + copystmnt
        cum_result_copystmnt = cum_result_copystmnt + '\n' + result_copystmnt

        log_update('copycommand',[returncode,cum_copystmnt,cum_result_copystmnt],batch_id,job_id)
        
        if returncode != 0:
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
            rc_code= rc_code + returncode

        #print("COPY COMMAND COMPLETED FOR :",sftablename)
        ##print("RANGA")
        ##print("SRIMATHA")
        #print("MERGE STATEMENT STARTED FOR :",sftablename)
        returncode,merstmnt,result_merstmnt=mergecommand(job,uploadfilename)
        #print("RC_SUM",rc_sum)
        #print("MERGE STATEMENT COMPLETED FOR :",sftablename)
        rc_sum=rc_sum+returncode

        cum_merstmnt = cum_merstmnt + '\n' + merstmnt
        cum_result_merstmnt = cum_result_merstmnt + '\n' + result_merstmnt 

        log_update('mergecommand',[returncode,cum_merstmnt,cum_result_merstmnt],batch_id,job_id)
        
        if returncode != 0:
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
            rc_code= rc_code + returncode

    if rc_code != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename 

    uploadfilename=uploadfilename.replace('.csv','')
    returncode,archive_put = az_archive(cloud_path,uploadfilename,batch_id)

    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    ##print("Srirama")
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    
    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    #if returncode != 0:
    #    return sftablename
    print(f"Ingestion Completed for {oracletablename}")

    return sftablename




def full_execution(task):
    #time.sleep(1)
    job=task
    rc_sum=0
    job_id=job[16]
    batch_id=job[15]
    oracleschemaname=job[0]
    oracletablename=job[1]
    sfdbname=job[2]
    sfschname=job[3]
    sftablename=job[4]
    sfwarehouse=job[5]
    scdtype=job[6]
    loadtype=job[7]
    cdccolumns=job[8]
    primarykey=job[9]
    delimiter=job[10]
    filtercondition=job[11]
    trim=job[12]
    encryptioncolumns=job[13]
    cloud_path=job[14]
    # customsql=job[16]
    # executionmode=job[17]
    # enabled=job[18]
    #print(f"schema : {oracleschemaname} , table : {oracletablename}")
    
    print(f"Export started for {oracletablename}")
    returncode,returnmessage,exportedfilenames_list,exportedfilename,extract_start_dttm,extract_end_dttm,cdc_id=oracle_export(oracleschemaname,oracletablename,job)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    
    # #print("RC_SUM",rc_sum)
    log_update('oracleexport',[returncode,returnmessage,exportedfilenames_list,exportedfilename],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    
    # #print("MADHAVA")
    uploadfilename=exportedfilename.replace('.csv','')
    #print(f"cloud LOAD STARTED FOR : {cloud_path},{uploadfilename}")
    # #print("GOVINDA")
    
    returncode,cloud_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,cloud_cmd,cloud_log],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    print(f"Cloud Upload completed for {oracletablename}")
    print(f"Export Completed for {oracletablename}")
    print(f"Ingestion started for {oracletablename}")
    #print(f"cloud UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")
    #print("SRINIVASA")
    
    returncode,result=create_table(sfdbname,sfschname,sftablename,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("MADHAVA","MADHAVA",result)
    
    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename 

    #print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)
    
    # #print("KODANDAPANI")
    
    #print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    #print("CREAT STAGE COMPLETED FOR:",sftablename)

    # #print("VARADHA")
    #print("COPY COMMAND STARTED FOR :",sftablename)
    
    returncode,copystmnt,result=copycommand(stagename,job,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print(returncode,copystmnt,result)


    log_update('copycommand',[returncode,copystmnt,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    #print("COPY COMMAND COMPLETED FOR :",sftablename)
    # #print("RANGA")
    
    # #print("SRIMATHA")
    #print("MERGE STATEMENT STARTED FOR :",sftablename)
    returncode,merstmnt,result=mergecommand(job,exportedfilename)
    #print("RC_SUM",rc_sum)
    #print("MERGE STATEMENT COMPLETED FOR :",sftablename)
    rc_sum=rc_sum+returncode
    log_update('mergecommand',[returncode,merstmnt,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    
    returncode,archive_put = az_archive(cloud_path,uploadfilename,batch_id)

    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename   

    returncode_final=rc_sum
    log_update('final_status',[returncode_final],batch_id,job_id)

    # #print("Srirama")
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    eturncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE STARTED FOR :",sftablename)

    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename 
    print(f"Ingestion Completed for {oracletablename}")
    return sftablename
    

    
if __name__ == "__main__":
    # #print("SriRama")
    start=time.time()

    with open('C:/Users/palanivelu.murug/Documents/Datamigration_oracle_final_version - 1215/Datamigration/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    
    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse,
        insecure_mode=True  )
    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema": sf_schema,
    "insecure_mode": True
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('param', nargs='*')
    args = parser.parse_args()
    execution_mode = ""
    count = len(args.param)
    #print(f"COUNT OF PARAMETERS : {count}")
    if count == 0:
        
        execution_mode = "EXECUTION_MODE"
        where_condition = ""
    elif count == 2:
    
        #print("Two parameters:", args.param)
    
        execution_mode = "EXECUTION_MODE"
        where_condition = f" AND ORACLE_SCHEMA_NAME = '{args.param[0]}' AND ORACLE_TABLE_NAME = '{args.param[1]}' "
    
    elif count == 3:
        
        
        execution_mode = f"'{args.param[2]}'"
        #print("Three parameters:", args.param)
        where_condition = f" AND ORACLE_SCHEMA_NAME = '{args.param[0]}' AND ORACLE_TABLE_NAME = '{args.param[1]}' "
    
    else:
        print("Invalid number of parameters")
    
    #print(f"Where condition: {where_condition}")

    #query="""SELECT * FROM DATAMIGRATION.DEMO_USER_ORACLE.CONFIG_TABLE;"""
    query=f"""SELECT ORACLE_SCHEMA_NAME,ORACLE_TABLE_NAME,SF_DATABASE_NAME,SF_SCHEMA_NAME,SF_TABLE_NAME,WAREHOUSE_NAME,SCD_TYPE,LOAD_TYPE,CDC_COLUMNS,PRIMARY_KEY,DELIMITER,FILTER_CONDITION,
            TRIM,ENCRYPTION_COLUMNS,S3_PATH,(SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER_ORACLE.LOG_TABLE)+1,10000)) AS BATCH_ID,JOB_ID,CUSTOM_SQL,{execution_mode},CDC_TYPE FROM DATAMIGRATION.DEMO_USER_ORACLE.CONFIG_TABLE WHERE ENABLED = 'Y' {where_condition};"""
    
    #print(f"MAIN QUERY : {query}")
    spsession=Session.builder.configs(spcon).create()
    batch=spsession.sql(query)
    ##print("Sridhara")
    
     

    ##print(list(test.collect()))
    config=batch.collect()
    configtable=list(config)

    try:
        batch_create(where_condition,execution_mode)
    except Exception as e:
        #print(e)
        #print("NOT ABLE TO CREATE LOG")
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
            ##print(sts.result(),"Return Code")
        ##print("s")
    '''
    #print("MIGRATION STARTS")
    with ProcessPoolExecutor() as executor:
        status_code_oracle_scr_gen = {executor.submit(datamigration, task): task for task in configtable}
        for return_code in as_completed(status_code_oracle_scr_gen):
            print(return_code.result(),"Return Code")
    

    '''
    for i in configtable:
        res=datamigration(i)
        #print(res)

    '''

    #status=tpt_script_generator(configtable)
    ##print("Krishna")
    ##print(status)
    
    '''
    for tptscrptnm in tpt_jobs:
        #print(tptscrptnm)
        #cmd=f"tbuild -f {tptscrptnm} -C"
        cmd = ["tbuild", "-f", tptscrptnm, "-C"]
        #t=subprocess.run(cmd,shell=True,stdout=subprocess.PIPE)
        #print("Damodhara")
        #print(cmd)
        t=subprocess.run(cmd, capture_output=True, text=True)
        
        #print(t.returncode)
        
        #print("SriRanga")
        #print(t.stdout)
        ##print(t.stdout)
    '''
    #EXPORT FILES FROM TERADATA
    
    '''
    with ProcessPoolExecutor() as executor:
        #print("Kanna")
        status_code_tpt={executor.submit(tptexport,tptscptnm_filename[0],tptscptnm_filename[1],tptscptnm_filename[2]): tptscptnm_filename for tptscptnm_filename in tpt_jobs}
    '''
    
    '''
    for tptscrptnm in tpt_jobs:
        #print("Export started for :" ,tptscrptnm)
        tptexport(tptscrptnm)
        #print("Export Completed for :",tptscrptnm)
    '''

    #print("MIGRATION COMPLETED")
    ##print(tpt_jobs)
    end=time.time()
    print(f"Job ran successfully in {end-start} seconds")
