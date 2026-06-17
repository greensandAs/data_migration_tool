import subprocess
import os
import argparse
from datetime import datetime
import time
import json
import pyodbc
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
import threading
from cloud_utils import cloud_upload,az_archive
from threading import Lock
from mslogger import batch_create,log_update
from mssql_utils import msquery,mscount,bcp_export,split_file_into_chunks
from mssql_utils import file_name_generator,getcolumnnames
from mssf_utils import sfquery,create_table,create_stage,copycommand,getcdcdates,mergecommand,auditupdate,sfcount
# from msaws_utils import s3upload
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
    mstablename=job[2]
    execution_mode=job[19]
    print(f"Migration started for {mstablename} with execution mode as {execution_mode}")
    if execution_mode == 'FULL':
        return full_execution(task)
    
    elif execution_mode == 'EXPORT':
        return export_only(task)
    
    elif execution_mode == 'INGEST':
        return ingest_only(task)
    
    
    
def export_only(task):

    job=task

    rc_sum=0
    msdbname=job[0]
    msschemaname=job[1]
    mstablename=job[2]
    sfdbname=job[3]
    sfschname=job[4]
    sftablename=job[5]
    loadtype=job[8]
    cloud_path=job[15]

    batch_id = job[16]
    job_id=job[17]
    custom_sql=job[18]


    print(f"Export started for {mstablename}")
    returncode,errormsg,condition,filename,bcpfilename,export_start_time,extract_start_dttm,extract_end_dttm,cdc_id,bcp_export_query,bcpsplitpath=file_name_generator(job)

    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('file_name_generator',[returncode,errormsg,filename,bcpfilename],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)

        return sftablename    
    #print("generate_file_name done")

    #print(returncode,errormsg)
									
										
    
    returncode,bcp_cmd,stdout=bcp_export(msdbname,msschemaname,mstablename,bcpfilename,bcp_export_query)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
							
    log_update('bcp_export',[returncode,bcp_cmd,stdout,filename,bcpfilename],batch_id,job_id)


    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)

        return sftablename
    #print("bcp_export done")
    
    # returncode,mscnt=mscount(msdbname,msschemaname,mstablename)
    # rc_sum=rc_sum+returncode
    # #print("RC_SUM",rc_sum,mscnt)
    # # log_update('mscount',[returncode,mscnt],batch_id,job_id)
    
    # if returncode != 0:
    #     returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    #     return sftablename



    
    returncode,errormsg,chunk_file_name,chunk_file_path = split_file_into_chunks(bcpfilename, bcpsplitpath,msdbname,mstablename) 

    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    
    if returncode != 0:
        return sftablename    
    #print("split file into chunks is done")

    

    uploadfilename=filename.replace('.csv','')
    #print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")
    
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    print(f"Cloud Upload completed for {mstablename}")
    #print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")
    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    print(f"Export completed for {mstablename}")
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    
def ingest_only(task):
    job=task
    rc_sum=0
    msdbname=job[0]
    msschemaname=job[1]
    mstablename=job[2]
    cloud_path=job[15]
    sfdbname=job[3]
    sfschname=job[4]
    sftablename=job[5]
    loadtype=job[8]
    batch_id=job[16]
    job_id=job[17]
    custom_sql=job[18]
    print(f"Ingestion Started for {mstablename}")
    log_update('start_time_update',[0],batch_id,job_id)

    
    extract_start_dttm = 'NULL'
    extract_end_dttm = 'NULL'
    cdc_id = 'NULL'


    
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    # print("checkpoint 1")
    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename    
    # print("checkpoint 2")
    #print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename)
    
    
    #print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    # print("checkpoint 3")
    #print("CREAT STAGE COMPLETED FOR:",sftablename)

    
    query_ingest_only = f"""SELECT * FROM AUDIT_TABLE WHERE INGESTION_COMPLETED = 'NO' AND MSSQL_TABLE_NAME = '{mstablename}' AND MSSQL_DATABASE_NAME = '{msdbname}' AND EXECUTION_MODE = 'EXPORT' AND FINAL_STATUS = 'SUCCESS' ORDER BY BATCH_ID ASC; """
    # print("audit query : ",query_ingest_only)
    list_of_files = sfquery(query_ingest_only)
    # print("list_of_files : ",list_of_files)
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
    # print("cdc_id before :",cdc_id)
    for file in list_of_files:
        # print(file)
        uploadfilename = file[28]
        extract_start_dttm = file[23]
        extract_end_dttm = file[24]
        cdc_id = file[25]
        # print("cdc_id :",cdc_id)
        # print("COPY COMMAND STARTED FOR :",sftablename)
        
        returncode,copystmnt,result_copystmnt=copycommand(stagename,job,uploadfilename)
        rc_sum=rc_sum+returncode
        #print("RC_SUM",rc_sum)
        # print(returncode,copystmnt,result)

        cum_copystmnt = cum_copystmnt + '\n' + copystmnt
        cum_result_copystmnt = cum_result_copystmnt + '\n' + result_copystmnt

        log_update('copycommand',[returncode,cum_copystmnt,cum_result_copystmnt],batch_id,job_id)
        
        if returncode != 0:
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
            rc_code= rc_code + returncode

        #print("COPY COMMAND COMPLETED FOR :",sftablename)

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
        
        uploadfilename=uploadfilename.replace('.csv','')
        returncode,archive_put = az_archive(cloud_path,uploadfilename,batch_id)

        # log_update('mergecommand',[returncode,cum_merstmnt,cum_result_merstmnt],batch_id,job_id)

        # if returncode != 0:
        #     returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        #     rc_code= rc_code + returncode
    # print("cdc_id out :",cdc_id)
    if rc_code != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename 


    returncode,sfcnt=sfcount(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('sfcount',[returncode,sfcnt],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    returncode_final=rc_sum
    
    log_update('final_status',[returncode_final],batch_id,job_id)

    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE COMPLETED FOR :",sftablename)
    
    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)

    #if returncode != 0:
    #    return sftablename
    print(f"Ingestion Completed for {mstablename}")

    return sftablename




def full_execution(task):
    #time.sleep(1)
    job=task
    rc_sum=0
    msdbname=job[0]
    msschemaname=job[1]
    mstablename=job[2]
    cloud_path=job[15]
    sfdbname=job[3]
    sfschname=job[4]
    sftablename=job[5]
    loadtype=job[8]
    batch_id=job[16]
    job_id=job[17]
    custom_sql=job[18]
    
    print(f"Export started for {mstablename}")
    returncode,errormsg,condition,filename,bcpfilename,export_start_time,extract_start_dttm,extract_end_dttm,cdc_id,bcp_export_query,bcpsplitpath=file_name_generator(job)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('file_name_generator',[returncode,errormsg,filename,bcpfilename],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename    
    

    #print(returncode,errormsg)
    #print("generate_file_name done")
    #print("inside full execuiton block")
    
    #print("bcp export started")
    file_name = "time_log.txt"
    returncode,bcp_cmd,stdout=bcp_export(msdbname,msschemaname,mstablename,bcpfilename,bcp_export_query)
    if returncode == 4:
        returncode = 0
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("bcp export done")
    log_update('bcp_export',[returncode,bcp_cmd,stdout,filename,bcpfilename],batch_id,job_id)


    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)

        return sftablename

    
    # returncode,mscnt=mscount(msdbname,msschemaname,mstablename)
    # rc_sum=rc_sum+returncode
    # #print("RC_SUM",rc_sum,mscnt)
    # log_update('mscount',[returncode,mscnt],batch_id,job_id)
    
    # if returncode != 0:
    #     returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    #     return sftablename
    
    returncode,errormsg,chunk_file_name,chunk_file_path = split_file_into_chunks(bcpfilename, bcpsplitpath,msdbname,mstablename) 


    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    
    if returncode != 0:
        return sftablename    
    #print("split file into chunks is done")


    uploadfilename=filename.replace('.csv','')
    #print(f"CLOUD UPLOAD STARTED FOR : {cloud_path},{uploadfilename}")
    current_time = datetime.now()
    with open(file_name, "a") as file:
        file.write(f"{mstablename} Export Ends and Upload Start: {current_time}\n")

    # #print("upload",time.time())
    returncode,upload_cmd,cloud_log=cloud_upload(cloud_path,uploadfilename)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('s3upload',[returncode,upload_cmd,cloud_log],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename
    # #print("upload",time.time())
    #print(f"CLOUD UPLOAD COMPLETED FOR :{cloud_path},{uploadfilename}")

    
    print(f"Cloud Upload completed for {mstablename}")
    print(f"Export Completed for {mstablename}")
    print(f"Ingestion started for {mstablename}")

    current_time = datetime.now()
    with open(file_name, "a") as file:
        file.write(f"{mstablename} Upload Ends and Ingestion Start: {current_time}\n")
    returncode,result=create_table(sfdbname,sfschname,sftablename,loadtype)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)

    log_update('create_table',[returncode,result],batch_id,job_id)
    
    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename    

    #print("TABLE CREATION COMPLETED",sfdbname,sfschname,sftablename,uploadfilename)


    #print("CREAT STAGE STARTED FOR:",sftablename)

    returncode,log,stagename=create_stage(sfdbname,sfschname,cloud_path)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    log_update('create_stage',[returncode,log,stagename],batch_id,job_id)

    if returncode != 0:
        returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
        return sftablename

    #print("CREAT STAGE COMPLETED FOR:",sftablename)

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
    
    #print("MERGE STATEMENT STARTED FOR :",sftablename)
    returncode,merstmnt,result=mergecommand(job,uploadfilename)
    #print("RC_SUM",rc_sum)
    #print("returncode", returncode)
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
    #print("AUDIT UPDATE STARTED FOR :",sftablename)
    returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id)
    rc_sum=rc_sum+returncode
    #print("RC_SUM",rc_sum)
    #print("AUDIT UPDATE COMPLETED FOR :",sftablename)

    log_update('auditupdate',[returncode,auditstmnt,result],batch_id,job_id)
    
    print(f"Ingestion Completed for {mstablename}")
    # if returncode != 0:
    #     return sftablename

    return sftablename


if __name__ == "__main__":
    #print("Run started")
    current_time = datetime.now()
    file_name = "time_log.txt"
    with open(file_name, "a") as file:
        file.write(f"Job Start: {current_time}\n")
    start=time.time()


    with open('C:/Users/palanivelu.murug/Documents/Datamigration_gzip_while_export_2026_04_03/credentials.json','r+') as config_file:
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
        insecure_mode=True )
    
    parser = argparse.ArgumentParser()
    parser.add_argument('param', nargs='*')
    args = parser.parse_args()
    execution_mode = ""
    count = len(args.param)
    if count == 0:
        
        execution_mode = "EXECUTION_MODE"
        where_condition = ""
    elif count == 3:
    
        #print("Two parameters:", args.param)
    
        execution_mode = "EXECUTION_MODE"
        where_condition = f" AND MSSQL_DATABASE_NAME = '{args.param[0]}' AND MSSQL_SCHEMA_NAME =  '{args.param[1]}' AND MSSQL_TABLE_NAME = '{args.param[2]}' "
    
    elif count == 4:

        execution_mode = f"'{args.param[3]}'"
        #print("Three parameters:", args.param)
        where_condition = f" AND MSSQL_DATABASE_NAME = '{args.param[0]}' AND MSSQL_SCHEMA_NAME =  '{args.param[1]}' AND MSSQL_TABLE_NAME = '{args.param[2]}' "
    
    else:
        print("Invalid number of parameters")
    
    #print(where_condition)
    
    
    query=f"""SELECT MSSQL_DATABASE_NAME , MSSQL_SCHEMA_NAME, MSSQL_TABLE_NAME , SF_DATABASE_NAME , SF_SCHEMA_NAME , SF_TABLE_NAME ,
		WAREHOUSE_NAME , SCD_TYPE , LOAD_TYPE , CDC_COLUMNS , PRIMARY_KEY , DELIMITER , FILTER_CONDITION , TRIM , ENCRYPTION_COLUMNS , 
		S3_PATH ,(SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000)) as BATCH_ID,  JOB_ID, CUSTOM_SQL,{execution_mode},CDC_TYPE
    FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE  WHERE ENABLED = 'Y' {where_condition};"""
   
    cur = sfcon.cursor()
    cur.execute(query)
		
		# Fetch and #print the result
    result = cur.fetchall()
    #print(f"configtable before list {result}")
    configtable = [list(row) for row in result]
    #print(f"configtable after list  is {configtable}")
    #print(type(configtable))
    
    try:
        batch_create(where_condition,execution_mode)
    except Exception as e:
        #print(e)
        #print("NOT ABLE TO CREATE LOG")
        exit()


		
    sfcon.close()

    with ProcessPoolExecutor() as executor:
      futures = {executor.submit(datamigration, task): task for task in configtable }
      for future in as_completed(futures):
        print(future.result())



    print("Extraction completed")
    end=time.time()
    current_time = datetime.now()
    file_name = "time_log.txt"
    with open(file_name, "a") as file:
        file.write(f"Job End: {current_time}\n")
    # print(f'total time taken {end-start}')
    print(f"Job ran successfully in {end-start} seconds")


