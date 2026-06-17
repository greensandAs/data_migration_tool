from sf_utils_ing import sfquery
from datetime import datetime, timedelta
import json

def batch_create(where_condition,path_ext):
        with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
            cred=json.load(config_file)

        sf_config_table = cred['sf_config_table']
        sf_log_table = cred['sf_log_table']


        query=f"""INSERT INTO {sf_log_table} (BATCH_ID, JOB_ID, FILE_PATTERN,CLOUD_PATH, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, FINAL_STATUS)
        SELECT CAST(COALESCE((SELECT MAX(BATCH_ID) FROM {sf_log_table})+1,10000) AS INT),JOB_ID, FILE_PATTERN, CONCAT(CLOUD_PATH,{path_ext}),SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, 'NOT RUNNING' FROM {sf_config_table}  WHERE{where_condition}; """
        
        sfquery(query)

def log_update(step,stepvalues,batch_id,job_id):
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_config_table = cred['sf_config_table']
    sf_log_table = cred['sf_log_table']

    if step == 'src_cnt':
        if stepvalues[0]==0:
            f_status="SUCCESS"
            src_count=str(stepvalues[1]).replace("'","''")
            src_info=str(stepvalues[2])
            job_start_time=str(datetime.now() - timedelta(hours=5))


            updquery=f"""UPDATE {sf_log_table} 
                    SET SOURCE_COUNT = '{src_count}', SOURCE_INFO = '{src_info}' ,JOB_START_TIME = '{job_start_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            
        else:
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            src_count=str(stepvalues[1]).replace("'","''")
            src_info=str(stepvalues[2])
            job_start_time=str(datetime.now() - timedelta(hours=5))
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET SOURCE_COUNT = '{src_count}', SOURCE_INFO = '{src_info}' ,JOB_START_TIME = '{job_start_time}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , '{job_start_time}' , '{job_end_time}' ),
                        FINAL_STATUS = '{f_status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        

        sfquery(updquery)

    if step == 'create_file_format':
        if stepvalues[0]==0:
            f_status="SUCCESS"
            file_format_obj_stmt = stepvalues[1].replace("'","''")
            file_format_obj_log = stepvalues[2].replace("'","''")
            file_format_obj_status = stepvalues[3].replace("'","''")
        
            updquery=f"""UPDATE {sf_log_table} 
                    SET FILE_FORMAT_OBJECT_STATEMENT = '{file_format_obj_stmt}' ,FILE_FORMAT_OBJECT_LOG = '{file_format_obj_log}' ,FILE_FORMAT_OBJECT_STATUS = '{file_format_obj_status}', FINAL_STATUS = 'RUNNING'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            file_format_obj_stmt = stepvalues[1].replace("'","''")
            file_format_obj_log = stepvalues[2].replace("'","''")
            file_format_obj_status = stepvalues[3].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET FILE_FORMAT_OBJECT_STATEMENT = '{file_format_obj_stmt}' ,FILE_FORMAT_OBJECT_LOG = '{file_format_obj_log}' ,FILE_FORMAT_OBJECT_STATUS = '{file_format_obj_status}' , FINAL_STATUS = '{f_status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        


        sfquery(updquery)

    if step == 'create_target_table':
        if stepvalues[0]==0:
            f_status="SUCCESS"
            create_table_log = stepvalues[1].replace("'","''")
        
            updquery=f"""UPDATE {sf_log_table} 
                    SET CREATE_TABLE_LOG = '{create_table_log}' , CREATE_TABLE_STATUS = 'SUCCESS'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            create_table_log = stepvalues[1].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET CREATE_TABLE_LOG = '{create_table_log}' , CREATE_TABLE_STATUS = 'FAILED' , FINAL_STATUS = '{f_status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        


        sfquery(updquery)

    if step == 'ingestion':
        if stepvalues[0]==0:
            job_end_time=str(datetime.now() - timedelta(hours=5))
            f_status="SUCCESS"
            ingestion_stmt = stepvalues[1].replace("'","''")
            ingestion_log = "NUMBER OF ROWS INSERTED : " + str(stepvalues[2])
            tar_count = str(stepvalues[2])
            updquery=f"""UPDATE {sf_log_table} 
                    SET INGESTION_STATEMENT = '{ingestion_stmt}' ,INGESTION_LOG = '{ingestion_log}' , INGESTION_STATUS = '{f_status}' , TARGET_COUNT = '{tar_count}' , JOB_END_TIME = '{job_end_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            
        else:
            job_end_time=str(datetime.now() - timedelta(hours=5))
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            ingestion_stmt = stepvalues[1].replace("'","''")
            ingestion_log = stepvalues[2].replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET INGESTION_STATEMENT = '{ingestion_stmt}' ,INGESTION_LOG = '{ingestion_log}'  ,INGESTION_STATUS = '{f_status}' , JOB_END_TIME = '{job_end_time}' , FINAL_STATUS = '{f_status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        


        sfquery(updquery)
    
    if step == 'copy_ingestion':
        if stepvalues[0]==0:
            job_end_time=str(datetime.now() - timedelta(hours=5))
            f_status="SUCCESS"
            ingestion_stmt = str(stepvalues[1]).replace("'","''")
            ingestion_log = str(stepvalues[2]).replace("'","''")
            tar_count = str(stepvalues[3])
            updquery=f"""UPDATE {sf_log_table} 
                    SET INGESTION_STATEMENT = '{ingestion_stmt}' ,INGESTION_LOG = '{ingestion_log}' , INGESTION_STATUS = '{f_status}' , TARGET_COUNT = '{tar_count}' , JOB_END_TIME = '{job_end_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            
        else:
            job_end_time=str(datetime.now() - timedelta(hours=5))
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            ingestion_stmt = str(stepvalues[1]).replace("'","''")
            ingestion_log = str(stepvalues[2]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET INGESTION_STATEMENT = '{ingestion_stmt}' ,INGESTION_LOG = '{ingestion_log}'  ,INGESTION_STATUS = '{f_status}' , JOB_END_TIME = '{job_end_time}' , FINAL_STATUS = '{f_status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        


        sfquery(updquery)

    if step == 'sfcount':
        if stepvalues[0]==0:
            status="SUCCESS"
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))
        
        
            updquery=f"""UPDATE {sf_log_table} 
                    SET SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        

        else:
            status='FAILED'
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE {sf_log_table} 
                    SET SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' , JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

 
        sfquery(updquery)


    if step == 'final_status':
        print(stepvalues[0])
        if stepvalues[0]==0:
            final_status='SUCCESS'
        else:
            final_status='FAILED'

        updquery=f"""UPDATE {sf_log_table} 
                    SET FINAL_STATUS = '{final_status}' , JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, JOB_END_TIME )
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        sfquery(updquery)