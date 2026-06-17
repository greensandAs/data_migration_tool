from mssf_utils import sfquery
from datetime import datetime, timedelta
 
def batch_create(where_condition,execution_mode):

    query=f"""INSERT INTO DATAMIGRATION.DEMO_USER.LOG_TABLE (BATCH_ID,JOB_ID, MSSQL_DATABASE_NAME, MSSQL_SCHEMA_NAME, MSSQL_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE, CUSTOM_SQL, S3_PATH, EXECUTION_MODE)
            SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE)+1,10000),JOB_ID,MSSQL_DATABASE_NAME, MSSQL_SCHEMA_NAME, MSSQL_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE , CUSTOM_SQL, S3_PATH,{execution_mode}  FROM DATAMIGRATION.DEMO_USER.CONFIG_TABLE  WHERE ENABLED = 'Y' {where_condition};"""
    sfquery(query)
    query1 =f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE
	        SET CREATE_TABLE_LOG = 'SKIPPED', CREATE_TABLE_STATUS = 'SKIPPED', CREATE_STAGE_NAME = 'SKIPPED', CREATE_STAGE_LOG = 'SKIPPED', CREATE_STAGE_STATUS = 'SKIPPED', COPY_COMMAND = 'SKIPPED', COPY_COMMAND_LOG = 'SKIPPED', COPY_COMMAND_STATUS = 'SKIPPED', MERGE_STATEMENT = 'SKIPPED', MERGE_STATEMENT_LOG = 'SKIPPED', MERGE_STATEMENT_STATUS = 'SKIPPED', AUDIT_STATEMENT = 'SKIPPED', AUDIT_STATEMENT_LOG = 'SKIPPED', AUDIT_STATEMENT_STATUS = 'SKIPPED' , INGESTION_COMPLETED = 'NO' WHERE BATCH_ID = (SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE),10000)) AND EXECUTION_MODE = 'EXPORT' {where_condition} ;"""

    sfquery(query1) 

    query2 = f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE
            SET	BCP_SCRIPT_NAME = 'SKIPPED', BCP_EXPORT_CMD = 'SKIPPED', BCP_EXPORT_LOG = 'SKIPPED', BCP_EXPORT_STATUS = 'SKIPPED', EXPORT_FILENAME = 'SKIPPED', S3_UPLOAD_CMD = 'SKIPPED', S3_UPLOAD_LOG = 'SKIPPED', S3_UPLOAD_STATUS = 'SKIPPED'  WHERE BATCH_ID = (SELECT COALESCE((SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.LOG_TABLE),10000)) AND EXECUTION_MODE = 'INGEST' {where_condition};"""
    sfquery(query2)

def log_update(step,stepvalues,batch_id,job_id):
    
    if step == 'file_name_generator':
        #print(step,stepvalues,batch_id,job_id)
        content=""
        log=""
        status=""
        if stepvalues[0]==0:
            content=stepvalues[2]
            log="BCP SCRIPT NAME GENERATED SUCCESSFULLY"
            status="SUCCESS"

 
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET BCP_SCRIPT_NAME = '{content}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}
                  """
        else:
            content=stepvalues[2]
            log=stepvalues[1]
            status="FAILED"
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET BCP_SCRIPT_NAME = '{stepvalues[2]}',
                        BCP_EXPORT_STATUS = '{p_status}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        sfquery(updquery)
        # return [t.returncode,bcp_cmd,t.stdout]
    
    if step == 'bcp_export':
        if stepvalues[0]==0:
            status="SUCCESS"
            cmd=stepvalues[1].replace("'","''")
            exportfilename=stepvalues[3].replace("'","''")
            
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET BCP_EXPORT_CMD = '{cmd}' , BCP_EXPORT_LOG = '{stepvalues[2]}', BCP_EXPORT_STATUS = '{status}', EXPORT_FILENAME = '{exportfilename}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            # print(updquery)
        
        else:
            status="FAILED"

            log=stepvalues[2].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            exportfilename=stepvalues[3].replace("'","''")
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET BCP_EXPORT_CMD = '{stepvalues[1]}' ,BCP_EXPORT_LOG = '{log}', BCP_EXPORT_STATUS = '{status}',
                    EXPORT_FILENAME = '{exportfilename}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        


        # print(updquery)
        sfquery(updquery)
        #print(stepvalues)
    

    if step == 's3upload':
        if stepvalues[0]==0:
            status="SUCCESS"
            cmd=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET S3_UPLOAD_CMD = '{cmd}' ,S3_UPLOAD_LOG = '{log}', S3_UPLOAD_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status="FAILED"
            cmd=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET S3_UPLOAD_CMD = '{cmd}' ,S3_UPLOAD_LOG = '{log}', S3_UPLOAD_STATUS = '{status}',
                    CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        
        sfquery(updquery)
        
    if step == 'start_time_update':
        if stepvalues[0]==0:

            job_start_time=str(datetime.now() - timedelta(hours=5))
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET JOB_START_TIME = '{job_start_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        sfquery(updquery)

    
    if step == 'create_table':
        if stepvalues[0]==0:
            status="SUCCESS"
            log="WORK TABLE CREATED SUCCESSFULLY"

        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_TABLE_LOG = '{log}' ,CREATE_TABLE_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            log=stepvalues[1].replace("'","''")
            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_TABLE_LOG = '{log}' ,CREATE_TABLE_STATUS = '{status}',
                    COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        

        sfquery(updquery)
    
    
    
    if step == 'create_stage':
        if stepvalues[0]==0:
            status="SUCCESS"
            stagename=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_STAGE_NAME = '{stagename}', CREATE_STAGE_LOG = '{log}' ,CREATE_STAGE_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            stagename=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET CREATE_STAGE_NAME = '{stagename}', CREATE_STAGE_LOG = '{log}' ,CREATE_STAGE_STATUS = '{status}',
                    COPY_COMMAND_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        sfquery(updquery)


    if step == 'copycommand':
        if stepvalues[0]==0:
            status="SUCCESS"
            copystmnt=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET COPY_COMMAND = '{copystmnt}', COPY_COMMAND_LOG = '{log}' ,COPY_COMMAND_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            copystmnt=stepvalues[1].replace("'","''")
            log=stepvalues[2].replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET COPY_COMMAND = '{copystmnt}', COPY_COMMAND_LOG = '{log}' ,COPY_COMMAND_STATUS = '{status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        sfquery(updquery)


    
    if step == 'mergecommand':
        if stepvalues[0]==0:
            status="SUCCESS"
            merstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MERGE_STATEMENT = '{merstmnt}', MERGE_STATEMENT_LOG = '{log}' ,MERGE_STATEMENT_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            merstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MERGE_STATEMENT = '{merstmnt}', MERGE_STATEMENT_LOG = '{log}' ,MERGE_STATEMENT_STATUS = '{status}',
                    AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""



        sfquery(updquery)
    
    if step == 'auditupdate':
        if stepvalues[0]==0:
            status="SUCCESS"
            auditstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET AUDIT_STATEMENT = '{auditstmnt}', AUDIT_STATEMENT_LOG = '{log}' ,AUDIT_STATEMENT_STATUS = '{status}'
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        
        else:
            status='FAILED'
            auditstmnt=str(stepvalues[1]).replace("'","''")
            log=str(stepvalues[2]).replace("'","''")

            p_status='FAILED IN PREVIOUS STEP'
            job_end_time=str(datetime.now() - timedelta(hours=5))
            
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET AUDIT_STATEMENT = '{auditstmnt}', AUDIT_STATEMENT_LOG = '{log}' ,AUDIT_STATEMENT_STATUS = '{status}',
                    JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        
        sfquery(updquery)
    
    if step == 'mscount':
        if stepvalues[0]==0:
            status="SUCCESS"
            mscnt=str(stepvalues[1]).replace("'","''")
            job_start_time=str(datetime.now() - timedelta(hours=5))

        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MSSQL_TABLE_COUNT = '{mscnt}', JOB_START_TIME = '{job_start_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
            
        else:
            f_status='FAILED'
            p_status='FAILED IN PREVIOUS STEP'
            mscnt=str(stepvalues[1]).replace("'","''")
            job_start_time=str(datetime.now() - timedelta(hours=5))
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET MSSQL_TABLE_COUNT = '{mscnt}', JOB_START_TIME = '{job_start_time}' ,
                        BCP_EXPORT_STATUS = '{p_status}',
                        S3_UPLOAD_STATUS = '{p_status}',
                        CREATE_TABLE_STATUS = '{p_status}',
                        COPY_COMMAND_STATUS = '{p_status}',
                        CREATE_STAGE_STATUS = '{p_status}',
                        MERGE_STATEMENT_STATUS = '{p_status}',
                        AUDIT_STATEMENT_STATUS = '{p_status}',
                        JOB_END_TIME = '{job_end_time}',
                        JOB_DURATION = TIMESTAMPDIFF( SECOND , '{job_start_time}' , '{job_end_time}' ),
                        FINAL_STATUS = '{f_status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""


        sfquery(updquery)

    if step == 'sfcount':
        if stepvalues[0]==0:
            status="SUCCESS"
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))
        
        
            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET  INGESTION_COMPLETED = 'YES', SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        

        else:
            status='FAILED'
            sfcnt=str(stepvalues[1]).replace("'","''")
            job_end_time=str(datetime.now() - timedelta(hours=5))

            updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET  INGESTION_COMPLETED = 'FAILED', SF_TABLE_COUNT = '{sfcnt}', JOB_END_TIME = '{job_end_time}' , JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}' ),
                        FINAL_STATUS = '{status}' 
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""

        sfquery(updquery)


    if step == 'final_status':
        #print(stepvalues[0])
        job_end_time=str(datetime.now() - timedelta(hours=5))
        if stepvalues[0]==0:
            final_status='SUCCESS'
        else:
            final_status='FAILED'

        updquery=f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE 
                    SET FINAL_STATUS = '{final_status}' , JOB_END_TIME = '{job_end_time}', JOB_DURATION = TIMESTAMPDIFF( SECOND , JOB_START_TIME, '{job_end_time}'  )
                    WHERE BATCH_ID={batch_id} AND JOB_ID={job_id}"""
        sfquery(updquery)