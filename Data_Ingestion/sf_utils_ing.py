import snowflake.connector
import json
#from td_utils import tdquery
from datetime import datetime, timedelta
import snowflake.snowpark as snowpark
from snowflake.snowpark import Session

def sfquery(query, job_warehouse='default'):
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    if job_warehouse=='default':
        sf_warehouse = cred['sf_warehouse']
    else:
        sf_warehouse = job_warehouse
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    

    sfcon = snowflake.connector.connect(
        account=sf_host ,
        user=sf_user, 
        password=sf_password,
        database=sf_database,
        schema=sf_schema,
        warehouse=sf_warehouse)
    
    query=query
     
    print(query)

    with sfcon.cursor() as curr:
        curr.execute(query)
        result=curr.fetchall()
    
    print(result,type(result))
    return result



def src_cnt(file_pattern, cloud_path, file_format_obj_name):

    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_aws_storage_int = cred['sf_aws_storage_integration']
    sf_aws_stage = cred['sf_aws_stage']
    sf_azure_storage_int = cred['sf_blob_storage_integration']
    sf_azure_stage = cred['sf_blob_stage']
    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = rf'{sf_aws_storage_int}'
        stage_object = rf'{sf_aws_stage}'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        storage_integration = rf'{sf_azure_storage_int}'
        stage_object = rf'{sf_azure_stage}'

    query = f"DESC STORAGE INTEGRATION {storage_integration};"
    #query = f"""SELECT COUNT(*) FROM @{cloud_code}_{file_pattern}"""
    try:
        result=sfquery(query)
        
        root_path = result[2][2]
        act_path = cloud_path.replace(root_path,'')
        print(act_path,"Actual Path")
        returncode=0
    except Exception as e:
        returncode=1
        result=str(e)
        src_count=str(e)
        src_info=str(e) 

        return [returncode,src_count,src_info,'NA']

    try:
        query1 = f"""SELECT 
                METADATA$FILENAME AS FILE_NAME,
                COUNT(*) AS ROW_COUNT
                FROM @{stage_object}/{act_path} (PATTERN => '{file_pattern}' , FILE_FORMAT => {file_format_obj_name})
                GROUP BY METADATA$FILENAME
                ORDER BY FILE_NAME; """
        print(query1)
        result=sfquery(query1)
        
        src_count = sum(item[1] for item in result)
        print(src_count) 
        src_info = "FILE_PATH : ROW_COUNT\n \n"

        for i in result:
            src_info = src_info + f"{root_path}{i[0]} : {i[1]}\n"
        
        print(src_info)
        returncode=0
    except Exception as e:
        returncode=1
        src_count=str(e)
        src_info=str(e) 


    return [returncode,src_count,src_info,act_path]

#ds=src_cnt('.*SERVICE.*.csv','s3://tdsfbucket/TDEXPORT/PARQUET_FOLDER/')

#azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_INVENTORY_TPT_20250309_1501/
#azure://snowflaketeradata213.blob.core.windows.net/teradataexport/
#ds=src_cnt('.*part.*..parquet','s3://tdsfbucket/TDEXPORT/PARQUET_FOLDER/')


#returncode,src_count,src_info=src_cnt('.*part.*..parquet','azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/DEMO_USER_INVENTORY_TPT_20250309_1501/')

#print(returncode,src_count,src_info)

#sfquery("SELECT * FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE")


def create_file_format(job):
    create_stmt = ""
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
    file_format_obj_name = f"{file_type}_{batch_id}_{job_id}"
    if file_type == 'CSV':
        escape_character = escape_character.replace("\\", "\\\\")
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '{field_optionally_enclosed_by}', FIELD_DELIMITER = '{field_delimiter}', ESCAPE = '{escape_character}', SKIP_HEADER = {skip_header} "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"
    
    elif file_type == 'PARQUET':
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'PARQUET' "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"
    
    elif file_type == 'JSON':
        create_stmt = create_stmt + f"CREATE OR REPLACE FILE FORMAT {sf_database_name}.{sf_schema_name}.{file_type}_{batch_id}_{job_id} TYPE = 'JSON' "
        if additional_file_format_options != None:
            create_stmt = create_stmt + f", {additional_file_format_options}"
        create_stmt = create_stmt + ";"

    else:
        create_stmt = "NA"

    print(create_stmt)

    try:
        result=str(sfquery(create_stmt))
        returncode=0
        status='SUCCESS'
    except Exception as e:
        returncode=1
        result=str(e)
        status='FAILED'
    
    print(returncode,create_stmt,result,status) 
    return [returncode,file_format_obj_name,create_stmt,result,status]


def ingestion(job,act_path,file_format_obj_name):


    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9].lower()

    print(act_path,file_format_obj_name)

    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_aws_storage_int = cred['sf_aws_storage_integration']
    sf_aws_stage = cred['sf_aws_stage']
    sf_azure_storage_int = cred['sf_blob_storage_integration']
    sf_azure_stage = cred['sf_blob_stage']
    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = rf'{sf_aws_storage_int}'
        stage_object = rf'{sf_aws_stage}'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        storage_integration = rf'{sf_azure_storage_int}'
        stage_object = rf'{sf_azure_stage}'

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = warehouse_name
    sf_database = sf_database_name
    sf_schema = sf_schema_name

    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema":sf_schema
    }

    print(spcon)

    spsession=Session.builder.configs(spcon).create()
    session_id = spsession.session_id
    opt={"pattern" : f"{file_pattern}",
     "inferSchema" :"True",
     "format_name" : f"{sf_database}.{sf_schema}.{file_format_obj_name}"}

    print(opt)

    
    #df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")
    try:
        
        if file_type == 'csv':
            df_snowpark = spsession.read.options(opt).csv(f"@{stage_object}/{act_path}")
        
        elif file_type == 'parquet':
            df_snowpark = spsession.read.options(opt).parquet(f"@{stage_object}/{act_path}")
            print(df_snowpark)
        elif file_type == 'json':
            df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")

        #df_snowpark = spsession.read.options(opt).options(opt).load(f"@{stage_object}/{act_path}")

        df_snowpark.write.mode(f"{load_mode}").save_as_table(f"{sf_database}.{sf_schema}.{sf_table_name}")
        
        file_format_drop_stmt = f'DROP FILE FORMAT IF EXISTS {sf_database_name}.{sf_schema_name}.{file_format_obj_name};'
        try:
            result=str(sfquery(file_format_drop_stmt))
        except Exception as e:
            returncode=1
        
        his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                            from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                            order by start_time DESC;""")
        history=his.collect()
        print('Query result',history[2][0] ,history[2][-3])
        ingestion_query=history[2][0]
        tar_cnt = history[2][-3]
        returncode = 0
        
        


        
    except Exception as e:

        his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                            from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
                            order by start_time DESC;""")
        history=his.collect()
        print('Query result',history[2][0] ,history[2][-3])
        ingestion_query=history[2][0]
        tar_cnt = history[2][-3]
        #ingestion_query=ingestion_query
        tar_cnt=str(e)
        returncode = 1


    spsession.close()
    
    return [returncode,ingestion_query,tar_cnt]

def copy_ingestion(job,act_path,file_format_obj_name):


    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    target_table = f"{sf_database_name}.{sf_schema_name}.{sf_table_name}"
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9]
    additional_copy_options = job[15]

    print(act_path,file_format_obj_name)
     
    with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_aws_storage_int = cred['sf_aws_storage_integration']
    sf_aws_stage = cred['sf_aws_stage']
    sf_azure_storage_int = cred['sf_blob_storage_integration']
    sf_azure_stage = cred['sf_blob_stage']
    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = rf'{sf_aws_storage_int}'
        stage_object = rf'{sf_aws_stage}'
        
    elif r'azure://' in cloud_path:
        cloud_code='AZ'
        storage_integration = rf'{sf_azure_storage_int}'
        stage_object = rf'{sf_azure_stage}'    


    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = warehouse_name
    sf_database = sf_database_name
    sf_schema = sf_schema_name

    spcon = {
    "account": sf_host,
    "user": sf_user,
    "password": sf_password,
    "warehouse": sf_warehouse,
    "database": sf_database,
    "schema":sf_schema
    }

    print(spcon)
    
    #spsession=Session.builder.configs(spcon).create()
    #session_id = spsession.session_id
    if file_type == 'CSV':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}' """
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'PARQUET':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}'"""
        additional_copy_options = additional_copy_options + " MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE"
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)
    
    elif file_type == 'JSON':
        copystmnt=fr"""COPY INTO {target_table} FROM @{stage_object}/{act_path} FILE_FORMAT = {sf_database_name}.{sf_schema_name}.{file_format_obj_name} PATTERN = '{file_pattern}'"""
        if additional_copy_options != None:
            copystmnt = copystmnt + f" {additional_copy_options}"
        copystmnt = copystmnt + ";"
        print(copystmnt)

    else:
        copystmnt = "NA"
        print(copystmnt)
    #spsession.close()
    try:
        result=sfquery(copystmnt,warehouse_name)
        print(result)
        returncode = 0
        ingestion_cnt = 0
        print(result,returncode,copystmnt)
        try:
            for row in result:
                ingestion_cnt = ingestion_cnt + row[2]
        except Exception as e:
            ingestion_cnt = 0
        return [returncode,copystmnt,result,ingestion_cnt]

    except Exception as e:
        returncode=1
        result=str(e)
        ingestion_cnt = 0
        return [returncode,copystmnt,result,ingestion_cnt]
        

def audit_entry(job):
    print("Audit test")



def insert_audit_batch(batch_id):
    try:
        query=f"""INSERT INTO DATAMIGRATION.DEMO_USER.DATA_INGESTION_AUDIT_TABLE (JOB_ID, BATCH_ID, FILE_PATTERN, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, SOURCE_INFO, FILE_FORMAT_OBJECT_STATEMENT, INGESTION_STATEMENT, SOURCE_COUNT, TARGET_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS) SELECT JOB_ID, BATCH_ID, FILE_PATTERN, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_MODE, FILE_TYPE, SOURCE_INFO, FILE_FORMAT_OBJECT_STATEMENT, INGESTION_STATEMENT, SOURCE_COUNT, TARGET_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS FROM DATAMIGRATION.DEMO_USER.DATA_INGESTION_LOG_TABLE WHERE BATCH_ID = {batch_id} ;"""
        result=str(sfquery(query))
        returncode=0  
    except Exception as e:
        returncode=1
        result=str(e)

def create_target_table(job,file_format_obj_name,act_path):
    print("Rama")
    job_id = job[0]
    batch_id = job[1]
    file_pattern = job[2]
    cloud_path = job[3]
    sf_database_name = job[4]
    sf_schema_name = job[5]
    sf_table_name = job[6]
    warehouse_name = job[7]
    load_mode = job[8]
    file_type = job[9].lower()
    table_exists = job[16]

    print(act_path,file_format_obj_name)

    if table_exists == 'YES' and load_mode == 'append':
        query=f"""SELECT TABLE_NAME FROM {sf_database_name}.INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = '{sf_schema_name}' AND TABLE_NAME = '{sf_table_name}';"""
        result=sfquery(query)
        print(result,len(result))
        if len(result)==1:
            result=f'Table {sf_database_name}.{sf_schema_name}.{sf_table_name} already exists.'
            returncode=0    
        else:
            result=f'Table {sf_database_name}.{sf_schema_name}.{sf_table_name} does not exist. Cannot proceed with APPEND load.'
            returncode=1
        return [returncode,result]
    
    elif table_exists == 'YES' and load_mode == 'overwrite':
        try:
            query=f"""TRUNCATE TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name};"""

            result=str(sfquery(query))
            result = query + '\n' + str(result)
            returncode=0
            
        except Exception as e:
            query=f"""TRUNCATE TABLE {sf_database_name}.{sf_schema_name}.{sf_table_name};"""
            
            result=str(e)
            result = query + '\n' + str(result)
            returncode=1
        
        return [returncode,result]
    
    elif table_exists == 'NO':

        with open(r'C:\Users\dines\Pictures\NEW_VM\TA_DATA_INGESTION_WRK\credentials.json','r+') as config_file:
            cred=json.load(config_file)

        sf_aws_storage_int = cred['sf_aws_storage_integration']
        sf_aws_stage = cred['sf_aws_stage']
        sf_azure_storage_int = cred['sf_blob_storage_integration']
        sf_azure_stage = cred['sf_blob_stage']
        if r's3://' in cloud_path:
            cloud_code='S3'
            storage_integration = rf'{sf_aws_storage_int}'
            stage_object = rf'{sf_aws_stage}'
            
        elif r'azure://' in cloud_path:
            cloud_code='AZ'
            storage_integration = rf'{sf_azure_storage_int}'
            stage_object = rf'{sf_azure_stage}'

        sf_host = cred['sf_host']
        sf_user = cred['sf_user']
        sf_password = cred['sf_password']
        sf_warehouse = warehouse_name
        sf_database = sf_database_name
        sf_schema = sf_schema_name

        spcon = {
        "account": sf_host,
        "user": sf_user,
        "password": sf_password,
        "warehouse": sf_warehouse,
        "database": sf_database,
        "schema":sf_schema
        }

        print(spcon)

        spsession=Session.builder.configs(spcon).create()
        session_id = spsession.session_id
        opt={"pattern" : f"{file_pattern}",
        "inferSchema" :"True",
        "format_name" : f"{sf_database_name}.{sf_schema_name}.{file_format_obj_name}"}

        print(opt)

        
        #df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")
        try:
            
            if file_type == 'csv':
                df_snowpark = spsession.read.options(opt).csv(f"@{stage_object}/{act_path}")
            
            elif file_type == 'parquet':
                df_snowpark = spsession.read.options(opt).parquet(f"@{stage_object}/{act_path}")
                print(df_snowpark)
            elif file_type == 'json':
                df_snowpark = spsession.read.options(opt).json(f"@{stage_object}/{act_path}")

            #df_snowpark = spsession.read.options(opt).options(opt).load(f"@{stage_object}/{act_path}")
            load_mode='overwrite' #ALWAYS OVERWRITE FOR CREATE TABLE
            df_snowpark.print_schema()
            df_snowpark.columns
            empty_df = df_snowpark.limit(0)
            empty_df.write.mode(f"{load_mode}").save_as_table(f"{sf_database_name}.{sf_schema_name}.{sf_table_name}")
            print(session_id)
            #his=spsession.sql(f"""select QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
            #                    from table(information_schema.query_history_by_session()) where SESSION_ID={session_id} 
            #                    order by start_time DESC;""")
            
            query_stmt = f"""SELECT QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                                FROM TABLE(DATAMIGRATION.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                                WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP) AND QUERY_TEXT LIKE 'CREATE  OR  REPLACE    TABLE  {sf_database_name}.{sf_schema_name}.{sf_table_name}(%' AND SESSION_ID = {session_id}
                                ORDER BY START_TIME DESC;"""
            print(query_stmt)
            his=spsession.sql(query_stmt)
            print(his)
            history=his.collect()
            print(history)
            print('Query result',history[0][0])
            
            create_table_query=history[0][0]
            
            returncode = history[0][4]
            if returncode == None:
                returncode=0
            print(returncode)
                
        except Exception as e:
            '''query_stmt = f"""SELECT QUERY_TEXT,SESSION_ID,ROWS_PRODUCED,ROWS_INSERTED,ERROR_CODE,ERROR_MESSAGE
                                FROM TABLE(DATAMIGRATION.INFORMATION_SCHEMA.QUERY_HISTORY_BY_SESSION())
                                WHERE START_TIME >= DATEADD('day', -1, CURRENT_TIMESTAMP) AND QUERY_TEXT LIKE 'CREATE  OR  REPLACE    TABLE  {sf_database_name}.{sf_schema_name}.{sf_table_name}(%' AND SESSION_ID = {session_id}
                                ORDER BY START_TIME DESC;"""
                     
            his=spsession.sql(query_stmt)
            history=his.collect()
            print(history)
            print('Query result',history[0][0], history[0][5])
            create_table_query=history[0][0] + '\n' + str(history[0][5])
            #ingestion_query=ingestion_query
            '''
            create_table_query=str(e)
            returncode = 1
        
        spsession.close()
        return [returncode,create_table_query]



 
def create_stage(sfdatabasename,sfschemaname,cloud_path):


    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = r'S3_BUCKET'
        
    elif r'blob.core.windows.net' in cloud_path:
        cloud_code='AZ'
        storage_integration = r'AZURE_BLOB_CONTAINER'


    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = '{cloud_code}_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)==1:
        print("Stage present")
        print(result)
        result=str(result[0][0]) + ' ALREADY EXISTS'
        stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        returncode=0
    else:
        print(result)
        print("Stage Not present")
        stagename=f'{cloud_code}_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{cloud_path}'
                STORAGE_INTEGRATION = {storage_integration};"""
        print(query1)
        try:
            result=sfquery(query1)
            print(result)
            returncode=0
            stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        except Exception as e:
            returncode=1
            result=str(e)
            stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
    

    return [returncode,result,stagename]
    '''
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
    except Exception as e:
        returncode=1
        stagename=f'S3_{sfdatabasename}_{sfschemaname}'
        result=str(e)
        return [returncode,f"""{sfdatabasename}.{sfschemaname}.{stagename}""",result]
'''



#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#a,b,c=create_stage('DATAMIGRATION','DEMO_USER','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/')

#print(a,b,c)
'''
def mergecommand(job,export_start_time):

    print(job,export_start_time)
''' 



'''
sd=getcdcdates('DEMO_USER','IOP')
print(sd)
'''

def auditupdate(job,export_start_time):

    print(job,export_start_time)
    tddbname=job[0]
    tdtablename=job[1]

    audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
    try:
        result=sfquery(audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)
    
    print(audit_query)
    return [returncode,auditstmnt,returnstmnt]

'''
export_start_time='2025-02-06 09:08:04.110000'
tdtablename='IOP'
tddbname='DEMO_USER'
audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE TD_DATABASE_NAME='{tddbname}' AND TD_TABLE_NAME='{tdtablename}';"""
print(audit_query)
'''


def sfcount(sfdbname,sfschname,sftablename):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #job_end_time=tdquery(query2)[0][0]
    #job_end_time=str(datetime.now() - timedelta(hours=5))
    try:
        query1=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}_WRK.{sftablename};"
        sfcnt=sfquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        sfcnt=str(e)
        

    return [returncode,sfcnt]
