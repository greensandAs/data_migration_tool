import snowflake.connector
import json

from datetime import datetime, timedelta

def sfquery(query):
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
    
    query=query
     
    #print(query)

    with sfcon.cursor() as curr:
        curr.execute(query)
        result=curr.fetchall()
    
    #print(result,type(result))
    return result


def create_table(sfdatabasename,sfschemaname,sftablename,loadtype):
    if loadtype == 'CUSTOM_SQL':
        '''
        try:
            query=f"""CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}_WRK.{sftablename} ("""
            for i in schcol:
                c=i.strip()
                c=c.replace("\n"," ")
                c=c.replace("\t"," ")
                query= query + c 
            query1 = query +");"
            
            query2= query.replace(f"CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}_WRK.{sftablename}",f"CREATE OR REPLACE TABLE {sfdatabasename}.{sfschemaname}.{sftablename}")
            query2 = query2 +");"
            result=str(sfquery(query1))
            result=str(sfquery(query2))
            returncode=0

            #print(query)
        except Exception as e:
            returncode=1
            result=str(e)
        '''
    else:
        try:
            query=f"""DELETE FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"""
        
            result=str(sfquery(query))
            returncode=0
            
        except Exception as e:
            returncode=1
            result=str(e)
    
    return [returncode,result]

 
def create_stage(sfdatabasename,sfschemaname,cloud_path):

    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = r'ms_s3_int'
    
    elif r'blob.core.windows.net' in cloud_path:
        cloud_code='AZ'
        storage_integration = r'mssql_azure_container'


    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = '{cloud_code}_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)==1:
        #print("Stage present")
        #print(result)
        result=str(result[0][0]) + ' ALREADY EXISTS'
        stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        returncode=0
    else:
        #print(result)
        #print("Stage Not present")
        stagename=f'{cloud_code}_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{cloud_path}'
                STORAGE_INTEGRATION = {storage_integration};"""
        #print(query1)
        try:

            result=sfquery(query1)
            #print(result)
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

def copycommand(stagename,jobdetails,uploadfilename):
    # print(stagename,jobdetails,uploadfilename)
    copystmnt = ""
    try:


        msdbname=jobdetails[0]
        msschemaname = jobdetails[1]
        mstablename=jobdetails[2]
        sfdatabasename=jobdetails[3]
        sfschemaname=jobdetails[4]
        sftablename=jobdetails[5]
        delimiter=jobdetails[11]
        s3_path=jobdetails[15]
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename



        #print(msdbname,msschemaname, mstablename,s3_path,sfdatabasename,sfschemaname,sftablename,delimiter,uploadfilename,sfwrktable)



        #stagename=create_stage(sfdatabasename,sfschemaname,s3_path)

        clear_stage_table =f"""DELETE FROM {sfwrktable};"""
        sfquery(clear_stage_table)
        # print("Checkkk point 1")
        copystmnt=fr"""COPY INTO {sfwrktable} FROM @{stagename}/{uploadfoldername}/ FILE_FORMAT = (TYPE = 'CSV', 
        FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}', COMPRESSION = 'GZIP')"""
        # print(f"copystmnt : {copystmnt}")
        
        result=str(sfquery(copystmnt))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,copystmnt,result]

#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#a,b,c=create_stage('DATAMIGRATION','DEMO_USER','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/')

##print(a,b,c)
'''
def mergecommand(job,export_start_time):
    #print("RAGAVA")
    #print(job,export_start_time)
''' 


def mergecommand(job,uploadfilename):

    #print(job)
    try:    
        msdbname=job[0]
        msschemaname = job[1]
        mstablename=job[2]   
        sfdatabasename=job[3]
        sfschemaname=job[4]
        sftablename=job[5]
        filter=job[12]
        scd_type=job[7]
        load_type=job[8]
        custom_sql=job[18]
        cloud_path=job[15]
        primarykey=list(job[10].split(","))
        #print(primarykey)

        if load_type=='FULL':
            #print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            #print(delstatement)
            #print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt = delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)


        elif load_type=='FILTER':
            if filter == None:
                filter = '(1=1)'

            if custom_sql != None:
                #try:
                filter = custom_sql[custom_sql.index('WHERE')+5:]
                #except Exception as f:
                #    filter = '(1=1)'
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename} WHERE {filter};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            #print(delstatement)
            #print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)
        
        elif load_type=='CUSTOM_SQL':
            #print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            #print(delstatement)
            #print(insstatement)
            try:
                delreturn=sfquery(delstatement)
                insreturn=sfquery(insstatement)
                runstmnt = delstatement +"   \n" + insstatement
                returnstmnt=f"Number Of Records Deleted : {str(delreturn[0][0])} \n Number Of Records Inserted : {str(insreturn[0][0])}"
                returncode=0
            except Exception as e:
                returncode=1
                runstmnt= delstatement +"   \n" + insstatement
                returnstmnt=str(e)

        elif load_type=='INCREMENTAL':
            
            if scd_type==0:
                insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
                try:
                    insreturn=sfquery(insstatement)
                    runstmnt=insstatement
                    returnstmnt=f"Number Of Records Inserted : {str(insreturn[0][0])}"
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt= insstatement
                    returnstmnt=str(e)

            if scd_type==1:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                

                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                # #print("Narayana",collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                #print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)



            if scd_type==2:
                pkcondition=""
                updstatement=""
                insstatement="("
                valstatement="("

                for i in primarykey:
                    t=f"TARGET.{i}=SOURCE.{i} AND "
                    pkcondition=pkcondition+t
                pkcondition=pkcondition[:-4]

                colliststatement=F"""SELECT COLUMN_NAME
                    FROM {sfdatabasename}.INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = '{sftablename}' and TABLE_SCHEMA='{sfschemaname}' AND TABLE_CATALOG='{sfdatabasename}'
                    ORDER BY ORDINAL_POSITION;"""    
                
                colreturn=sfquery(colliststatement)
                collist=[]
                
                for i in colreturn:
                    collist.append(i[0])
                
                # #print(collist)

                for i in collist:
                    t=f"TARGET.{i}=SOURCE.{i}, "
                    r=f"{i}, "
                    e=f"SOURCE.{i}, "
                    updstatement=updstatement+t
                    insstatement=insstatement+r
                    valstatement=valstatement+e
                updstatement=updstatement[:-2]
                insstatement=insstatement[:-2]+')'
                valstatement=valstatement[:-2]+')'

                merstatement=f"""MERGE INTO {sfdatabasename}.{sfschemaname}.{sftablename} AS TARGET USING {sfdatabasename}.{sfschemaname}_WRK.{sftablename} AS SOURCE ON 
                    {pkcondition}
                    WHEN MATCHED THEN UPDATE SET 
                    {updstatement}
                    WHEN NOT MATCHED THEN INSERT 
                    {insstatement}
                    VALUES
                    {valstatement} ;
                    """
                #print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)
                    
        update_audit_ingest = f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET INGESTION_COMPLETED = 'YES' WHERE MSSQL_DATABASE_NAME='{msdbname}' AND MSSQL_TABLE_NAME='{mstablename}' AND EXPORT_FILENAME = '{uploadfilename}' AND S3_PATH = '{cloud_path}' ;"""
        update_log_ingest = f"""UPDATE DATAMIGRATION.DEMO_USER.LOG_TABLE SET INGESTION_COMPLETED = 'YES' WHERE  MSSQL_DATABASE_NAME='{msdbname}' AND MSSQL_TABLE_NAME='{mstablename}' AND EXPORT_FILENAME = '{uploadfilename}' AND S3_PATH = '{cloud_path}' ;"""

        sfquery(update_audit_ingest)
        sfquery(update_log_ingest)
        return [returncode,runstmnt,returnstmnt]
    except Exception as e:
        returncode=1
        result=str(e)
        runstmnt=""

        return [returncode,runstmnt,returnstmnt]

def getcdcdates(msdbname,mstablename,cdc_type):
    if cdc_type == 'TIMESTAMP':
        query2=f"""SELECT CAST(EXTRACTENDDTTM AS VARCHAR) AS EXTRACTENDDTTM,CAST(EXTRACTSTARTDTTM AS VARCHAR) AS EXTRACTSTARTDTTM
        FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE MSSQL_DATABASE_NAME='{msdbname}' and MSSQL_TABLE_NAME='{mstablename}' AND FINAL_STATUS = 'SUCCESS' AND 
        BATCH_ID = (SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE MSSQL_DATABASE_NAME='{msdbname}'
        and MSSQL_TABLE_NAME='{mstablename}' AND FINAL_STATUS = 'SUCCESS');"""   
    elif cdc_type == 'ID':
        query2=f"""SELECT CAST(CDC_ID AS VARCHAR) AS CDC_ID
        FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE MSSQL_DATABASE_NAME='{msdbname}' and MSSQL_TABLE_NAME='{mstablename}' AND FINAL_STATUS = 'SUCCESS' AND 
        BATCH_ID = (SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE MSSQL_DATABASE_NAME='{msdbname}'
        and MSSQL_TABLE_NAME='{mstablename}' AND FINAL_STATUS = 'SUCCESS');""" 
    result=sfquery(query2)
    return result


'''
sd=getcdcdates('DEMO_USER','IOP')
#print(sd)
'''

def auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id):
    execution_mode=job[19]
    # if execution_mode == 'INGEST':
    #     extract_start_dttm = 'NULL'
    #     extract_end_dttm = 'NULL'
    #     cdc_id = 'NULL'
    # else:
    # print(extract_start_dttm)
    # print(extract_end_dttm)
    # print(cdc_id)
    extract_start_dttm = 'NULL' if (extract_start_dttm == 'NULL' or extract_start_dttm == 'None' or extract_start_dttm == None) else f"'{extract_start_dttm}'"
    extract_end_dttm = 'NULL' if (extract_end_dttm == 'NULL' or extract_end_dttm == 'None' or extract_end_dttm == None) else f"'{extract_end_dttm}'"
    cdc_id = f"{cdc_id}"
        
    audit_query = f"""INSERT INTO DATAMIGRATION.DEMO_USER.AUDIT_TABLE
    (BATCH_ID, JOB_ID, MSSQL_DATABASE_NAME ,MSSQL_SCHEMA_NAME ,MSSQL_TABLE_NAME ,SF_DATABASE_NAME ,SF_SCHEMA_NAME ,SF_TABLE_NAME , LOAD_TYPE, BCP_SCRIPT_NAME, BCP_EXPORT_CMD ,BCP_EXPORT_LOG , S3_UPLOAD_CMD, S3_UPLOAD_LOG, CREATE_TABLE_LOG, CREATE_STAGE_NAME, CREATE_STAGE_LOG, COPY_COMMAND, COPY_COMMAND_LOG, MERGE_STATEMENT, MERGE_STATEMENT_LOG, PREV_EXTRACTSTARTDTTM, PREV_EXTRACTENDDTTM, EXTRACTSTARTDTTM, EXTRACTENDDTTM,CDC_ID,S3_PATH,EXPORT_FILENAME,EXECUTION_MODE, MSSQL_TABLE_COUNT, SF_TABLE_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS,INGESTION_COMPLETED)
    SELECT BATCH_ID, JOB_ID, MSSQL_DATABASE_NAME ,MSSQL_SCHEMA_NAME ,MSSQL_TABLE_NAME ,SF_DATABASE_NAME ,SF_SCHEMA_NAME ,SF_TABLE_NAME , LOAD_TYPE, BCP_SCRIPT_NAME, BCP_EXPORT_CMD ,BCP_EXPORT_LOG , S3_UPLOAD_CMD, S3_UPLOAD_LOG, CREATE_TABLE_LOG, CREATE_STAGE_NAME, CREATE_STAGE_LOG, COPY_COMMAND, COPY_COMMAND_LOG, MERGE_STATEMENT, MERGE_STATEMENT_LOG, NULL, NULL, {extract_start_dttm}, {extract_end_dttm},{cdc_id},S3_PATH,EXPORT_FILENAME,EXECUTION_MODE, MSSQL_TABLE_COUNT, SF_TABLE_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS,INGESTION_COMPLETED 
    FROM DATAMIGRATION.DEMO_USER.LOG_TABLE WHERE BATCH_ID = {batch_id} AND JOB_ID = {job_id};"""   
    #print("Audit update-",audit_query) 

    try:
        result=sfquery(audit_query)
        #print("sfquery auditquery", audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)  

    return [returncode,auditstmnt,returnstmnt]
'''


    # audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE MSSQL_DATABASE_NAME='{msdbname}' AND MSSQL_SCHEMA_NAME = '{msschemaname}' AND MSSQL_TABLE_NAME='{mstablename}';"""
    # try:
    #     result=sfquery(audit_query)
    #     auditstmnt=audit_query
    #     returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
    #     returncode=0
    # except Exception as e:
    #     returncode=1
    #     auditstmnt=audit_query
    #     returnstmnt=str(e)
    
    # #print(audit_query)
    # return [returncode,auditstmnt,returnstmnt]



'''

def sfcount(sfdbname,sfschname,sftablename,loadtype):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #job_end_time=tdquery(query2)[0][0]
    #job_end_time=str(datetime.now() - timedelta(hours=5))
    try:
        if loadtype == 'FILTER':
            sfcount_query=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}_WRK.{sftablename};"
        else:
            sfcount_query=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}.{sftablename};"
        sfcnt=sfquery(sfcount_query)[0][0]

        returncode=0
    except Exception as e:
        returncode=1
        sfcnt=str(e)
        

    return [returncode,sfcnt]
