import snowflake.connector
import json
# from oracle_utils import oracle_query
from datetime import datetime, timedelta

def sfquery(query):
    with open('C:/Users/palanivelu.murug/Documents/Datamigration_oracle_final_version - 1215/Datamigration/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    sf_host = cred['sf_host']
    sf_user = cred['sf_user']
    sf_password = cred['sf_password']
    sf_warehouse = cred['sf_warehouse']
    sf_database = cred['sf_database']
    sf_schema = cred['sf_schema']
    ##print("RAMA")            

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
    
    # #print(result,type(result))
    return result


def create_table(sfdatabasename,sfschemaname,sftablename,loadtype):
    #print("SF TABLE DETAILS: ",sfdatabasename,sfschemaname,sftablename)
    if loadtype == 'CUSTOM_SQL':
        # #print("KAMAKSHI")
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
    # #print("MAHALAKSHMI")

    if r's3://' in cloud_path:
        cloud_code='S3'
        storage_integration = r'S3_BUCKET'
        
    elif r'blob.core.windows.net' in cloud_path:
        cloud_code='AZ'
        storage_integration = r'oracle_azure_container'


    query=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = '{cloud_code}_{sfdatabasename}_{sfschemaname}';"""
    result=sfquery(query)
    if len(result)==1:
        #print("Stage present")
        # #print(result)
        result=str(result[0][0]) + ' ALREADY EXISTS'
        stagename=f'{sfdatabasename}.{sfschemaname}.{cloud_code}_{sfdatabasename}_{sfschemaname}'
        returncode=0
    else:
        # #print(result)
        #print("Stage Not present")
        stagename=f'{cloud_code}_{sfdatabasename}_{sfschemaname}'
        query1=f"""CREATE OR REPLACE STAGE {sfdatabasename}.{sfschemaname}.{stagename}
                URL='{cloud_path}'
                STORAGE_INTEGRATION = {storage_integration};"""
        #print(query1)
        try:
            result=sfquery(query1)
            # #print(result)
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
    try:

        ##print("NARAYANA")
        #print(jobdetails,uploadfilename)


        oracleschemaname=jobdetails[0]
        oracletablename=jobdetails[1]
        sfdatabasename=jobdetails[2]
        sfschemaname=jobdetails[3]
        sftablename=jobdetails[4]
        delimiter=jobdetails[10]
        s3_path=jobdetails[14]
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename

        '''
        uploadfilename='DEMO_USER_IOP_TPT_20250119_0818.csv'
        sfdatabasename='DATAMIGRATION'
        sfschemaname='DEMO_USER_ORACLE'
        sftablename='IOP'
        delimiter=','
        uploadfoldername=uploadfilename.replace('.csv','')
        sfwrktable=sfdatabasename+'.'+sfschemaname+'_WRK'+'.'+sftablename
        '''

        # #print(oracleschemaname,oracletablename,s3_path,uploadfilename,sfdatabasename,sfschemaname,sftablename,delimiter,uploadfilename,uploadfoldername,sfwrktable)

        ##print("THIRUVIKRAMA")

        #stagename=create_stage(sfdatabasename,sfschemaname,s3_path)

        
        clear_stage_table =f"""DELETE FROM {sfwrktable};"""
        sfquery(clear_stage_table)
        ##print("THIRUVIKRAMA") 
        

        copystmnt=fr"""COPY INTO {sfwrktable} FROM @{stagename}/{uploadfoldername}/ FILE_FORMAT = ( TYPE = 'CSV', FIELD_OPTIONALLY_ENCLOSED_BY = '"', FIELD_DELIMITER = '{delimiter}', SKIP_HEADER = 1 )"""

    
        result=str(sfquery(copystmnt))
        returncode=0
        
    except Exception as e:
        returncode=1
        result=str(e)
    
    return [returncode,copystmnt,result]

#copycommand(1,1)

#sfquery('test') 
#sfdatabasename='DATAMIGRATION'
#sfschemaname='DEMO_USER_ORACLE'

#qw=f"""SELECT STAGE_NAME FROM  {sfdatabasename}.INFORMATION_SCHEMA.STAGES WHERE stage_schema = '{sfschemaname}' AND STAGE_NAME = 'S3_{sfdatabasename}_{sfschemaname}';"""
#sd=sfquery(qw)

#a,b,c=create_stage('DATAMIGRATION','DEMO_USER_ORACLE','s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER_ORACLE/')

##print(a,b,c)
'''
def mergecommand(job,export_start_time):
    #print("RAGAVA")
    #print(job,export_start_time)
''' 


def mergecommand(job,uploadfilename):
    ##print("RAGAVA")
    #print(job)
    try:
        oracleschemaname=job[0]
        oracletablename=job[1]
        sfdatabasename=job[2]
        sfschemaname=job[3]
        sftablename=job[4]
        filter=job[11]
        scd_type=job[6]
        load_type=job[7]
        custom_sql=job[17]
        cloud_path=job[14]
        primarykey=list(job[9].split(","))
        # #print(primarykey)

        if load_type=='FULL':
            # #print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            # #print(delstatement)
            # #print(insstatement)
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
            # #print(delstatement)
            # #print(insstatement)
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
            # #print(load_type)
            
            delstatement=f"DELETE FROM {sfdatabasename}.{sfschemaname}.{sftablename};"
            insstatement=f"INSERT INTO {sfdatabasename}.{sfschemaname}.{sftablename} SELECT * FROM {sfdatabasename}.{sfschemaname}_WRK.{sftablename};"
            # #print(delstatement)
            # #print(insstatement)
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
                
                ##print("Narayana",collist)

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
                # #print(merstatement)
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
                
                ##print("Narayana",collist)

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
                # #print(merstatement)
                try:
                    merreturn=sfquery(merstatement)
                    runstmnt=merstatement
                    returnstmnt=f"""Number Of Records Inserted : {str(merreturn[0][0])} \n Number of Records Updated : {str(merreturn[0][1])}"""
                    returncode=0
                except Exception as e:
                    returncode=1
                    runstmnt=merstatement
                    returnstmnt=str(e)
        update_audit_ingest = f"""UPDATE DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE SET INGESTION_COMPLETED = 'YES' WHERE ORACLE_SCHEMA_NAME='{oracleschemaname}' AND ORACLE_TABLE_NAME='{oracletablename}' AND EXPORT_FILENAME = '{uploadfilename}' AND S3_PATH = '{cloud_path}' ;"""
        update_log_ingest = f"""UPDATE DATAMIGRATION.DEMO_USER_ORACLE.LOG_TABLE SET INGESTION_COMPLETED = 'YES' WHERE ORACLE_SCHEMA_NAME='{oracleschemaname}' AND ORACLE_TABLE_NAME='{oracletablename}' AND EXPORT_FILENAME = '{uploadfilename}' AND S3_PATH = '{cloud_path}' ;"""

        sfquery(update_audit_ingest)
        sfquery(update_log_ingest)
        ##print("AACHUTHA")
        return [returncode,runstmnt,returnstmnt]
    except Exception as e:
        returncode=1
        result=str(e)
        runstmnt=""
        ##print("AACHUTHA")

        return [returncode,runstmnt,returnstmnt]

def getcdcdates(schema_name,table_name):
    query2=f"""SELECT CAST(EXTRACTSTARTDTTM AS VARCHAR) AS EXTRACTSTARTDTTM,CAST(EXTRACTENDDTTM AS VARCHAR) AS EXTRACTENDDTTM FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE ORACLE_SCHEMA_NAME='{schema_name}' and ORACLE_TABLE_NAME='{table_name}'"""
    result=sfquery(query2)
    return result
# query2=f"""SELECT CAST(EXTRACTENDDTTM AS VARCHAR) AS EXTRACTENDDTTM,CAST(EXTRACTSTARTDTTM AS VARCHAR) AS EXTRACTSTARTDTTM FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE TD_DATABASE_NAME='{tddbname}' and TD_TABLE_NAME='{tdtablename}' AND FINAL_STATUS = 'SUCCESS' AND BATCH_ID = (SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER.AUDIT_TABLE WHERE TD_DATABASE_NAME='{tddbname}' and TD_TABLE_NAME='{tdtablename}' AND FINAL_STATUS = 'SUCCESS');"""
    
def getcdcdatesfororacle(oracle_schema_name,oracle_table_name,cdc_type):
    if cdc_type == 'TIMESTAMP':
        query2=f"""SELECT CAST(TO_CHAR(EXTRACTENDDTTM, 'YYYY-MM-DD HH12:MI:SS AM') AS VARCHAR) AS EXTRACTENDDTTM , CAST(TO_CHAR(EXTRACTSTARTDTTM, 'YYYY-MM-DD HH12:MI:SS AM') AS VARCHAR) AS EXTRACTSTARTDTTM FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE ORACLE_SCHEMA_NAME='{oracle_schema_name}' and ORACLE_TABLE_NAME='{oracle_table_name}' AND FINAL_STATUS = 'SUCCESS' AND BATCH_ID = (SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE ORACLE_SCHEMA_NAME='{oracle_schema_name}' and ORACLE_TABLE_NAME='{oracle_table_name}' AND FINAL_STATUS = 'SUCCESS');"""
    elif cdc_type == 'ID':
        query2=f"""SELECT CAST(CDC_ID AS VARCHAR) AS CDC_ID FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE ORACLE_SCHEMA_NAME='{oracle_schema_name}' and ORACLE_TABLE_NAME='{oracle_table_name}' AND FINAL_STATUS = 'SUCCESS' AND BATCH_ID = (SELECT MAX(BATCH_ID) FROM DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE WHERE ORACLE_SCHEMA_NAME='{oracle_schema_name}' and ORACLE_TABLE_NAME='{oracle_table_name}' AND FINAL_STATUS = 'SUCCESS');"""
    # print(query2)
    result=sfquery(query2)
    return result

'''
sd=getcdcdates('DEMO_USER_ORACLE','IOP')
#print(sd)
'''

def auditupdate(job,extract_start_dttm,extract_end_dttm,cdc_id,batch_id,job_id):
    # print("AACHUTHA")
    # print(cdc_id)
    '''
    #print(job,export_start_time)
    tddbname=job[0]
    tdtablename=job[1]

    audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE ORACLE_SCHEMA_NAME='{tddbname}' AND ORACLE_TABLE_NAME='{tdtablename}';"""
    try:
        result=sfquery(audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)
    
    #print(audit_query)
    return [returncode,auditstmnt,returnstmnt]
    '''
    execution_mode=job[18]
    if execution_mode == 'INGEST':
        extract_start_dttm = 'NULL'
        extract_end_dttm = 'NULL'
        cdc_id = 'NULL'
    else:
        extract_start_dttm = 'NULL' if extract_start_dttm == 'NULL' else f"'{extract_start_dttm}'"
        extract_end_dttm = 'NULL' if extract_end_dttm == 'NULL' else f"'{extract_end_dttm}'"
        cdc_id = f"{cdc_id}"
    audit_query = f"""INSERT INTO DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE(
            BATCH_ID, JOB_ID, ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE, ORACLE_EXPORT_QUERY, ORACLE_EXPORT_LOG, S3_UPLOAD_CMD, S3_UPLOAD_LOG, CREATE_TABLE_LOG, CREATE_STAGE_NAME, CREATE_STAGE_LOG, COPY_COMMAND, COPY_COMMAND_LOG, MERGE_STATEMENT, MERGE_STATEMENT_LOG, PREV_EXTRACTSTARTDTTM, PREV_EXTRACTENDDTTM, EXTRACTSTARTDTTM, EXTRACTENDDTTM,CDC_ID,S3_PATH,EXPORT_FILENAME,EXECUTION_MODE, ORACLE_TABLE_COUNT, SF_TABLE_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS,INGESTION_COMPLETED)
            SELECT BATCH_ID, JOB_ID, ORACLE_SCHEMA_NAME, ORACLE_TABLE_NAME, SF_DATABASE_NAME, SF_SCHEMA_NAME, SF_TABLE_NAME, LOAD_TYPE, ORACLE_EXPORT_QUERY, ORACLE_EXPORT_LOG, S3_UPLOAD_CMD, S3_UPLOAD_LOG, CREATE_TABLE_LOG, CREATE_STAGE_NAME, CREATE_STAGE_LOG, COPY_COMMAND, COPY_COMMAND_LOG, MERGE_STATEMENT, MERGE_STATEMENT_LOG, NULL, NULL, TO_TIMESTAMP({extract_start_dttm}, 'YYYY-MM-DD HH12:MI:SS AM'), TO_TIMESTAMP({extract_end_dttm}, 'YYYY-MM-DD HH12:MI:SS AM'),{cdc_id},S3_PATH,EXPORT_FILENAME,EXECUTION_MODE, ORACLE_TABLE_COUNT, SF_TABLE_COUNT, JOB_START_TIME, JOB_END_TIME, JOB_DURATION, FINAL_STATUS,INGESTION_COMPLETED FROM DATAMIGRATION.DEMO_USER_ORACLE.LOG_TABLE WHERE BATCH_ID = {batch_id} AND JOB_ID = {job_id};"""
    #print(audit_query) 
    ##print("AACHUTHA")

    try:
        result=sfquery(audit_query)
        auditstmnt=audit_query
        returnstmnt=f"Number Of Records Updated : {str(result[0][0])}"
        returncode=0
    except Exception as e:
        returncode=1
        auditstmnt=audit_query
        returnstmnt=str(e)  

    return [returncode,auditstmnt,returnstmnt]
'''
    audit_query = f"""INSERT INTO DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE (TD_DATABASE_NAME,TD_TABLE_NAME,EXTRACTSTARTDTTM,EXTRACTENDDTTM) SELECT ()
 



export_start_time='2025-02-06 09:08:04.110000'
tdtablename='IOP'
tddbname='DEMO_USER_ORACLE'
audit_query=f"""UPDATE DATAMIGRATION.DEMO_USER_ORACLE.AUDIT_TABLE SET PREV_EXTRACTSTARTDTTM=EXTRACTSTARTDTTM  , PREV_EXTRACTENDDTTM='{export_start_time}' , EXTRACTSTARTDTTM='{export_start_time}' ,EXTRACTENDDTTM=NULL WHERE ORACLE_SCHEMA_NAME='{tddbname}' AND ORACLE_TABLE_NAME='{tdtablename}';"""
#print(audit_query)
'''


def sfcount(sfdbname,sfschname,sftablename,loadtype):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #job_end_time=tdquery(query2)[0][0]
    #job_end_time=str(datetime.now() - timedelta(hours=5))
    try:
        if loadtype == 'FILTER':
            query1=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}_WRK.{sftablename};"
        else:
            query1=f"SELECT COUNT(*) FROM {sfdbname}.{sfschname}.{sftablename};"
        sfcnt=sfquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        sfcnt=str(e)
        

    return [returncode,sfcnt]
