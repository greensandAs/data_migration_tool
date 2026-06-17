import subprocess
import os
import time
from datetime import datetime
import json
import pyodbc
import re
import snowflake.connector
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import ProcessPoolExecutor
from concurrent.futures import as_completed
from mslogger import batch_create,log_update
from mssf_utils import auditupdate,getcdcdates
import threading
from threading import Lock
import shutil
import math
import gzip





def msquery(query):
    try:
        with open('C:/Users/palanivelu.murug/Documents/Datamigration_gzip_while_export_2026_04_03/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        server_name = cred['server_name']
        sqlsvr_user = cred['sqlsvr_user']
        sqlsvr_password = cred['sqlsvr_password']

        mscon = pyodbc.connect(f'DRIVER={{ODBC Driver 17 for SQL Server}};'
                      f'SERVER={server_name};'
                      f'Trusted_Connection=yes;'
                      f'Encrypt=yes;'
                      f'TrustServerCertificate=yes') 
        ##print("Connected to database.")     

        cur=mscon.cursor()
        sd=cur.execute(query)
        result=sd.fetchall()
        # print(query)
        # print(type(result))
        # print(result)
        return result
    except Exception as e:
        ##print(f"An error occurred: {e}")
        return None



def mscount(count_query):
    try:
        # query1=f"SELECT COUNT(*) FROM {msdbname}.{msschemaname}.{mstablename};"
        # print(f"count_query: {count_query}")
        mssqlcnt=msquery(count_query)[0][0]
        returncode=0
        ##print(f'mscountdone & {mssqlcnt}')
    except Exception as e:
        returncode=1
        mssqlcnt=str(e)
    return [returncode,mssqlcnt]





def file_name_generator(job):
    try:
        with open('C:/Users/palanivelu.murug/Documents/Datamigration_gzip_while_export_2026_04_03/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        server_name = cred['server_name']
        sqlsvr_user = cred['sqlsvr_user']
        sqlsvr_password = cred['sqlsvr_password']
        bcp_export_path = cred['bcp_export_path']
        bcp_split_path = cred['bcp_split_path']

        msdbname=job[0]
        msschemaname = job[1]
        mstablename=job[2]
        scdtype=job[7]
        loadtype=job[8]
        cdccol=job[9]
        delimiter=job[11]
        filterconditon=job[12]
        trim=job[13]
        encrpt=job[14]
        custom_sql = job[18]
        batch_id = job[16]
        job_id=job[17]
        cdc_type = job[20]
        # print("cdc_type")
        # print(cdc_type)
        export_start_time=msquery("SELECT CURRENT_TIMESTAMP")[0][0]
        ##print(export_start_time)
        curr_datetime = str(export_start_time)[:19]
        curr_datetime=curr_datetime.replace(" ","_")
        curr_datetime=curr_datetime.replace(":","_")
        curr_datetime=curr_datetime.replace("-","")
        bcpexpdir=bcp_export_path
        bcpsplitdir  = bcp_split_path
        filename = msdbname+"_"+msschemaname+"_"+mstablename+"_BCP_"+curr_datetime+".csv"
        bcpfilename = fr"{bcpexpdir}/{filename}"
        bcpsplitpath = fr"{bcpsplitdir}/"
        ##print(f'{bcpsplitpath} -bcpsplitpath')
        # schemaname=f"BCP_SCH_{mstablename}"
        collist =  getcolumnnames(msdbname,mstablename,loadtype,custom_sql)
        # print(type(collist))
        # print(f"collist:{collist}")
        # print(collist[0])
        # print(collist[0][0])
        selsmnt=""
        
        # print("checkpoint 1")
        if cdc_type=='TIMESTAMP':
            cdc_id="NULL"
        elif cdc_type=='ID':
            cdcdates="NULL"
            extract_start_dttm="NULL"
            extract_end_dttm="NULL"
        # print("checkpoint 2")
        else:
            cdc_id="NULL"
            cdcdates="NULL"
            extract_start_dttm="NULL"
            extract_end_dttm="NULL"
    # print("checkpoint 2")
        ##print(msdbname,msschemaname,mstablename,scdtype,loadtype,cdccol,delimiter,filterconditon,trim,encrpt,custom_sql)
        
        
        


        condition=""
            
        if loadtype== 'FULL':
            condition="(1=1)"
            
        
        elif loadtype=='FILTER':
            try:
                # filterconditon=filterconditon.replace("'","''")
                condition="("+filterconditon+")"
            except Exception as g:
                condition="(1=1)"        
        elif loadtype=='INCREMENTAL':
            cdcdates=getcdcdates(msdbname,mstablename,cdc_type)
            # print("cdcdates",cdcdates)
            if len(cdcdates)==0 or  cdcdates[0][0] == None:
                if cdc_type=='TIMESTAMP':
                    cdcdates=['1900-01-01 00:00:00.000','1900-01-01 00:00:00.000']
                elif cdc_type=='ID':
                    cdc_id=0
                    # print(cdc_id)
                    # print("inside cdcdates")
            else:
                if cdc_type=='TIMESTAMP':
                    cdcdates=cdcdates[0]
                    # print(cdcdates,type(cdcdates),cdcdates[0][1])
                elif cdc_type=='ID':
                    cdc_id=int(cdcdates[0][0])
                    print(cdc_id)
            if scdtype==0:
                # print("scd 0",cdccol,type(cdccol))
                # print(cdccol,type(cdccol))
                # print(cdc_type)
                if cdc_type=='TIMESTAMP':
                    # print("Test11111")
                    if str(cdccol)!='None' and len(cdccol)>1:
                        condition="("
                        clms=cdccol.split(",")
                        for clmnitem in range(0,len(clms)):
                            condition=condition+f"""(({clms[clmnitem]} >= TRY_CAST('{cdcdates[0]}' AS DATETIME2)) and ({clms[clmnitem]} < TRY_CAST('{export_start_time}' AS DATETIME2))) OR """
                        condition=condition[:-4]+")"
                    else:
                        condition='(1=1)'
                elif cdc_type=='ID':
                    # print("Testtttttttttt")
                    if str(cdccol)!='None' and len(cdccol)>1:
                        condition="("
                        clms=cdccol.split(",")
                        clmnitem=clms[0]
                        # print(clmnitem, "NAMO NARAYANAN")
                        condition=f"CAST({clmnitem} AS INTEGER) > CAST({cdc_id} AS INTEGER)"
                        # print(condition)
                    else:
                        condition='(1=1)'
                        # print(condition)
            elif scdtype==1:
                ##print("scd 1",cdccol,type(cdccol))
                if str(cdccol)!='None' and len(cdccol)>1:
                    condition="("
                    clms=cdccol.split(",")
                    
                    for clmnitem in range(0,len(clms)):
                        condition=condition+f"""(({clms[clmnitem]} >= TRY_CAST('{cdcdates[0]}' AS DATETIME2)) and ({clms[clmnitem]} < TRY_CAST('{export_start_time}' AS DATETIME2))) OR """
                    condition=condition[:-4]+")"
                else:
                    condition='(1=1)'
            elif scdtype==2:
                ##print("scd 2",cdccol,type(cdccol))
                if str(cdccol)!='None' and len(cdccol)>1:
                    condition="("
                    clms=cdccol.split(",")
                    for clmnitem in range(0,len(clms)):
                        condition=condition+f"""(({clms[clmnitem]} >= TRY_CAST('{cdcdates[0]}' AS DATETIME2)) and ({clms[clmnitem]} < TRY_CAST('{export_start_time}' AS DATETIME2))) OR """
                    condition=condition[:-4]+")"
                else:
                    condition='(1=1)'              
        if loadtype != 'INCREMENTAL':
            if cdc_type=='TIMESTAMP':
                extract_end_dttm = export_start_time
                extract_start_dttm = '1900-01-01 01:01:01.000'
            elif cdc_type=='ID':
                cdc_id = msquery(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {msdbname}.{msschemaname}.{mstablename}")[0][0]
                # extract_start_dttm = 0
        else:
            if cdc_type=='TIMESTAMP':
                extract_end_dttm = export_start_time
                extract_start_dttm = cdcdates[0]
            elif cdc_type=='ID':
                # print(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {msdbname}.{msschemaname}.{mstablename}")
                cdc_id = msquery(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {msdbname}.{msschemaname}.{mstablename}")[0][0]
                # print("cdc_id: ",cdc_id)
                # extract_start_dttm = cdcdates
        

        if custom_sql != None:
            collist
            selsmnt = collist[-1][-1]
            ##print("selsmt-",selsmnt)
            query = selsmnt


            query = query.replace(r'{extract_end_dttm}',f"""CAST('{datetime.strptime(str(extract_end_dttm), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')}' AS DATETIME)""")
            query = query.replace(r'{extract_start_dttm}',f"""CAST('{datetime.strptime(str(extract_start_dttm), '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S')}' AS DATETIME)""")


            bcp_export_query = query
            # print(bcp_export_query)
            # bcp_export_query = bcp_export_query.replace("'","''")
            count_query = f"WITH CTE AS ({custom_sql}) SELECT COUNT(*) from CTE;"
        else:
            # Build a SELECT that encloses every column value in double quotes
            # and escapes any embedded double quotes by doubling them.
            # Cast each column to VARCHAR to avoid type issues.
            # print("in else block of non custom sql column list : ",collist)
            for col in collist:
                # print(type(col))
                # col can be a tuple like ("COLUMN_NAME",)
                # colname = col[0] if isinstance(col, (list, tuple)) else col
                if isinstance(col, (list, tuple)) or type(col).__name__ == 'Row':
                    colname = col[0]
                else:
                    colname = col
                # print(colname)
                # Expression: '"' + REPLACE(COALESCE(CAST(colname AS VARCHAR(6400)), ''), '"', '""') + '"'
                # sel_expr = ("'\"' + REPLACE(COALESCE(CAST(" + colname + " AS VARCHAR(6400)), ''), '\"', '\"\"') + '\"' AS " + colname)
                sel_expr = f"'\\\"' + REPLACE(COALESCE(CAST({colname} AS VARCHAR(6400)), ''), '\\\"', '\\\"\\\"') + '\\\"' AS {colname}"
                selsmnt = selsmnt + sel_expr + ","
                # print(selsmnt)


            selsmnt = "SELECT " + selsmnt
            selsmnt = selsmnt[:-1]
            
            # print(f"selsmnt is {selsmnt}")
            # ##print(type(collist))
        
            bcp_export_query=selsmnt + f" FROM {msdbname}.{msschemaname}.{mstablename} WHERE "+condition+";"
            if loadtype == 'FILTER':
                count_query = f" SELECT COUNT(*) FROM {msdbname}.{msschemaname}.{mstablename} WHERE "+condition+";"
            else:
                count_query = f" SELECT COUNT(*) FROM {msdbname}.{msschemaname}.{mstablename};"
            # print(count_query)
        
        returncode,mssqlcnt = mscount(count_query)
        # print(f"mscount: {mssqlcnt}, returncode : {returncode}")
        log_update('mscount',[returncode,mssqlcnt],batch_id,job_id)
        
        if returncode != 0:
            # print("return code 0")
            returncode,auditstmnt,result=auditupdate(job,extract_start_dttm,extract_end_dttm,batch_id,job_id)
            return mstablename
                        
        ##print(condition)


        # bcp_export_query = f"SELECT  {collist} FROM {msdbname}.{msschemaname}.{mstablename} where {condition} "
        
        
        # print(bcp_export_query)
        # print(f'{bcpfilename} -bcpfilename')
        # ##print(filename)

        returncode=0
        errormsg=" "
        
    
    except Exception as e:
        returncode=1
        errormsg=e
        condition=""
        filename=""
        bcpfilename=""
        export_start_time="" 
        extract_start_dttm= "" 
        extract_end_dttm = ""
        bcp_export_query=""  
        cdc_id=""

    # print("cdc_id :",cdc_id)
    # print("Testtt")
    # print(returncode,errormsg,condition,filename,bcpfilename,export_start_time,extract_start_dttm,extract_end_dttm,bcp_export_query,bcpsplitpath)
    return [returncode,errormsg,condition,filename,bcpfilename,export_start_time,extract_start_dttm,extract_end_dttm,cdc_id,bcp_export_query,bcpsplitpath]




def getcolumnnames(msdbname,mstablename,loadtype,custom_sql):
    if custom_sql is not None: 
        ##print(loadtype,custom_sql,msdbname,mstablename)
        custom_columns = []

        
        select_part = re.search(r'SELECT(.*?)FROM', custom_sql, re.S).group(1)
        # select part:  JOB_ID  ,EMPLOYEE_ID ,START_DATE ,END_DATE ,JOB_TITLE ,SALARY ,DEPARTMENT_ID ,D.DEPARTMENT_NAME ,LOCATION
        remaining_part = custom_sql[custom_sql.index('FROM'):]

        # remaining part FROM JOB_HISTORY JH INNER JOIN DEPARTMENTS D ON JH.DEPARTMENT_ID = D.DEPARTMENT_ID
        columns = re.split(r',\s*(?![^()]*\))', select_part.strip())
        # COLUMNS ['JOB_ID  ', 'EMPLOYEE_ID ', 'START_DATE ', 'END_DATE ', 'JOB_TITLE ', 'SALARY ', 'DEPARTMENT_ID ', 'D.DEPARTMENT_NAME ', 'LOCATION']
        
        col_cnt=0
        rows=[]
        sel_stmnt=""
        for column in columns:
            ###print(column)
            column=column.replace("\t"," ")
            column=column.replace("\n"," ")
            alias_chk_list = column.split(" ")
            col_cnt=col_cnt+1
            while '' in alias_chk_list:
                alias_chk_list.remove('')
            
        
            mssql_data_types = [ "BIT", "TINYINT", "SMALLINT", "INT", "BIGINT", "DECIMAL", "NUMERIC",
                                "MONEY", "SMALLMONEY", "FLOAT", "REAL", "DATE", "DATETIME", "DATETIME2", "SMALLDATETIME", 
                                "DATETIMEOFFSET", "TIME", "CHAR", "NCHAR", "VARCHAR", "NVARCHAR", "TEXT", "NTEXT",
                                "BINARY", "VARBINARY", "IMAGE", "XML", "UNIQUEIDENTIFIER", "SQL_VARIANT", "CURSOR", "TABLE"]           
            key_exception=0

            for j in mssql_data_types:
                if j in alias_chk_list[-1]:
                    key_exception = 1
            if len(alias_chk_list)>2 and alias_chk_list[-2] == 'AS' and key_exception==0:
                ###print(alias_chk_list[-1])
                ##print(column)
                column = column[:column.rindex('AS')]

            ##print(column)
            custom_columns.append(column.strip())
            column = column.strip()
            column = f"CAST({column} AS VARCHAR(6400)) AS COLUMN_{col_cnt}"
            # JOB_ID
# CAST(JOB_ID AS VARCHAR(6400)) AS COLUMN_1
# EMPLOYEE_ID
# CAST(EMPLOYEE_ID AS VARCHAR(6400)) AS COLUMN_2
# START_DATE
# CAST(START_DATE AS VARCHAR(6400)) AS COLUMN_3

            ##print(column)
            sel_stmnt = sel_stmnt + ',' + column + ' '
            # ,CAST(JOB_ID AS VARCHAR(6400)) AS COLUMN_1 ,CAST(EMPLOYEE_ID AS VARCHAR(6400)) AS COLUMN_2 ,....
            rows.append([msdbname,mstablename,col_cnt,'COLUMN_'+str(col_cnt),'VARCHAR(12800)',column.strip(),''])
            # ['DEMO_DB', 'DEPARTMENTS_JOBHISTORY', 1, 'COLUMN_1', 'VARCHAR(12800)', 'CAST(JOB_ID AS VARCHAR(6400)) AS COLUMN_1', '']
        
        if select_part.strip() == '*':
            sel_stmnt = custom_sql

        else:
            sel_stmnt = "SELECT "+sel_stmnt[1:] +" "+ remaining_part

        
        rows[-1][-1]=sel_stmnt
        ##print("rows-",rows)
        return rows
    
    else:
        # print("into else get column names")
        query = f"""select COLUMN_NAME from {msdbname}.INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{mstablename}'"""
        # print(query)
        result = msquery(query)
        # result = [list(row) for row in result]
        # print(f"result is {result}")
        # print(type(result))
        return result
        # ##print(f"after list {result}")
        # selstmt = ""

        # for items in result:
        #     selstmt = selstmt + items[0] + ','
           

        # selstmt = selstmt[:-1]
        # ##print(selstmt)
        # ##print(type(selstmt))
        # # return selstmt
        # return [item for item in result]




def bcp_export(msdbname,msschemaname,mstablename,bcpfilename,bcp_export_query):
    with open('C:/Users/palanivelu.murug/Documents/Datamigration_gzip_while_export_2026_04_03/credentials.json','r+') as config_file:
            cred=json.load(config_file)

    server_name = cred['server_name']
    sqlsvr_user = cred['sqlsvr_user']
    sqlsvr_password = cred['sqlsvr_password']
    bcp_export_path = cred['bcp_export_path']



    start = time.time() 
    bcp_cmd=f"""bcp "{bcp_export_query}" queryout "{bcpfilename}" -S {server_name} -U {sqlsvr_user} -P {sqlsvr_password} -t"," -c"""
    # print(f"bcp_cmd : {bcp_cmd}")

    t=subprocess.run(bcp_cmd, shell=True, capture_output=True, text=True)
    # print(f"completed in {time.time() - start:.2f} seconds.")

    #print(t.returncode)
    #print(t.stdout)
    #print(t.stderr)
    

    
    return [t.returncode,bcp_cmd,t.stdout]




def split_file_into_chunks(bcpfilename, bcpsplitpath,msdbname,mstablename ): 
    chunk_file_name = ""
    chunk_file_path = ""
    with open('C:/Users/palanivelu.murug/Documents/Datamigration_gzip_while_export_2026_04_03/credentials.json','r+') as config_file:
        cred=json.load(config_file)
        chunk_size = cred['chunk_size']
        # print(type(chunk_size))
        chunk_size = int(chunk_size)
        # print(type(chunk_size))
    try:
        start = time.time() 
        # chunk_size= 1024* 1024 * 1024
        file_size = os.path.getsize(bcpfilename)
        # print(f"Total file size: {file_size / chunk_size:.2f} GB")
        
        num_chunks = (file_size + chunk_size - 1) // chunk_size
        # print(f"Total chunks: {num_chunks}")
        
        base_filename = os.path.splitext(os.path.basename(bcpfilename))[0]
        file_index = 1  
        
        # header_str = getcolumnnames(msdbname, mstablename) 
        # header_line = (header_str + "\n").encode("utf-8")   # Convert to bytes for binary write

        with open(bcpfilename, 'rb') as f:
            chunk_file_name = ''
            chunk_file_path = ' '
            while True:
                start_pos = f.tell()
                #print(f"start position -{start_pos}")

                chunk = f.read(chunk_size)

                if not chunk:
                    break

                last_newline = chunk.rfind(b'\n')

                if last_newline == -1:

                    data = chunk
                    f.seek(start_pos + len(chunk))  #
                    #print(f'{f.seek(start_pos + len(chunk))} - seek start_pos + len(chunk)')
                else:
                    data = chunk[:last_newline + 1]
                    # Move file pointer to just after the last newline
                    f.seek(start_pos + last_newline + 1)
                    #print(f'{f.seek(start_pos + last_newline + 1)}-seek(start_pos + last_newline + 1')



                chunk_file_name = f"{base_filename}_part{file_index}.csv.gz"
                chunk_file_path = os.path.join(bcpsplitpath, chunk_file_name)

                # with open(chunk_file_path, 'wb') as chunk_file:
                #     chunk_file.write(data)
                # print(f"chunk_file_path : {chunk_file_path}")
                with gzip.open(chunk_file_path, 'wb') as chunk_file:
                    chunk_file.write(data)


                # Log progress
                # print(f"Written: {chunk_file_name} ({len(data) / (1024 * 1024):.2f} MB)")
                file_index += 1  

        #print(f"completed in {time.time() - start:.2f} seconds.")
        returncode=0
        errormsg=" "

    except Exception as e:
        #print("SRINIVASA")
        returncode=1
        errormsg=e
    
    return [returncode,errormsg,chunk_file_name,chunk_file_path]






