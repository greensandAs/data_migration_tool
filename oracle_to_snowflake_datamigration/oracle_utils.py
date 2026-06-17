import cx_Oracle
import csv
import io
import time
from datetime import datetime
import json
import math
import sys
from logger import batch_create,log_update
from sf_utils import getcdcdatesfororacle

def oracle_conn():
    with open('C:/Users/palanivelu.murug/Documents/Datamigration_oracle_final_version - 1215/Datamigration/credentials.json','r+') as config_file:
        cred=json.load(config_file)
        # Database connection details
        oraclehost= cred['oracle_host']
        oracleport= cred['oracle_port']
        oracleservicename= cred['oracle_servicename']
        oracleusername= cred['oracle_user']
        oraclepassword= cred['oracle_password']
        dsn_tns = cx_Oracle.makedsn(oraclehost, oracleport, service_name=oracleservicename)
        conn = cx_Oracle.connect(user=oracleusername, password=oraclepassword, dsn=dsn_tns)
        return conn

def close_oracle_conn(cursor,conn):
    # #print("test")
    if cursor:
        cursor.close()
    if conn:
        conn.close()
    
def oracle_query(query):
    conn = oracle_conn()
    # print(f"Oracle Query : {query}")
    # Execute the query
    cursor = conn.cursor()
    cursor.execute ("ALTER SESSION ENABLE PARALLEL DML")
    cursor.execute(query)

    # Fetch column names
    columns = [desc[0] for desc in cursor.description] 
    return columns,cursor,conn

def oracle_count(query):
    try:       
        columns,cursor,conn=oracle_query(query)
        oraclecount = cursor.fetchall()[0][0]
        close_oracle_conn(cursor,conn)
        returncode=0
    except Exception as e:
        returncode=1
        oraclecount=str(e)
    return returncode,oraclecount


def save_csv(data, headers, filename):
    with open('C:/Users/palanivelu.murug/Documents/Datamigration_oracle_final_version - 1215/Datamigration/credentials.json','r+') as config_file:
        cred=json.load(config_file)
        # Database connection details
        oracleexportpath= cred['oracle_export_path']
    with open(f'{oracleexportpath}/{filename}', "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file)
        writer.writerow(headers)  # Write column headers
        writer.writerows(data)  # Write batch of rows

    # #print(f"Saved: {filename} ({sys.getsizeof(data)} bytes)")

def oracle_export(schema_name, table_name,job):
    schema_name=job[0]
    table_name=job[1]
    scdtype=job[6]
    loadtype=job[7]
    cdccol=job[8]
    delimiter=job[10]
    filterconditon=job[11]
    trim=job[12]
    encrypt=job[13]
    custom_sql = job[17]
    exportedfilenames = []
    job_id=job[16]
    cdc_type = job[19]
    # print("cdc_type")
    # print(cdc_type)
    batch_id=job[15]
    exportedfilename = ""

    #Get Export Start time
    current_timestamp_query = f"SELECT TO_CHAR(current_timestamp, 'YYYY-MM-DD HH:MI:SS AM') FROM dual"
    timestamp_column,cursor,conn = oracle_query(current_timestamp_query)
    current_timestamp = cursor.fetchall()[0][0]
    # curr_datetime = str(current_timestamp)[:16]
    curr_datetime = str(current_timestamp)
    export_start_time=curr_datetime
    curr_datetime=curr_datetime[:16].replace(" ","_")
    curr_datetime=curr_datetime.replace(":","")
    curr_datetime=curr_datetime.replace("-","")
    close_oracle_conn(cursor,conn)

    # print("checkpoint 1")
    if cdc_type=='TIMESTAMP':
        cdc_id="NULL"
    elif cdc_type=='ID':
        cdcdates="NULL"
        extract_start_dttm="NULL"
        extract_end_dttm="NULL"
    else:
        cdc_id="NULL"
        cdcdates="NULL"
        extract_start_dttm="NULL"
        extract_end_dttm="NULL"
    # print("checkpoint 2")

    if loadtype=='FULL':
        condition="(1=1)"
    elif loadtype=='FILTER':
        condition="("+filterconditon+")"
    elif loadtype=='INCREMENTAL':
        cdcdates=getcdcdatesfororacle(schema_name,table_name,cdc_type)
        #print(f"CDCDATES :  {cdcdates}")
        if len(cdcdates) == 0 or cdcdates[0][0] == None:
            if cdc_type=='TIMESTAMP':
                #print(f"CDCDATES : {cdcdates}")
                date_str = "01-01-1900"

                # Convert to datetime object 
                dt = datetime.strptime(date_str, "%d-%m-%Y")

                # Format with time included
                # formatted_time = dt.strftime("%d-%m-%Y %I:%M:%S %p")
                formatted_time = dt.strftime("%Y-%m-%d %I:%M:%S %p")
                # formatted_time = dt.strftime("%Y-%m-%d %H:%M:%S")
                cdcdates=[formatted_time,formatted_time]
                #print(f"CDCDATE: {cdcdates[0]}")
            elif cdc_type=='ID':
                cdc_id=0
                # print(cdc_id)
                # print("inside cdcdates")
        else:
            if cdc_type=='TIMESTAMP':
                # #print(f"TESSSSSSidsopfiSSF {cdcdates[0]}")
                cdcdates = cdcdates[0]
                # #print(f"testttt time : {date_str}")

                # # Convert to datetime object 
                # dt = datetime.strptime(date_str, "%d-%m-%Y %H:%M:%S")

                # # Format with time included
                # cdcdates = dt.strftime("%d-%m-%Y %I:%M:%S %p")
                # print(cdcdates,type(cdcdates),cdcdates[0][1])
            elif cdc_type=='ID':
                cdc_id=int(cdcdates[0][0])
                # print(cdc_id)
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
                        condition=condition+f"""(({clms[clmnitem]} >= TO_TIMESTAMP('{datetime.strptime(str(cdcdates[0]), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM')) and ({clms[clmnitem]} < TO_TIMESTAMP('{datetime.strptime(str(export_start_time), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM'))) OR """
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
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            #print("cdccol : ",cdccol,type(cdccol))
        if scdtype==1:
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            #print("cdccol : ",cdccol,type(cdccol))
            
            if str(cdccol)!='None' and len(cdccol)>1:
                condition="("
                clms=cdccol.split(",")
                
                for clmnitem in range(0,len(clms)):
                    condition=condition+f"""(({clms[clmnitem]} >= TO_TIMESTAMP('{datetime.strptime(str(cdcdates[0]), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM')) and ({clms[clmnitem]} < TO_TIMESTAMP('{datetime.strptime(str(export_start_time), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM'))) OR """
                condition=condition[:-4]+")"

            else:
                condition='(1=1)'
        
        if scdtype==2:
            #auditcondition=getcolumninfo
            #condition=f"({cdc}>'2024-12-25 23:08:45')"
            #print("cdccol : ",cdccol,type(cdccol))
            
            if str(cdccol)!='None' and len(cdccol)>1:
                condition="("
                clms=cdccol.split(",")
                #print(f"clms : {clms}")
                
                for clmnitem in range(0,len(clms)):
                    condition=condition+f"""(({clms[clmnitem]} >= TO_TIMESTAMP('{datetime.strptime(str(cdcdates[0]), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM')) and ({clms[clmnitem]} < TO_TIMESTAMP('{datetime.strptime(str(export_start_time), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM'))) OR """
                condition=condition[:-4]+")"

            else:
                condition='(1=1)'
    
    if loadtype != 'INCREMENTAL':
        if cdc_type=='TIMESTAMP':
            extract_end_dttm = export_start_time
            date_str = "01-01-1900"
            dt = datetime.strptime(date_str, "%d-%m-%Y")

            # Format with time included
            # formatted_time = dt.strftime("%d-%m-%Y %I:%M:%S %p")
            formatted_time = dt.strftime("%Y-%m-%d %I:%M:%S %p")
            # Convert to datetime object 
            extract_start_dttm = formatted_time #datetime.strptime(date_str, "%Y-%m-%d %I:%M:%S %p")
        elif cdc_type=='ID':
            cdc_id = oracle_query(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {schema_name}.{table_name}")[1].fetchall()[0][0]
            # extract_start_dttm = 0
    else:
        if cdc_type=='TIMESTAMP':
            extract_end_dttm = export_start_time
            extract_start_dttm = cdcdates[0]
        elif cdc_type=='ID':
            # print(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {schema_name}.{table_name}")
            # print(oracle_query(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {schema_name}.{table_name}")[1].fetchall()[0][0])
            cdc_id = oracle_query(f"SELECT MAX(CAST({cdccol} AS INTEGER)) FROM {schema_name}.{table_name}")[1].fetchall()[0][0]
            # print("cdc_id: ",cdc_id)
            # extract_start_dttm = cdcdates
    
    if custom_sql != None:
        #print(loadtype)
        query = custom_sql
        # collist
        # selsmnt = collist[-1][-1]
        query = query.replace(r'{extract_end_dttm}',f"""TO_TIMESTAMP('{datetime.strptime(str(extract_end_dttm), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM')""")
        query = query.replace(r'{extract_start_dttm}',f"""TO_TIMESTAMP('{datetime.strptime(str(extract_start_dttm), "%Y-%m-%d %I:%M:%S %p").strftime("%d-%m-%Y %I:%M:%S %p")}', 'DD-MM-YYYY HH:MI:SS AM')""")

        countquery=f"WITH CTE AS ({custom_sql}) SELECT COUNT(*) from CTE"
        returncode,oraclecount=oracle_count(countquery)
        log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)

        # tpt_extract_query = selsmnt
        # tpt_extract_query = tpt_extract_query.replace("'","''")
    else:
        # count_query = f"SELECT count(*) FROM {schema_name}.{table_name} WHERE {condition}"
        if loadtype != 'INCREMENTAL':
            countquery=f"SELECT COUNT(*) FROM {schema_name}.{table_name} WHERE {condition}"
        else:
            countquery=f"SELECT COUNT(*) FROM {schema_name}.{table_name}"            
        query = f"SELECT /*+ parallel(4) */* FROM {schema_name}.{table_name} WHERE {condition}"
        
        returncode,oraclecount=oracle_count(countquery)
        #print(f"Incremental queries: {query} Count Query: {countquery}")
        log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)
    # #print(query)
    
    if loadtype=='FULL':
        if custom_sql == None:
            byte_size_query = f"SELECT bytes from user_segments where segment_name='{table_name.upper()}'"
            byte_size_columns,cursor,conn = oracle_query(byte_size_query)
            byte_size = cursor.fetchall()
            byte_size = byte_size[0][0]
            # #print(byte_size)
            close_oracle_conn(cursor,conn)
            returncode=0
            approx_chunks = math.ceil(byte_size/1073741824)
            # returncode,oraclecount=oracle_count(schema_name,table_name,condition,custom_sql)
            # log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)
            # count_query = f"SELECT count(*) FROM {schema_name}.{table_name}  WHERE {condition}"
            # count_columns,cursor,conn = oracle_query(count_query)
            # count = cursor.fetchall()
            # close_oracle_conn(cursor,conn)
            returncode=0
            # print(type(oraclecount))
            # print(type(approx_chunks))
            
            # print(oraclecount)
            # print(approx_chunks)
            approx_fetch_size = (oraclecount//approx_chunks)
        else:
            byte_size = 0

        # Run the select query and save the result set to csv
        try:
            columns,cursor,conn = oracle_query(query)
            exportedfilename=f"{schema_name}_{table_name}_ORACLE_{curr_datetime}.csv"
            file_number = 1
            if byte_size <= 1073741824:
                result= cursor.fetchall()
                # print(result)
                close_oracle_conn(cursor,conn)
                exportfilename=f"{schema_name}_{table_name}_ORACLE_{curr_datetime}.csv"
                save_csv(result, columns, exportfilename)
                exportedfilenames.append(exportfilename)
                # print("checkpoint 1")
            else:
                while True:
                    result = cursor.fetchmany(approx_fetch_size)
                    exportfilename = f"{schema_name}_{table_name}_ORACLE_{curr_datetime}_{file_number}.csv"
                    exportedfilenames.append(exportfilename)
                    if not result:
                        close_oracle_conn(cursor,conn)
                        break
                    save_csv(result[0:], columns, exportfilename)
                    file_number += 1
            returncode=0
            returnmessage = query
        except Exception as e:
            returncode=1
            returnmessage=str(e)
    else:
        try:
            # #print(query)
            # returncode,oraclecount=oracle_count(schema_name,table_name,condition)
            # log_update('oraclecount',[returncode,oraclecount],batch_id,job_id)
            columns,cursor,conn = oracle_query(query)
            exportedfilename=f"{schema_name}_{table_name}_ORACLE_{curr_datetime}.csv"
            result= cursor.fetchall()
            close_oracle_conn(cursor,conn)
            exportfilename=f"{schema_name}_{table_name}_ORACLE_{curr_datetime}.csv"
            save_csv(result, columns, exportfilename)
            exportedfilenames.append(exportfilename)
            returncode=0
            returnmessage = query
        except Exception as e:
            returncode=1
            returnmessage=str(e)
    
    return returncode,returnmessage,exportedfilenames,exportedfilename,extract_start_dttm,extract_end_dttm,cdc_id
