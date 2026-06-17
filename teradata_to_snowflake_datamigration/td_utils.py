# -*- coding: utf-8 -*-
"""
Created on Wed Dec 25 16:48:38 2024

@author: manga
"""

import teradatasql
import pandas as pd
import json
import re 


def getcolumninfo(databasename,tablename,loadtype,custom_sql):
    if custom_sql != None: 
        print(loadtype,custom_sql,databasename,tablename)
        custom_columns = []
        '''
        custom_sql = r"""
                SELECT C1 AS vss, concat(A.PS_SUPPKEY,B.PS_SUPPKEY), CAST(A.PS_PARTKEY AS VARCHAR(64000))      AS   C2, concat(A.PS_SUPPKEY,B.PS_SUPPKEY) AS C4, 
                    COALESCE(CAST(B.PS_SUPPKEY AS VARCHAR(64000)), 'GOVINDA') AS VARADHA), 
                    CAST('A.PS_AVAILQTY' AS VARCHAR(64000)) AS din,CAST(B.PS_SUPPLYCOST AS VARCHAR(64000))   AS     dd   , 
                    SUBSTR(CAST(A.LOAD_DTTM AS VARCHAR(64000)),2) AS GOV,C11,C1 
                FROM (SELECT * FROM ORDERS D) A 
                JOIN (SELECT * FROM CUSTOMER Z) B ON A.LOAD_DTTM = B.LOAD_DTTM
                """
        custom_sql = r"""
                SELECT CAST(B.PS_SUPPKEY AS VARCHAR(64000)),
                CAST(concat(A.PS_SUPPKEY,'HC') AS VARCHAR(64000)) AS COLUMN_2 ,
                CAST(CAST(A.PS_PARTKEY AS VARCHAR(64000)) AS VARCHAR(64000)) AS COLUMN_3 ,
                CAST(concat(A.PS_SUPPKEY,'JNJ') AS VARCHAR(64000)) AS COLUMN_4 

                FROM (SELECT * FROM ORDERS D) A 
                                JOIN (SELECT * FROM CUSTOMER Z) B ON A.LOAD_DTTM = B.LOAD_DTTM"""
                        
        custom_sql = r"""
                SELECT PS_AVAILQTY , PS_SUPPKEY , PS_SUPPLYCOST , PS_AVAILQTY AS C1 , PS_SUPPKEY AS C3  FROM CUSTOMER"""
        '''
        
        select_part = re.search(r'SELECT(.*?)FROM', custom_sql, re.S).group(1)
        remaining_part = custom_sql[custom_sql.index('FROM'):]
        print(remaining_part)
        columns = re.split(r',\s*(?![^()]*\))', select_part.strip())
        col_cnt=0
        rows=[]
        sel_stmnt=""
        print(select_part)
        for column in columns:
            #print(column)
            column=column.replace("\t"," ")
            column=column.replace("\n"," ")
            alias_chk_list = column.split(" ")
            col_cnt=col_cnt+1
            while '' in alias_chk_list:
                alias_chk_list.remove('')
            
            
            print("Rama", column)
            teradata_data_types = [
                "BYTEINT", "SMALLINT", "INTEGER", "BIGINT", "DECIMAL", "NUMERIC", "FLOAT", "REAL", "DOUBLE PRECISION",
                "CHAR", "CHARACTER", "VARCHAR", "CHARACTER VARYING",
                "DATE", "TIME", "TIMESTAMP", "INTERVAL",
                "BYTE", "VARBYTE",
                "BLOB", "CLOB",
                "PERIOD(DATE)", "PERIOD(TIME)", "PERIOD(TIMESTAMP)"]
            
            key_exception=0

            for j in teradata_data_types:
                if j in alias_chk_list[-1]:
                    key_exception = 1
            if len(alias_chk_list)>2 and alias_chk_list[-2] == 'AS' and key_exception==0:
                #print(alias_chk_list[-1])
                print(column)
                column = column[:column.rindex('AS')]

            print(column)
            custom_columns.append(column.strip())
            column = column.strip()
            column = f"CAST({column} AS VARCHAR(6400)) AS COLUMN_{col_cnt}"
            print(column)
            sel_stmnt = sel_stmnt + ',' + column + ' '
            rows.append([databasename,tablename,col_cnt,'COLUMN_'+str(col_cnt),'VARCHAR(12800)',column.strip(),''])
        
        if select_part.strip() == '*':
            sel_stmnt = custom_sql

        else:
            sel_stmnt = "SELECT "+sel_stmnt[1:] +" "+ remaining_part

        print(sel_stmnt)
        rows[-1][-1]=sel_stmnt
        return rows

    else:
        with open('/media/ssd/python/credentials.json','r+') as config_file:
            cred=json.load(config_file)

        td_host = cred['td_host']
        td_user = cred['td_user']
        td_password = cred['td_password']

        tdcon=teradatasql.connect(
            user=td_user,
            password=td_password,
            host=td_host)
        
 
        cur=tdcon.cursor()
        sql="""    
        SELECT Coalesce(Trim(DATABASENAME),''), Coalesce(Trim(TABLENAME),''), 
        TRIM(ROW_NUMBER() OVER(PARTITION BY   TABLENAME ,  DATABASENAME ORDER BY COLUMNID )), 
        Coalesce(Trim(COLUMNNAME),''), Trim(Coalesce(COLUMN_DATATYPE,''))
        ,Coalesce(Trim(ColumnLength),''),Coalesce(Trim(ColumnFormat),'') FROM
        (
        select c.tablename ,  c.DATABASENAME , c.COLUMNNAME, c.ColumnLength , c.ColumnFormat ,  CASE c.ColumnType
            WHEN 'BF' THEN 'BYTE('            || TRIM(ColumnLength (FORMAT '-(9)9')) || ')'
            WHEN 'BV' THEN 'VARBYTE('         || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'CF' THEN 'CHAR('            || TRIM( CASE WHEN ColumnLength * 2 > 64000 THEN 64000 ELSE ColumnLength * 2 END (FORMAT 'Z(9)9')) || ')'
            WHEN 'CV' THEN 'VARCHAR('         || TRIM( CASE WHEN ColumnLength * 2 > 64000 THEN 64000 ELSE ColumnLength * 2 END (FORMAT 'Z(9)9')) || ')'
            WHEN 'D ' THEN 'DECIMAL('         || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DA' THEN 'INTDATE' /* DATE WAS HERE*/
            WHEN 'F ' THEN 'FLOAT'
            WHEN 'I1' THEN 'BYTEINT'
            WHEN 'I2' THEN 'SMALLINT'
            WHEN 'I8' THEN 'BIGINT'
            WHEN 'I ' THEN 'INTEGER'
            WHEN 'AT' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'TS' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'TZ' THEN 'TIME('            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
            WHEN 'SZ' THEN 'TIMESTAMP('       || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE'
            WHEN 'YR' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'YM' THEN 'INTERVAL YEAR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MONTH'
            WHEN 'MO' THEN 'INTERVAL MONTH('  || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DY' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'DH' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO HOUR'
            WHEN 'DM' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
            WHEN 'DS' THEN 'INTERVAL DAY('    || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'HR' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'HM' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO MINUTE'
            WHEN 'HS' THEN 'INTERVAL HOUR('   || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'MI' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'MS' THEN 'INTERVAL MINUTE(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ')'      || ' TO SECOND('
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'SC' THEN 'INTERVAL SECOND(' || TRIM(DecimalTotalDigits (FORMAT '-(9)9')) || ','
                                            || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')'
            WHEN 'BO' THEN 'BLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'CO' THEN 'CLOB('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
        
            WHEN 'PD' THEN 'PERIOD(DATE)'
            WHEN 'PM' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
            WHEN 'PS' THEN 'PERIOD(TIMESTAMP('|| TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
            WHEN 'PT' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || '))'
            WHEN 'PZ' THEN 'PERIOD(TIME('     || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) || ')' || ' WITH TIME ZONE)'
            WHEN 'UT' THEN COALESCE(ColumnUDTName,  '<Unknown> ' || ColumnType)
        
            WHEN '++' THEN 'TD_ANYTYPE'
            WHEN 'N'  THEN 'NUMBER('          || CASE WHEN DecimalTotalDigits = -128 THEN '*' ELSE TRIM(DecimalTotalDigits (FORMAT '-(9)9')) END
                                            || CASE WHEN DecimalFractionalDigits IN (0, -128) THEN '' ELSE ',' || TRIM(DecimalFractionalDigits (FORMAT '-(9)9')) END
                                            || ')'
            WHEN 'A1' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
            WHEN 'AN' THEN COALESCE('SYSUDTLIB.' || ColumnUDTName,  '<Unknown> ' || ColumnType)
        
            WHEN 'JN' THEN 'JSON('            || TRIM(ColumnLength (FORMAT 'Z(9)9')) || ')'
            WHEN 'VA' THEN 'TD_VALIST'
            WHEN 'XM' THEN 'XML'
        
            ELSE '<Unknown> ' || ColumnType
        END  COLUMN_DATATYPE ,  INDEXTYPE,
        CASE INDEXTYPE
        WHEN  'P'     then 'Nonpartitioned primary index'
        WHEN  'Q'     then 'Partitioned primary index'
        WHEN  'S'     then 'Secondary index'
        WHEN  'J'     then 'n index'
        WHEN  'N'    Then 'Hash index'
        WHEN  'K'     then 'Primary key'
        WHEN  'U'     then 'Unique constraint'
        WHEN  'V'     then 'Value-ordered secondary index'
        WHEN  'H'     then 'Hash-ordered ALL covering secondary index'
        WHEN  'O'     then 'Valued-ordered ALL covering secondary index'
        WHEN  'I'      then 'dering column of a composite secondary index'
        WHEN  'G'     then 'Geospatial non-unique secondary index.'
        when 'M'	  then 'Multi column statistics'
        when 'D'	     then 'Derived column partition statistics'
        when '1'    	then 'field1 column of a join or hash index'
        when '2'	    then ' field2 column of a join or hash index'
        END INDEX_TYPE_NAME  ,
        ColumnPosition ,IndexNumber ,
        PartitioningColumn
        , CASE
                WHEN ColumnType IN ('CV', 'CF', 'CO')
                THEN CASE CharType
                        WHEN 1 THEN ' CHARACTER SET LATIN'
                        WHEN 2 THEN ' CHARACTER SET UNICODE'
                        WHEN 3 THEN ' CHARACTER SET KANJISJIS'
                        WHEN 4 THEN ' CHARACTER SET GRAPHIC'
                        WHEN 5 THEN ' CHARACTER SET KANJI1'
                        ELSE ''
                    END
                ELSE ''
            END STRING_TYPE ,COLUMNID
        
        from DBC.columnsV  c
        left join    DBC.IndicesV  i   on   c.tablename=i.tablename AND c.DATABASENAME=i.DATABASENAME  and c.COLUMNNAME=i.COLUMNNAME
        where upper(C.tablename)=upper('{}')
        AND upper(C.DATABASENAME)=upper('{}')
        ) a;
            """.format(tablename, databasename)
        cur.execute(f"{sql}")
        result=cur.fetchall()
        #print(sql)
        return result
    #return [databasename,tablename]

def tdquery(query):
    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)

    td_host = cred['td_host']
    td_user = cred['td_user']
    td_password = cred['td_password']

    tdcon=teradatasql.connect(
        user=td_user,
        password=td_password,
        host=td_host)

    cur=tdcon.cursor()
    sd=cur.execute(query)
    result=sd.fetchall()
    print(result)
    return result

def tdcount(stdout):
    #query2=f"SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR(26));"
    #export_start_time=tdquery(query2)[0][0]
    '''
    try:
        if custom_sql != None:
            print('RANGA',custom_sql,'RANGA')
            print(len(custom_sql))
            core_statement = custom_sql[custom_sql.index('FROM'):]
            core_statement = core_statement.replace(r'{extract_end_dttm}','1900-01-01 00:00:00.000')
            core_statement = core_statement.replace(r'{extract_start_dttm}','2200-01-01 00:00:00.000')
            query1=f"SELECT COUNT(*) {core_statement}"
            
            print(loadtype)
            print(query1)
        else:
            query1=f"SELECT COUNT(*) FROM {tddbname}.{tdtablename};"
        tdcnt=tdquery(query1)[0][0]
        returncode=0
    except Exception as e:
        returncode=1
        tdcnt=str(e)
        

    #return [returncode,tdcnt,export_start_time]
    return [returncode,tdcnt]
    '''
    try:
        out = str(stdout)
        cnt_line = out.index('Total Rows Exported: ')
        out_half = out[cnt_line+21:]
        tdcnt = out_half[:out_half.index('\n')]
        tdcnt = tdcnt.strip()
        returncode = 0

    except Exception as e:
        returncode=1
        tdcnt=str(e)
    return [returncode,tdcnt]
