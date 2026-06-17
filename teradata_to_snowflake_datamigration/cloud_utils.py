import subprocess
import os
import glob
import json
from datetime import datetime

#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_QWE_TPT_20250208_2235')

def s3upload(s3_path,filename):

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    tpt_export_path = cred['tpt_export_path']

    cmd=f"""aws s3 cp '{tpt_export_path}/' '{s3_path}{filename}/' '--recursive' '--exclude' '*' '--include' '*{filename}*'"""

    print(cmd)
    t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(t.returncode)
    #print(t.stdout)
    if t.returncode==0:
        log=t.stdout
        print(t.stderr)
        


        uploaded_files_txt=""
        uploaded_files=[]
        uploaded_log=log.split('\n')
        for i in uploaded_log:
            #print("DHAMODAHARA")

            if 'upload:' in i:
                uploaded_files_txt=uploaded_files_txt+'\n'+i
                uploaded_files.append(i)
        header=f"Number of files uploaded: {len(uploaded_files)}"
  

        s3_log=f"{header} \n{uploaded_files_txt}"

        print(s3_log)
        return [t.returncode,cmd,s3_log]

    else:
        print("")
        print(t.stderr)
        print(t.stdout)
        return [t.returncode,cmd,t.stderr]

#s3upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_QWE_TPT_20250208_2235')



def azupload(az_path,filename):

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    tpt_export_path = cred['tpt_export_path']

    azcopy_app = r'/media/ssd/azcopy_linux_amd64_10.28.0/azcopy'
    sas_token = "?sp=racwdli&st=2026-04-26T09:05:11Z&se=2030-01-12T17:20:11Z&sv=2025-11-05&sr=c&sig=P9UW%2ByqlpvVhRKFISGc5xwRXHw2Jl%2FL0m%2FVOQjeZqMg%3D"


    cmd=f"""{azcopy_app} cp '{tpt_export_path}/*{filename}*' '{az_path}{filename}/{sas_token}' --recursive """

    print(cmd)
    t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
    print(t.returncode)
    print(t.stdout)
    # 
    
    if t.returncode==0:
        out_log=t.stdout

        out_log=out_log.split("\n")
        log_file_name=""
        for i in range(0,len(out_log)):
            if 'Log file is located at:' in out_log[i]:
                print(out_log[i])
                log_file=out_log[i].strip()
                log_file_name=log_file[log_file.index('at:')+4:]
                break

        with open(rf'{log_file_name}') as lf:
            log_content=str(lf.read())
            print(log_content)
        
        log_content_list=[]
        log_content_list=log_content.split("\n")
        
        uploaded_csv_files=''
        csv_cnt=0
        for i in range(0,len(log_content_list)):

            if 'Starting transfer: Source' in log_content_list[i]:
                file = log_content_list[i]
                csv_name = file[file.index('Source')+7:file.index('Destination')] 
                print(log_content_list[i])
                print(csv_name)
                csv_cnt=csv_cnt+1
                uploaded_csv_files = uploaded_csv_files + '\n' + csv_name
            


        csv_cnt_header="No Of File Uploaded : "+str(csv_cnt)

        print(uploaded_csv_files)
        print(csv_cnt_header)
        
        az_log = csv_cnt_header + '\n' + uploaded_csv_files

        print(az_log)

        return [t.returncode,cmd,az_log]

    else:
        print("")
        print(t.stderr)
        print(t.stdout)
        return [t.returncode,cmd,t.stdout]


def cloud_upload(cloud_path,filename):
    print(cloud_path)
    if r's3://' in cloud_path:
        cloud_returns = s3upload(cloud_path,filename)
        
    elif r'blob.core.windows.net' in cloud_path:
        cloud_path=cloud_path.replace("azure","https")
        cloud_returns = azupload(cloud_path,filename)
    
    else:
        cloud_returns = [1,'Invalid cloud path', filename]
    
    return cloud_returns

#print(cloud_upload('s3://tdsfbucket/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_SERVICE_TPT_20250303_0536'))

#print(cloud_upload('azure://snowflaketeradata213.blob.core.windows.net/teradataexport/TDEXPORT/DATAMIGRATION/DEMO_USER/','DEMO_USER_SERVICE_TPT_20250303_0536'))



def az_archive(az_path,filename,batch_id):

    with open('/media/ssd/python/credentials.json','r+') as config_file:
        cred=json.load(config_file)
    
    tpt_export_path = cred['tpt_export_path']
    archive_path = cred['archive_path']
    az_path=az_path.replace("azure","https")
    azcopy_app = r'/media/ssd/azcopy_linux_amd64_10.28.0/azcopy'
    sas_token = "?sp=racwdli&st=2026-04-26T09:05:11Z&se=2030-01-12T17:20:11Z&sv=2025-11-05&sr=c&sig=P9UW%2ByqlpvVhRKFISGc5xwRXHw2Jl%2FL0m%2FVOQjeZqMg%3D"
    current_date = datetime.now()
    current_date = current_date.strftime("%Y%m%d")
    archive_path = archive_path + current_date + "/" + str(batch_id) +"/"
    try:
        cmd=f"""{azcopy_app} cp '{az_path}{filename}/{sas_token}' '{archive_path}/{sas_token}'  --recursive """

        print(cmd)
        t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(t.returncode)
        print(t.stdout)
        cmd=f"""{azcopy_app} rm '{az_path}{filename}/{sas_token}' """

        print(cmd)
        t=subprocess.run(cmd, shell=True, capture_output=True, text=True)
        print(t.returncode)
        print(t.stdout)

    except Exception as e:
        print(e)
        return [1,str(e)]
    return [t.returncode,t.stdout]
