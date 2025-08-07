######Scipt created date3/1/3024


##########################################################


import zipfile
import os
import shutil
import json
import re
import pandas as pd
import numpy as np
import glob
import numpy as np
import snowflake.connector
from dotenv import load_dotenv
load_dotenv()
import sys

# Path to the main zip file
main_zip_file ="/mnt/informatica/PPD/SMDW/json_read/FINAL_SOURCE/"

#Temperary file path
sub_zip_file ="/mnt/informatica/PPD/SMDW/json_read/sub_zip_1/"
json_path="/mnt/informatica/PPD/SMDW/json_read/json_files_1/"


#Final Excel path
excel_path="/mnt/informatica/PPD/SMDW/json_read/excel/"




for filenames in os.listdir(sub_zip_file):
    if filenames.endswith('.zip'):
        os.remove(os.path.join(sub_zip_file, filenames))

for filenames in os.listdir(json_path):
    if filenames.endswith('.json') or filenames.endswith('.csv') or filenames.endswith('.bin'):
        os.remove(os.path.join(json_path, filenames))    

#print("Please enter table names:")
#table_name1 = sys.stdin.read()
#table_name1=table_name1.strip().lower()
table_name1=input("Please enter table names:").lower()

print('___________________________________________')
print(table_name1)
table_name=table_name1.split(',')
table_name=[i.strip() for i in table_name]

session_name=[]
Transformation_name=[]
Transaction_type=[]
querys_all=[]
search=[]
folder=[]
pre=[]
presql11=[]
extend_name_source=[]
target_object=[]
post_sql_list=[]
sql_override_list=[]
current_value_list=[]
initial_value_list=[]
workflows_name_list=[]
shell_script_name=[]
stored_procedure_name=[]
########

type_system_name=[]
oracle_bin_name=[]

#######


workflow_count=0

lists=os.listdir(main_zip_file)

lists=[i for i in lists if i.endswith('.zip')]

for zip_file in lists:
    workflows_name=zip_file
    workflows_name=workflows_name.replace('.zip','')
    workflow_count+=1
    
    with zipfile.ZipFile(os.path.join(main_zip_file, zip_file), 'r') as zf:
        for file_info in zf.infolist():
            try:
                shutil.copy(f'{main_zip_file}{zip_file}',sub_zip_file)
            except:
                pass
            if file_info.filename.endswith('.zip'):
                # Extract the nested zip file
                nested_zip_data = zf.read(file_info)
                nested_zip_filename = os.path.join(sub_zip_file, os.path.basename(file_info.filename))

                # Write the extracted zip file to the destination folder
                with open(nested_zip_filename, 'wb') as nested_zip_file:
                    nested_zip_file.write(nested_zip_data)
            

    list_file=os.listdir(sub_zip_file)
    count=0    
    for file in list_file:
        if file.endswith('.zip'):
            with zipfile.ZipFile(f'{sub_zip_file}{file}', 'r') as zip_ref:
                for file_info in zip_ref.infolist():
                   
                    if file_info.filename.endswith('.json'):
                        with zip_ref.open(file_info) as source, open(os.path.join(json_path, os.path.basename(f'json_files_{count}.json')), 'wb') as dest:
                            dest.write(source.read())
                            count+=1
                    if file_info.filename.endswith('.csv') :
                        with zip_ref.open(file_info) as source, open(os.path.join(json_path, os.path.basename(file_info.filename)), 'wb') as dest:
                            dest.write(source.read())

                    if file_info.filename.endswith('.bin'):
                        with zip_ref.open(file_info) as source, open(os.path.join(json_path, os.path.basename(f'bin_file{count}.bin')), 'wb') as dest:
                            dest.write(source.read())
                            count+=1




        

    json_list=os.listdir(json_path)
    csv_file=glob.glob(os.path.join(json_path, "*.csv"))
    csv_file=csv_file[0]
    #csv_file=', '.join(csv_file)

    df=pd.read_csv(csv_file)
    pd_csv=df
 
    for files in json_list:
               
            
        if files.endswith('.json'):


            try:
                
                with open(f'{json_path}{files}', 'r') as file:
                    data = json.load(file)
            except:
                data=''
                

        
     
    
            def extract_table_names(sql_script):

                sql_script=sql_script.replace('$','')
                #sql_script=sql_script.replace('\\n','')
                sql_script=sql_script.replace('\t','@')
                sql_script=sql_script.replace('@','')
                sql_script=sql_script.replace('\n\t','')
                sql_script=sql_script.replace('\n','')
                sql_script=sql_script.replace('\t','')


                pattern = r'(?:from|join|update)\s+([\w\.]+\s*(?:,\s*[\w\.]+\s*)*)'


                pattern_value = re.findall(pattern, sql_script, re.IGNORECASE)

                all_table_names= [i.split(',') for i in pattern_value]

                all_table_names = [item for sublist in all_table_names for item in sublist]
                all_table_names=[i.split('.')[-1].strip() for i in all_table_names]
                return all_table_names            

        
        
            currentValue=[]
            initialValue=[]
            value_list=[]
            def find_custom_query3(data):
                if isinstance(data, dict):
                    for key, value in data.items():
                        if key == "currentValue":
                            currentValue.append(value)
                        if key=="initialValue":
                            initialValue.append(value)
                        if key=="value":
                            value1=str(value)
                            
                    
        
                            if 'select' in value1.lower() or 'from' in value1.lower():
           

                                currentValue.append(value) 
                            
                        elif isinstance(value, (dict, list)):
                            result = find_custom_query3(value)
                            if result is not None:
                                currentValue.append(result)
                elif isinstance(data, list):
                    for item in data:
                        result = find_custom_query3(item)
                        if result is not None:
                            initialValue.append(result)                
                

            find_custom_query3(data)

                    
            
        
        

            for item in data:
                try:
                    type_values = item.get("name")
                   
              
                except:
                    type_values='null'

                try:
                    shell_file = item.get("postProcessingCmd")
                    if '.sh' in shell_file or '.SH' in shell_file:
                        shell_file1=shell_file
 
                    else:
                        shell_file1="NaN"

                   
              
                except:
                    shell_file1='NaN'                    
                   
                try:
                    parameters = item.get("parameters", [])
                except:
                    parameters=''
                    
             
                if parameters is not None:

                    for parameter in parameters:
                        type_value = parameter.get("type")   
                        querys = parameter.get("customQuery")
                        pm_name=parameter.get("name")
                        try:
                            query1=parameter.get("runtimeAttrs",{})
                            pre_sql=query1.get('Pre SQL',None)

                        except:
                            pre_sql=None
                        try:
                            query2=parameter.get("oprRuntimeAttrs",{})
                            presql=query2.get('presql',None)

                        except:
                            presql=None                           
 
################################
                        try:
                            
                            extended=parameter.get("extendedObject",{})
                            extended_object=extended.get("object",{})
                            extend_name=extended_object.get("name")
                        

                        except:
                            extend_name=None
                            
                            
                        try:
                            target=parameter.get("targetObject")
                        

                        except:
                            target=None
###########################################
                        try:
                            post_query=parameter.get("runtimeAttrs",{})
                            post_sql=post_query.get('Post SQL',None)                           

                        except:
                            post_sql=None   
                            
                        try:
                            override_query=parameter.get("oprRuntimeAttrs",{})
                            sql_override=override_query.get('sqloverride',None)                           

                        except:
                            sql_override=None     
                            
                        try:
                            
                            values=df[df['objectName']==type_values]
                            fst_col=values['objectPath']
                            
                        except:
                            fst_col='null'

                        for current in currentValue:
                            
                            
                            if current is not None:

                                query=current
                                query=query.replace('\n','')
                                query=query.replace('\t','')
                                query=query.replace('\n\t','')
                                checking_sql=query[:]
                                checking_sql=checking_sql.lower()
                                if 'select' in checking_sql and 'from' in checking_sql:

                                    table_names = extract_table_names(query)
                                    second_values =table_names
                                else:
                                    second_values=None
                                    
                                if second_values is not None:

                                    second_values1=[value.lower() for value in second_values if value is not None]
                                    for name in table_name:
                                        if any(name.lower() in values for values in second_values1):                  
                                            session_name.append(type_values)

                                            Transformation_name.append('NaN')
                                            Transaction_type.append('currentValue')
                                            querys_all.append('NaN')
                                            search.append(name)
                                            folder.append(fst_col)
                                            pre.append('NaN')
                                            presql11.append('NaN')
                                            extend_name_source.append('NaN')
                                            target_object.append('NaN')
                                            post_sql_list.append("NaN")
                                            sql_override_list.append('NaN')
                                            current_value_list.append(current)
                                            initial_value_list.append("NaN")
                                            workflows_name_list.append(workflows_name)
                                            shell_script_name.append(shell_file1)
                                            stored_procedure_name.append('NaN')


                                
                        for initial in initialValue:
                            
                            
                            if initial is not None:

                                query=initial
                                query=query.replace('\n','')
                                
                                checking_sql_initial=query[:]
                                checking_sql_initial=checking_sql_initial.lower()
                                if 'select' in checking_sql_initial and 'from' in checking_sql:
                                    
                                    table_names = extract_table_names(query)
                                    second_values =table_names
                                else:
                                    second_values=None
                                    
                                

                                if second_values is not None:
                                    
                                    second_values1=[value.lower() for value in second_values if value is not None]
                                    for name in table_name:
                                        if any(name.lower() in values for values in second_values1):                  
                                            session_name.append(type_values)

                                            Transformation_name.append('NaN')
                                            Transaction_type.append('initialValue')
                                            querys_all.append('NaN')
                                            search.append(name)
                                            folder.append(fst_col)
                                            pre.append('NaN')
                                            presql11.append('NaN')
                                            extend_name_source.append('NaN')
                                            target_object.append('NaN')
                                            post_sql_list.append("NaN")
                                            sql_override_list.append('NaN')
                                            initial_value_list.append(initial)
                                            current_value_list.append("NaN")
                                            workflows_name_list.append(workflows_name)
                                            shell_script_name.append(shell_file1)
                                            stored_procedure_name.append('NaN')                               

                                
                                



                        query=querys
                        if query is not None:
                            
                            query=querys[:]
                            query=query.replace('\n','')
                  
                            table_names = extract_table_names(query)
                            second_values =table_names
                            
    
                            second_values1=[value.lower() for value in second_values if value is not None]
                            for name in table_name:
                                if any(name.lower() in values for values in second_values1):                  
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append(querys)
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append('NaN')
                                    extend_name_source.append('NaN')
                                    target_object.append('NaN')
                                    post_sql_list.append("NaN")
                                    sql_override_list.append('NaN')
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                    
                                    
                        if pre_sql is not None:
                            pre_sql1=pre_sql[:]
                            pre_sql1=pre_sql1.replace('\n','')
                            pre_table_names=extract_table_names(pre_sql1)
                            only_table = pre_table_names
                            second_values1=[value.lower() for value in only_table if value is not None]
                            for name in table_name:
                                if any(name.lower() in values for values in second_values1):                            
                        
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append(pre_sql)
                                    presql11.append('NaN')
                                    extend_name_source.append('NaN')
                                    target_object.append('NaN')
                                    post_sql_list.append("NaN")
                                    sql_override_list.append('NaN')
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                    
                                    
                            
                        if presql is not None:
                            presql1=presql[:]
                            presql1=presql1.replace('\n','')
                            pre_table_names=extract_table_names(presql1)
                            only_table =pre_table_names
                            second_values1=[value.lower() for value in only_table if value is not None]
                            for name in table_name:
                                if any(name.lower() in values for values in second_values1):                            
                                
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append(presql)
                                    extend_name_source.append('NaN')
                                    target_object.append('NaN')
                                    post_sql_list.append("NaN")
                                    sql_override_list.append('NaN')
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                    
                                    
                        if extend_name is not None:
                            
                            for name in table_name:
                            
                                if name.lower() in extend_name.lower():   
                                     
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append('NaN')
                                    post_sql_list.append("NaN")
                                    extend_name_source.append(extend_name)
                                    sql_override_list.append('NaN')

                                    target_object.append('NaN')
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                   
                                    

                                
                           
                        if target is not None:
                           
                            for name in table_name:
                                if name.lower() in target.lower():                              

                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append('NaN')
                                    target_object.append(target)
                                    post_sql_list.append("NaN")
                                    sql_override_list.append('NaN')

                                    extend_name_source.append("NaN")
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                   
                                    
                                    
                        if post_sql is not None:
                            post_sql1=post_sql[:]
                            post_sql1=post_sql1.replace('\n','')
                            pre_table_names=extract_table_names(post_sql1)
                            only_table =pre_table_names
                            second_values1=[value.lower() for value in only_table if value is not None]
                            for name in table_name:
                                if any(name.lower() in values for values in second_values1):      
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append('NaN')
                                    extend_name_source.append('NaN')
                                    target_object.append('NaN')
                                    post_sql_list.append(post_sql)
                                    sql_override_list.append('NaN')
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                  
                                    
                        if sql_override is not None:
                            sql_override1=sql_override[:]
                            sql_override1=sql_override1.replace('\n','')
                            pre_table_names=extract_table_names(sql_override1)
                            only_table = pre_table_names
                            second_values1=[value.lower() for value in only_table if value is not None]
                            for name in table_name:
                                if any(name.lower() in values for values in second_values1):                             
                            
                           
                                
                                    session_name.append(type_values)

                                    Transformation_name.append(pm_name)
                                    Transaction_type.append(type_value)
                                    querys_all.append('NaN')
                                    search.append(name)
                                    folder.append(fst_col)
                                    pre.append('NaN')
                                    presql11.append('NaN')
                                    extend_name_source.append('NaN')
                                    target_object.append('NaN')
                                    post_sql_list.append("NaN")
                                    sql_override_list.append(sql_override)
                                    initial_value_list.append("NaN")
                                    current_value_list.append("NaN")
                                    workflows_name_list.append(workflows_name)
                                    shell_script_name.append(shell_file1)
                                    stored_procedure_name.append('NaN')                                    
                                    
                                
                else:
                    pass

        if files.endswith('.bin'):
           

            
            
            def extract_table_names1(sql_script):

                sql_script=sql_script.replace('$','')
                sql_script=sql_script.replace('\\n','')
                sql_script=sql_script.replace('\t','@')
                sql_script=sql_script.replace('@','')
                sql_script=sql_script.replace('\n','')
                sql_script=sql_script.replace('\t','')

                sql_script=sql_script.replace('\n\t','')




                pattern = r'(?:from|join|update)\s+([\w\.]+\s*(?:,\s*[\w\.]+\s*)*)'


                pattern_value = re.findall(pattern, sql_script, re.IGNORECASE)

                all_table_names= [i.split(',') for i in pattern_value]

                all_table_names = [item for sublist in all_table_names for item in sublist]
                all_table_names=[i.split('.')[-1].strip() for i in all_table_names]

                return all_table_names            
            


            def find_custom_query_bin(file_path):
                with open(file_path, 'rb') as bin_file:
                    content = bin_file.read().decode('utf-8')

                    matches = re.findall(r'"customQuery"\s*:\s*"(.*?)"', content)
                    matches_name = re.findall(r'"name"\s*:\s*"(.*?)"', content)
                    bin_call=re.findall(r'"value"\s*:\s*"(.*?)"', content)
                    default_value=re.findall(r'"defaultValue"\s*:\s*"(.*?)"', content)
                    connectionType=re.findall(r'"connectionType"\s*:\s*"(.*?)"', content)
                    typeSystem=re.findall(r'"typeSystem"\s*:\s*"(.*?)"', content)
                 
                
                  

                    
                    queries_1=[i for i in matches_name if i.startswith('s_') or i.startswith('S_') or i.startswith('m_') or i.startswith('M_')] 
                    queries_1=queries_1[0]
                    

                    return matches,queries_1,bin_call,default_value,connectionType,typeSystem

             
                
                

            bin_file_path=f'{json_path}{files}'
                              
            try:
                
                bin_customquery,bin_name,bin_call_final,default_value,connection_type,type_system=find_custom_query_bin(bin_file_path)
     

                
            except:
                bin_customquery=[]
                bin_name=None
                bin_call_final=None
                default_value=[]
                connection_type=[]
                type_system=[]
                
            if connection_type:
                for conn in connection_type:
                    if conn.lower().strip()=='oracle':
                        if bin_name is not None:
                       

                          

                            oracle_bin_name.append(bin_name)
                        else:
                            oracle_bin_name.append('NaN')
                            
            if type_system:
                for conn in type_system:
                    if conn.lower().strip()=='oracle':
                        if bin_name is not None:
                     

                            type_system_name.append(bin_name)
                        else:
                            type_system_name.append('NaN')                        
                    
                
                
                
                
            if bin_name is not None:
                values=pd_csv[pd_csv['objectName']==bin_name]
                fst_col12=values['objectPath']
            bin_call_final_o=[]
            if bin_call_final is not None:
                for i in bin_call_final:
 
                    checker=i[:]
                    checker=checker.lower()
                    if('call' in checker and '$$' in checker):
                        
                        if 'select' not in checker:
                            bin_call_final_o.append(checker)
                    if 'select' in checker and 'from' in checker:
                        bin_customquery=bin_customquery+[i]
                    if 'update' in checker and 'set' in checker:
                        bin_customquery=bin_customquery+[i]
                    if 'delete' in checker and 'from' in checker:
                        bin_customquery=bin_customquery+[i]                        
                        
                            
                
            bin_call_final_oo=''
            for j in bin_call_final_o:
                if len(j) >0:
                    bin_call_final_oo=j
            
        
           
            if bin_call_final_oo is not None and len(bin_call_final_oo)>0:
                bin_call_final_oo=bin_call_final_oo
            else:
                bin_call_final_oo='NaN'
                
            if default_value:
                for default in default_value:
                    if 'select' in default.lower() or 'from' in default.lower():
                        #print([default])
                        #print("_______________________")
                        bin_customquery+=[default]
                        
                
            if bin_customquery:
                for sql_bin in bin_customquery:

                    if sql_bin is not None:
                       
                    

                        query=sql_bin[:]
                        query=query.replace('\n','')

                        table_names_bin = extract_table_names1(query)
                   
                        second_values1=[value.lower() for value in table_names_bin if value is not None]
 
                        for name in table_name:
                            if any(name.lower() in values for values in second_values1):
                                session_name.append(bin_name)

                                Transformation_name.append('bin')
                                Transaction_type.append('bin')
                                querys_all.append(sql_bin)
                                search.append(name)
                                folder.append(fst_col12)
                                pre.append('NaN')
                                presql11.append('NaN')
                                extend_name_source.append('NaN')
                                target_object.append('NaN')
                                post_sql_list.append("NaN")
                                sql_override_list.append('NaN')
                                initial_value_list.append("NaN")
                                current_value_list.append("NaN")
                                workflows_name_list.append(workflows_name)
                                shell_script_name.append('NaN')
                                stored_procedure_name.append(bin_call_final_oo)                               
                                

    for filenames in os.listdir(sub_zip_file):
        if filenames.endswith('.zip'):
            os.remove(os.path.join(sub_zip_file, filenames))

    for filenames in os.listdir(json_path):
        if filenames.endswith('.json') or filenames.endswith('.csv')or filenames.endswith('.bin'):
            os.remove(os.path.join(json_path, filenames))                


df=pd.DataFrame({'Search_Text':search,'Workflow_Name':workflows_name_list,'Folder_name_&_Project_name':folder,
                 'session_name':session_name,'Transformation_name':Transformation_name,'Transaction_type':Transaction_type,
                 'customQuery':querys_all,'Pre_SQL':pre,'presql':presql11,'Extended_source':extend_name_source,'Extended_target':target_object,
                 'Post_Sql':post_sql_list,'Sql_Override':sql_override_list,'current_value':current_value_list,'Initial_value':initial_value_list,
                 'Shell_file':shell_script_name,'Stored_procedure':stored_procedure_name},dtype='str')










df['Pre_SQL_Query'] = np.where(df['Pre_SQL'] == 'NaN', df['presql'], df['Pre_SQL'])
df = df.drop(['presql','Pre_SQL'], axis=1)
df=df.drop_duplicates()

df=df[['Search_Text','Workflow_Name','session_name','Transformation_name','Transaction_type','Shell_file','Stored_procedure','customQuery','Pre_SQL_Query',
       'Post_Sql','Sql_Override','Extended_source','Extended_target','current_value','Initial_value','Folder_name_&_Project_name']]

def sh_table_names(sql_script):

    sql_script=sql_script.replace('$','')
    sql_script=sql_script.replace('\\n','')
    sql_script=sql_script.replace('\t','@')
    sql_script=sql_script.replace('@','')
    sql_script=sql_script.replace('\n\t','')
    sql_script=sql_script.replace('\n','')
    sql_script=sql_script.replace('\t','')
   

    pattern = r'(?:from|join|update)\s+([\w\.]+\s*(?:,\s*[\w\.]+\s*)*)'


    pattern_value = re.findall(pattern, sql_script, re.IGNORECASE)

    all_table_names= [i.split(',') for i in pattern_value]

    all_table_names = [item for sublist in all_table_names for item in sublist]
    all_table_names=[i.split('.')[-1].strip() for i in all_table_names]
    return all_table_names
for index,values in df.iterrows():
    shell_file_values=values['Shell_file']
    shell_check_names=values['Search_Text']

    if shell_file_values != 'NaN':
        
        shell_file_values=shell_file_values.split(' ')
        shell_file_values=[[values] for values in shell_file_values]
        shell_file_values=shell_file_values[-2]
        shell_file_values=' '.join(shell_file_values)
        shell_file_values=shell_file_values.split(';')
        shell_file_values=[[i] for i in shell_file_values]
        shell_file_values=shell_file_values[0]
        shell_file_values=' '.join(shell_file_values)
        try:

            with open(shell_file_values,'r') as shell_file:
                shell_value=shell_file.read()
        except:
            shell_value=''
            
        shell_value=shell_value.lower()    
        if shell_check_names in shell_value:
            pass
        else:
            
            df.loc[index, 'Shell_file'] = 'NaN'
            
            
##################Read all sh file#####


shell_file_search=[]
shell_file_path=[]
all_matching_lines=[]

src_folder=os.getenv('unix_file_path')
src_folder_list=src_folder.split(',')
for src_folder in src_folder_list:

    for root, dirs, files in os.walk(src_folder):
        for file in files:
            if file.endswith(".sh") or file.endswith(".SH") or file.endswith(".ksh") or file.endswith(".KSH")or file.endswith(".csv") or file.endswith(".prm") or file.endswith(".log") or file.endswith(".txt") or file.endswith(".CSV") or file.endswith(".PRM") or file.endswith(".LOG") or file.endswith(".TXT"):
                shell_path=os.path.join(root,file)
                try:            
                    with open(shell_path,'r') as shell_file_all:
                        shell_value_all=shell_file_all.read()
                    #sh_table_list=sh_table_names(shell_value_all)
                except:
                    sh_table_list=None

                sh_table_list=shell_value_all.lower()
                if sh_table_list is not None:
                    for name in table_name:
                        if name in sh_table_list:
                            matching_lines=[]
                            sh_table_list=sh_table_list.splitlines()

                            for lines in sh_table_list:
                                if re.search(name, lines):
                                    matching_lines.append(lines.strip())

                            results = ",".join(matching_lines) 
                            all_matching_lines.append(results)
                            shell_file_search.append(name)
                            shell_file_path.append(shell_path)

            if file.endswith(".xlsx") or file.endswith(".XLSX"):
                excel_paths=os.path.join(root,file)
                read_excel_file=pd.read_excel(excel_paths)

                for table in table_name:
                    column_values=0
                    matching_rows=[]
                    for index, row in read_excel_file.iterrows():
                        for column_list in read_excel_file.columns:
                            if table in str(row[column_list]).lower():
                                matching_rows.append(row[column_list].strip())
                                column_values+=1


                    if column_values>0:
                        shell_file_search.append(table)
                        shell_file_path.append(excel_paths)
                        excel_results = ",".join(matching_rows) 
                        all_matching_lines.append(excel_results)



                    

sh_df=pd.DataFrame({'SEARCH':shell_file_search,'IMPACTED_UNIX_FILE':shell_file_path,'MATCHED_WORD':all_matching_lines})
 
##############
####snowflake 



def initialize_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv('user'),
        password=os.getenv('password'),
        account=os.getenv('account'),
        warehouse=os.getenv('warehouse'),
        database=os.getenv('database'),
        schema=os.getenv('schema')
    )
    return conn

conn = initialize_snowflake_connection()


df_start = pd.DataFrame(columns=['SEARCH','PROCEDURE_NAME', 'PROCEDURE_DEFINITION'])
df_view = pd.DataFrame(columns=['SEARCH','VIEW_NAME', 'VIEW_DEFINITION'])
wf_list=pd.DataFrame(columns=['SEARCH','FOLDER_NAME','WORKFLOW_NAME','TASK_NAME','MAPPING_NAME','PARAMETER_NAME','PARAMETER_VALUE'])
for schema in table_name:

    query =f'''select DISTINCT '{schema}' AS SEARCH,TRIM(PROCEDURE_SCHEMA)||'.'||TRIM(PROCEDURE_NAME) AS PROCEDURE_NAME, PROCEDURE_DEFINITION from 
    information_schema.procedures where LOWER(PROCEDURE_DEFINITION) like '%{schema}%' order by 1'''
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    df_snow = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    df_start=pd.concat ([df_start,df_snow],axis=0)
    
    query1 =f'''select DISTINCT '{schema}' AS SEARCH,TRIM(TABLE_SCHEMA)||'.'||TRIM(TABLE_NAME) AS VIEW_NAME, VIEW_DEFINITION 
    from information_schema.views where LOWER(VIEW_DEFINITION) like '%{schema}%'  order by 1'''
    cursor = conn.cursor()
    cursor.execute(query1)
    results = cursor.fetchall()
    df_snow_1 = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    df_view=pd.concat ([df_view,df_snow_1],axis=0)   
    
    query2 =f'''SELECT DISTINCT '{schema}' AS SEARCH,BATCH.FOLDERNAME AS FOLDER_NAME,
BATCH.WORKFLOW_NAME AS WORKFLOW_NAME,
BATCH_STEP_RUN.TASK_NAME AS TASK_NAME,
BATCH_STEP_RUN.MAPPING_NAME AS MAPPING_NAME,
BATCH_PARAMETER.PARAMETER_NAME, 
BATCH_PARAMETER.PARAMETER_VALUE  
FROM DDSV.BATCH_PARAMETER 
JOIN DDSV.BATCH ON BATCH_PARAMETER.BATCH_ID=BATCH.BATCH_ID 
JOIN DDSV.BATCH_STEP_RUN  ON BATCH_STEP_RUN.BATCH_STEP_RUN_ID=BATCH_PARAMETER.BATCH_STEP_RUN_ID
WHERE LOWER(BATCH_PARAMETER.PARAMETER_VALUE) LIKE '%{schema}%'
ORDER BY 1,2,3,4,5'''
    cursor = conn.cursor()
    cursor.execute(query2)
    results = cursor.fetchall()
    df_snow_wf = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
    wf_list=pd.concat ([wf_list,df_snow_wf],axis=0)     

cursor.close()
df_start=df_start.drop_duplicates()
df_view=df_view.drop_duplicates()
wf_list=wf_list.drop_duplicates()
#############a


impact_workflow_count=len(pd.unique(df['Workflow_Name']))
impact_session_count = len(pd.unique(df['session_name'][df['session_name'].str.startswith(('s_', 'S_', 'mct_', 'mt_'))]))
impact_mapping_count = len(pd.unique(df['session_name'][df['session_name'].str.startswith(('m_', 'M_'))]))
df_unix_count= pd.DataFrame({'IMPACTED_UNIX_SCRIPT': df['Shell_file'][df['Shell_file'] != 'NaN']}).drop_duplicates()
impact_unix_count=len(df_unix_count)
df_stored_count= pd.DataFrame({'IMPACTED_STORED_PROCEDURE': df['Stored_procedure'][df['Stored_procedure'] != 'NaN']}).drop_duplicates()
impact_shell_count=len(df_stored_count)




df2 = pd.DataFrame({'Workflow_Name': df['Workflow_Name'].unique()})
df3 = pd.DataFrame({'Session_name': df['session_name'][df['session_name'].str.startswith(('s_', 'S_', 'mct_', 'mt_'))].unique()})
df4 = pd.DataFrame({'Mapping_name': df['session_name'][df['session_name'].str.startswith(('m_', 'M_'))].unique()})
df5= pd.DataFrame({'IMPACTED_UNIX_SCRIPT': df['Shell_file'][df['Shell_file'] != 'NaN']}).drop_duplicates()
df5['IMPACTED_UNIX_SCRIPT']=df5['IMPACTED_UNIX_SCRIPT'].str.replace('ksh -x ','')
df5['IMPACTED_UNIX_SCRIPT']=df5['IMPACTED_UNIX_SCRIPT'].str.replace(' ;','')

#df5=pd.concat ([df5,sh_df],axis=0)
df5=df5.drop_duplicates()
sh_df=sh_df.drop_duplicates()

impact_sf_unix_count=len(sh_df.index)
impact_sf_stored_procedure_count=len(df_start.index)
impact_sf_view_procedure_count=len(df_view.index)
impact_sf_batch_procedure_count=len(wf_list.index)

df6= pd.DataFrame({'IMPACTED_STORED_PROCEDURE': df['Stored_procedure'][df['Stored_procedure'] != 'NaN']}).drop_duplicates()                                                             

df1=pd.DataFrame({'SEARCH_STRING_USED':[table_name1],'NO_OF_WORKFLOWS_SCANNED':[workflow_count],'IMPACTED_WORKFLOWS':[impact_workflow_count],
                  'IMPACTED_SESSIONS':[impact_session_count],'IMPACT_MAPPING':[impact_mapping_count],'IMPACTED_IICS_UNIX_SCRIPT':[impact_unix_count],
                 'IMPACTED_IICS_STORED_PROCEDURE':[impact_shell_count],'IMPACTED_SHELL_COUNT':[impact_sf_unix_count],
                 'IMPACTED_STORED_PROCEDURE':[impact_sf_stored_procedure_count],'IMPACTED_VIEW_PROCEDURE':[impact_sf_view_procedure_count],'IMPACTED_BATCH_PROCEDURE':[impact_sf_batch_procedure_count]},dtype='str')
df1= df1.melt(var_name='Summary_sheet', value_name='value')
#############oracle
oracle_bin_name=oracle_bin_name+type_system_name
oracle_df=pd.DataFrame({'Mapping_name':oracle_bin_name})


########
print(table_name1)
with pd.ExcelWriter(f'{excel_path}{table_name1}.xlsx') as writer:
    # Write each dataframe to a separate sheet
    df1.to_excel(writer, sheet_name="SUMMARY",index=False)
    df.to_excel(writer, sheet_name="IICS_EXTRACT", index=False)
    df5.to_excel(writer, sheet_name="IICS_UNIX_SCRIPT",index=False)
    df6.to_excel(writer, sheet_name="IICS_SP",index=False)
    df2.to_excel(writer, sheet_name="WORKFLOWS",index=False)
    df3.to_excel(writer, sheet_name="SESSIONS",index=False)
    df4.to_excel(writer, sheet_name="MAPPING",index=False)
    sh_df.to_excel(writer, sheet_name="UNIX_FILE",index=False)
    df_start.to_excel(writer, sheet_name="STORED_PROCEDURE",index=False)
    df_view.to_excel(writer, sheet_name="VIEW",index=False)
    wf_list.to_excel(writer, sheet_name="BATCH_PARAMETER",index=False)
    oracle_df.to_excel(writer, sheet_name="ORACLE_MAPPING",index=False)