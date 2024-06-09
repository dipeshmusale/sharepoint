import pandas as pd
import io
import boto3

s3client = boto3.client('s3')
def df_to_parquet(df):
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer
def upload_parquet_to_s3(out_buffer, name):
    # Upload the parquet file to cleansed layer bucket
    s3client.put_object(Bucket='harmonizedlayerbucket',
                        Key= name + '.parquet',
                        Body=out_buffer.getvalue())
con = pd.read_excel('C:/Users/Admin/PycharmProjects/pythonProject/countries.xlsx')
dep = pd.read_excel('C:/Users/Admin/PycharmProjects/pythonProject/departments.xlsx')
emp = pd.read_excel('C:/Users/Admin/PycharmProjects/pythonProject/employees.xlsx')

reg = pd.read_csv('C:/Users/Admin/PycharmProjects/pythonProject/regions.csv')
jobs = pd.read_csv('C:/Users/Admin/PycharmProjects/pythonProject/jobs.csv')
jh = pd.read_csv('C:/Users/Admin/PycharmProjects/pythonProject/job_history.csv')
loc = pd.read_csv('C:/Users/Admin/PycharmProjects/pythonProject/locations.csv')

reg_con = reg.merge(con,on = 'REGION_ID', how = 'outer')

reg_con_loc = reg_con.merge(loc,on = 'COUNTRY_ID', how = 'outer')

reg_con_loc_dep = reg_con_loc.merge(dep,on = 'LOCATION_ID', how = 'outer')

reg_con_loc_dep_jh = reg_con_loc_dep.merge(jh,on = 'DEPARTMENT_ID', how = 'outer')

reg_con_loc_dep_jh_emp = reg_con_loc_dep_jh.merge(emp,on = 'EMPLOYEE_ID', how = 'outer')
reg_con_loc_dep_jh_emp = reg_con_loc_dep_jh_emp[reg_con_loc_dep_jh_emp.columns.drop(list(reg_con_loc_dep_jh_emp.filter(regex='_y')))]
cols = []
for c in reg_con_loc_dep_jh_emp.columns:
    if '_x' in c:
        c = c.replace('_x','')
        cols.append(c)
    else:
        cols.append(c)
reg_con_loc_dep_jh_emp.columns = cols

reg_con_loc_dep_jh_emp_j = reg_con_loc_dep_jh_emp.merge(jobs,on = 'JOB_ID', how = 'outer')

out_buffer = df_to_parquet(reg_con_loc_dep_jh_emp_j)
filename = 'harmonized_parquet_file'
upload_parquet_to_s3(out_buffer, filename)# print(combined_df)


# combined_dfs = pd.DataFrame()
#     all_dfs = []
#     all_PKs = []
#     df_names = []
#     for file in files:
#         df, filename = create_df_from_file(file, prefix)
#         all_dfs.append(df)
#         primary_key = get_primary_key(filename)
#         all_PKs.append(primary_key)
#         df_names.append(filename)
