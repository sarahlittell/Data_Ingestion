import csv
import testutility as util
import datetime
import time


# config_data=util.read_config_file('file.yaml')
# config_data #is supposed to have output? 
import pandas as pd
# #normal processing of the file
df_sample=pd.read_csv("customers-11500000.csv",delimiter=',')
df_sample.head()


# # read the file using config file
# file_type = config_data['file_type']
# source_file = "./" + config_data['file_name'] + f'.{file_type}' 
# df = pd.read_csv(source_file,config_data['inbound_delimiter']) 
# df.head()

# #validate the headers of the file
# util.col_header_val(df,config_data)
# # print("columns of files are:" ,df.columns)
# # print("columns of YAML are:" ,config_data['columns'])

#converting file with pipeline delimiter
# import csv
# with open('customers-11500000.csv') as fin:
#     with open('OutputFile.txt','w',newline='') as fout:
#         reader=csv.DictReader(fin, delimiter=',')
#         writer=csv.DictWriter(fout,reader.fieldnames,delimiter='|')
#         writer.writeheader()
#         writer.writerows(reader)

# #convert output.txt to the gz format type
# import gzip
# with open('OutputFile.txt','rb') as orig_file:
#     with gzip.open('OutputFile.txt.gz','wb') as zipped_file:
#         zipped_file.writelines(orig_file)

#Dask:
# start=datetime.datetime.now()
# import dask
# import dask.dataframe as dd
# df=dd.read_csv('customers-11500000.csv')
# df.head()
# end=datetime.datetime.now()
# elapsed=end-start
# print('run time: ',elapsed)

#Ray: (could not get to work properly)
# import ray
# ds=ray.data.read_csv('customers-11500000.csv').repartition(100000) #customers-11500000.csv
# ds=data.repartition(100)
# ds=ray.data.read_csv('customers-11500000.csv')
#ds.head()

num_rows = len(df_sample.index)
num_cols = len(df_sample.columns)
file_size = '2.02 GB'
print('Number of rows: ',num_rows)
print('Number of columns: ',num_cols)
print('File size: ',file_size)