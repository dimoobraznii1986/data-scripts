from fileinput import filename
from pyathena import connect
import re
import boto3
import pandas as pd
from datetime import datetime
# Provide Staging Dir
staging_dir = "<path to s3 dir>"

#Connect Athena
cursor = connect(s3_staging_dir=staging_dir,
                 region_name='us-east-1').cursor()

def get_list_schemas() -> list:
    """"
    Running sql command SHOW SCHEMAS against Athena
    and return list with all schemas
    """

    cursor.execute(f"SHOW SCHEMAS")
    schemas = cursor.fetchall()

    schema_list = []
    for schema in schemas:
        schema_str = re.sub(r"[(\"  \']|[,)]","",str(schema))
        schema_list.append(schema_str)
    
    return schema_list

def get_list_tables(schema: str) -> list:
    schema_name = schema

    cursor.execute(f"SHOW TABLES in {schema_name}")
    tables = cursor.fetchall()

    table_list = []
    for table in tables:
        table_str = re.sub(r"[(\"  \']|[,)]","",str(table))
        table_list.append(table_str)
    
    return table_list

def get_s3_path(schema: str, table: str) -> str:
    """
    This function is using schema and table name in order to check the
    SHOW CREATE TABLE command output and excreatct the s3 LOCATION path 
    of the table
    """
    schema_name = schema
    table_name = table
    ddl_lines = cursor.execute(f"SHOW CREATE TABLE {schema_name}.{table_name}")
    
    #Search for S3 location in DDL output
    substring = 's3://'
    for ddl_line in ddl_lines:
        if substring in str(ddl_line):
            s3_path = re.sub(r"[(\"  \']|[,)]","",str(ddl_line))
    
    return s3_path


def get_folder_size(bucket, prefix):
    """
    Using boto3 library we can get the size of all objects in particular s3 bucket and path.
    """
    total_size = 0
    for obj in boto3.resource('s3').Bucket(bucket).objects.filter(Prefix=prefix):
        total_size += obj.size
    return round(total_size/1024/1024/1024,2)

def write_to_csv(output: list, schema):
    """
    Pandas operation to write into CSV file. Currently the Path is hardcoded.
    Pandas create file but doesn't create a folder. Could be solve twith os library.
    """
    df = pd.DataFrame(data = output, columns = ["schema_name", "table_name", "s3path", "size_in_gb"])
    schema_name = schema
    
    #save result to CSV
    datetimenow = datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    
    file_name = f"/Users/dimaanoshin/1p-projects/scripts/scripts/scripts/output_with_schema/{schema_name}datalake_size_{datetimenow}.csv"
    df.to_csv(file_name, sep='\t', encoding='utf-8',index=False)

    return print(f"Schema {schema_name} processed")

def main():
    schema_list = get_list_schemas()
    for schema in schema_list:
        result = []
        table_list = get_list_tables(schema)
        print(f"{len(table_list)} tables will be processed in {schema} schema")
        for table in table_list:
            #Filter out VIEWS
            if table not in  ['src_timeseries_account_history1','test_count_accounts']:
                s3path = get_s3_path(schema, table)
                #Get bucket name and prefix path
                bucket = s3path.split('/')[2]
                substring = 's3://'+ bucket + '/'
                prefix = s3path.replace(substring,"")
                
                #Get the size of the tabe in Gb
                athena_table_size_gb = get_folder_size(bucket, prefix)
                
                #Build tuple raw
                line = (schema, table, s3path, athena_table_size_gb)
                #Build list for data frame
                result.append(line)

        #Writing result for one schema
        write_to_csv(result, schema)


    return result


if __name__ == "__main__":
    main()
