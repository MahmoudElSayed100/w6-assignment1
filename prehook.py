import os
from database_handler import execute_query, create_connection, close_connection,return_data_as_df, return_create_statement_from_df
from lookups import ErrorHandling, PreHookSteps, SQLTablesToReplicate, InputTypes, SourceName
from logging_handler import show_error_message
import pandas as pd

def execute_sql_folder(db_session, sql_command_directory_path):
    sql_files = [sqlfile for sqlfile in os.listdir(sql_command_directory_path) if sqlfile.endswith('.sql')]
    sorted_sql_files =  sorted(sql_files)
    for sql_file in sorted_sql_files:
        with open(os.path.join(sql_command_directory_path,sql_file), 'r') as file:
            sql_query = file.read()
            return_val = execute_query(db_session= db_session, query= sql_query)
            if not return_val == ErrorHandling.NO_ERROR:
                raise Exception(f"{PreHookSteps.EXECUTE_SQL_QUERY.value} = SQL File Error on SQL FILE = " +  str(sql_file))
    
def return_tables_by_schema(schema_name):
    schema_tables = list()
    tables = [table.value for table in SQLTablesToReplicate]
    for table in tables:
        if table.split('.')[0] == schema_name:
            schema_tables.append(table)
    return schema_tables

def create_sql_staging_tables(db_session, source_name):
    tables = return_tables_by_schema(source_name)
    for table in tables:
        staging_query = f"""
                SELECT * FROM {source_name}.{table} LIMIT 1
        """
        staging_df = return_data_as_df(db_session= db_session, input_type= InputTypes.SQL, file_executor= staging_query)
        dst_table = f"stg_{source_name}_{table}"
        create_stmt = return_create_statement_from_df(staging_df, 'dw_reporting', dst_table)
        execute_query(db_session=db_session, query= create_stmt)

def execute_prehook(sql_command_directory_path = './SQL_Commands'):
    try:
        db_session = create_connection()
        # Step 1:
        execute_sql_folder(db_session, sql_command_directory_path) 
        # Step 2 getting dvd rental staging:
        create_sql_staging_tables(db_session,SourceName.DVD_RENTAL)
        # Step 3 getting college staging:
        # create_sql_staging_tables(db_session,SourceName.COLLEGE)
        close_connection(db_session)
    except Exception as error:
        suffix = str(error)
        error_prefix = ErrorHandling.PREHOOK_SQL_ERROR
        show_error_message(error_prefix.value, suffix)
        raise Exception("Important Step Failed")
    
def create_table_from_csv(db_session, csv_file_path, schema_name, table_name):
    try:
        df = pd.read_csv(csv_file_path)
        create_table_sql = return_create_statement_from_df(df, schema_name, table_name)
        execute_query(db_session, create_table_sql)
    except Exception as e:
        print(f"Error: {str(e)}")