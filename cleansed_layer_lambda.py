import boto3
import pandas as pd
import os
import datetime
import io
import phonenumbers
import re
import sys
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv,
                          ['cleansedlayerbucket',
                           'rawlayerbucket',
                           'dynamodb_table_headers'])

# Create boto3 client and resource for s3
s3client = boto3.client('s3')
s3resource = boto3.resource('s3')
# Create a boto3 client for dynamoDB
dynamodbclient = boto3.client('dynamodb')
cleansed_bucket = args['cleansedlayerbucket']
raw_bucket = args['rawlayerbucket']
dynamodb_headers_table = args['dynamodb_table_headers']

##########################################
# Function to convert dataframe to parquet and upload
def upload_parquet_to_s3(out_buffer, prefix, name):
    # Upload the parquet file to cleansed layer bucket
    s3client.put_object(Bucket=cleansed_bucket,
                        Key='success/' + prefix + '/' + name + '.parquet',
                        Body=out_buffer.getvalue())

##########################################
# Function to convert dataframe to csv and upload (only for rejected emails and phone numbers)
def upload_csv_to_s3(out_buffer, prefix, filename):
    # Upload the excel file to cleansed layer bucket
    s3client.put_object(Bucket=cleansed_bucket,
                        Key='reject/' + prefix + '/' + filename + '.csv',
                        Body=out_buffer.getvalue())

# Function to upload excel files to s3
def df_to_csv(df):
    out_buffer = io.BytesIO()
    df.to_csv(out_buffer, index=False)
    return out_buffer  # out_buffer has the excel data to be written in excel file

##########################################
# Function to get todays date
def get_todays_date():
    # Get today's date e.g. 2022-10-01
    date = datetime.date.today()
    # Extract year
    year = str(date.year)
    # Month is available in form of integer
    # To convert it into word findmonth function is used
    month = findmonth(date.month)
    # Extract day in required format
    day = "Day" + str(date.day)

    return date, year, month, day

##########################################
# Function to convert month text to month number
def findmonth(month):
    months = ["January", "February", "March", "April", "May", "June", "July", "August",
              "September", "October", "November", "December"]
    return months[month - 1]

##########################################
# Function to get file list from raw layer current day folder
def get_file_list(prefix):
    file_list_response = s3client.list_objects(
        Bucket=raw_bucket,
        Prefix=prefix)
    files = []
    # Extract the key from the response
    for i in range(len(file_list_response['Contents'])):
        key = file_list_response['Contents'][i]['Key']
        # key will have full path of the file
        # Extract the filename from the path
        # e.g. 2022/October/Day12/regions.csv
        files.append(key[len(prefix) + 1:])
    return files, file_list_response

##########################################
# Function to check the contents of df are empty or non-empty
def validate_file_size_from_df(df):
    rows, cols = df.shape
    if rows != 0 and cols != 0:
        pass
    else:
        raise Exception('File is empty')

##########################################
# Function to validate file headers
def validate_headers(valid_file, df):
    # Seperate the filename and extension
    file_name, ext = valid_file.split('.')
    response1 = dynamodbclient.get_item(
        TableName=dynamodb_headers_table,
        Key={
            'table_names': {
                'S': file_name
            }
        }
    )
    dynamo_headers = list(response1['Item']['schema']['M'].keys())
    primary_key = list(response1['Item']['primary_key'].values())
    # Convert index data type to list
    column_names = df.columns.tolist()  # ['employee_id, first_name' ]
    # for CSV files the headers are returned as a single string
    if len(column_names) == 1:
        column_names = column_names[0].split(',')
    if len(column_names) == len(dynamo_headers):
        column_names.sort()
        dynamo_headers.sort()
        if column_names == dynamo_headers:
            return primary_key
        else:
            raise Exception('Headers mismatch in {}'.format(valid_file))
    else:
        raise Exception('Headers mismatch')

##########################################
# Function to make column headers in lower case and
# strip white space from start and end
def cleanse_column_headers(df):
    # Get all the column names in col_headers
    col_headers = df.columns
    column_names = []
    for i in range(len(col_headers)):
        # Remove the white space from front and end of the column name
        col_name = col_headers[i].strip()
        # make the column name in lower case
        column_names.append(col_name.lower())
    # Assign the validated column names to the dataframe
    df.columns = column_names
    return df, column_names

##########################################
# Function to create dataframe from file
def create_df_from_file(file, prefix):
    key = prefix + '/' + file
    obj = s3resource.Object(raw_bucket, key)
    response = obj.get()
    data = io.BytesIO(response['Body'].read())
    df = pd.DataFrame()
    # extract the filename and extension of the file
    filename, extention = file.split(".")
    # For excel files
    if extention == "xlsx":
        df = pd.read_excel(data)
    # For csv files
    if extention == "csv":
        df = pd.read_csv(data, delimiter=",",
                         error_bad_lines=False,
                         encoding='utf-8')
    return df, filename

##########################################
# Function to upload valid files to s3 in parquet format
def df_to_parquet(df):
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    return out_buffer  # out_buffer has the parquet data to be written in parquet file

##########################################
# Function to replace special characters by underscore
def remove_special_chars(cleansed_df):
    cols = cleansed_df.columns
    # List of special charaters, new charaters can be added in this list
    special_chars = ['@', '-']
    for i in special_chars:
        for j in range(len(cols)):
            if i in cols[j]:
                cols[j] = cols[j].replace(i, '_')
    cleansed_df.columns = cols
    return cleansed_df

##########################################
# Function for validiting email
def validate_email_id(cleansed_df):
    regex = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    column_names = cleansed_df.columns
    # Following code will be executed only if email column is present in the dataframe
    if 'email' in column_names:
        rows, columns = cleansed_df.shape
        for i in range(rows):
            # Extract emails one by one from emails column
            email = cleansed_df.loc[i, 'email']
            if re.fullmatch(regex, email):
                # add temporary column named valid_email
                # insert true if email is valid and false otherwise
                cleansed_df.loc[i, 'valid_email'] = True
            else:
                cleansed_df.loc[i, 'valid_email'] = False
        # Extract and store invalid emails in invalid_df
        invalid_df = cleansed_df[cleansed_df['valid_email'] == False]
        invalid_df = invalid_df.drop(['valid_email'], axis=1)
        # Remove the added column valid_email
        cleansed_df = cleansed_df.drop(['valid_email'], axis=1)
        # If invalid emails are present then indicate it with a flag
        flag = "Invalid emails"
        return cleansed_df, invalid_df, flag
    else:
        flag = "No invalid emails"
        return cleansed_df, None, flag

##########################################
# Function for validating phone numbers
def validate_phone_numbers(cleansed_df):
    column_names = cleansed_df.columns
    # Execute the following code only if phone_number column is present in the dataframe
    if 'phone_number' in column_names:
        rows, columns = cleansed_df.shape
        for i in range(rows):
            # Extract phone number one by one from phone_number column
            ph_number = str(cleansed_df.loc[i, 'phone_number'])
            ph_number = "+" + ph_number
            number = phonenumbers.parse(ph_number)
            if phonenumbers.is_valid_number(number):
                # add temporary column named valid_phone_number
                # insert true if phone number is valid and false otherwise
                cleansed_df.loc[i, 'valid_phone_number'] = True
            else:
                cleansed_df.loc[i, 'valid_phone_number'] = False

        invalid_df = cleansed_df[cleansed_df['valid_phone_number'] == False]
        invalid_df = invalid_df.drop(['valid_phone_number'], axis=1)
        cleansed_df = cleansed_df.drop(['valid_phone_number'], axis=1)
        flag = "Invalid Phone Numbers"
        return cleansed_df, invalid_df, flag
    else:
        flag = "Valid Phone Numbers"
        return cleansed_df, None, flag

##########################################
# Function for Primary Key validation
def primary_key_validation(cleansed_df, primary_key, prefix):
    column_names = cleansed_df.columns
    if primary_key[0] in column_names:
        # Add a temporary column named duplicates
        # add a true value if the entry in column is duplicate
        cleansed_df['duplicates'] = cleansed_df.duplicated(primary_key, keep=False)
        # Extract the duplicate records in duplicate_records_df
        duplicate_records_df = cleansed_df[cleansed_df['duplicates'] == True]
        duplicate_records_df = duplicate_records_df.drop(['duplicates'], axis=1)
        if duplicate_records_df.shape[0] != 0:
            filename = 'duplicate_records'
            # Optionally the duplicate records can be uploaded to an excel file
            upload_reject_records(duplicate_records_df, prefix, filename)
            # raise an exception if duplicate records are found
            raise Exception('Duplicates found in {} (primary key) column'.format(primary_key))

##########################################
# Function to upload rejected email, phone number and duplicate records
def upload_reject_records(df, prefix, filename):
    if df.shape[0] != 0:
        out_buffer = df_to_csv(df)  # change to csv
        upload_csv_to_s3(out_buffer, prefix, filename)

##########################################

# Get todays date
date, year, month, day = get_todays_date()
# Common path required, is stored in the variable prefix
prefix = year + '/' + month + '/' + day
# Get the list of files in the current day folder
files, file_list_response = get_file_list(prefix)
files = files[1:]
# The following loop will run for every valid file
for i in range(len(files)):
    df, filename = create_df_from_file(files[i], prefix)
    validate_file_size_from_df(df)
    # Validate headers with headers from dynamodb
    primary_key = validate_headers(files[i], df)
    # Remove white space and convert to smallcase
    cleansed_df, column_names = cleanse_column_headers(df)
    # Replace special characters like @, - from header names with _
    cleansed_df = remove_special_chars(cleansed_df)
    # Validate emails
    cleansed_df, invalid_email_df, flag = validate_email_id(cleansed_df)
    # Upload invalid email records to reject folder
    if flag == "Invalid emails":
        file_name = 'rejected_email_records'
        upload_reject_records(invalid_email_df, prefix, file_name)
    # Validate Phone numbers
    cleansed_df, invalid_phone_num_df, flag = validate_phone_numbers(cleansed_df)
    if flag == "Invalid Phone Numbers":
        file_name = 'rejected_phone_number_records'
        upload_reject_records(invalid_phone_num_df, prefix, file_name)
    # Specify the column name as primary key
    # Program will error out if duplicate entries are found
    primary_key_validation(cleansed_df, primary_key, prefix)
    # Convert valid and cleansed df to parquet
    out_buffer = df_to_parquet(cleansed_df)
    # Upload parquet file to Cleansed Bucket
    upload_parquet_to_s3(out_buffer, prefix, filename)