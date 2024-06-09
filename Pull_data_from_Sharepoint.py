from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
import boto3
import os
import datetime
import pytz

###  Global variables
website = os.environ['website']
bucket_name = os.environ['bucketname']

######################## USER DEFINED FUNCTIONS ############################ 

# Function to convert month text to month number
def findmonth(month):
    months = ["January","February","March","April","May","June","July","August",
    "September","October","November","December"] 
    return months[month-1]

# Function to check if bucket exists and create bucket if it does not exists    
def check_create_bucket(bucket_name):
    # Create boto3 client for s3
    s3client = boto3.client('s3')
    # Get the list of buckets in s3
    s3response = s3client.list_buckets()
    # Extract the bucket names from the s3response and store it as a list
    bucketlist = []
    for i in range(0,len(s3response["Buckets"])):
        bucketlist.append(s3response["Buckets"][i]["Name"])
    
    # If bucket name does not exists then create bucket otherwise skip
    if bucket_name not in bucketlist:
        response = s3client.create_bucket(
            Bucket= bucket_name,
            CreateBucketConfiguration={
                'LocationConstraint': 'ap-south-1'
            },
        )
# Function to get todays date        
def get_todays_date():
    
    # Get today's date e.g. 2022-10-01
    date = pytz.timezone('Asia/Kolkata').localize(datetime.now())
    # Extract year
    year = str(date.year)
    # Month is available in form of integer
    # To convert it into word findmonth function is used
    month = findmonth(date.month)
    # Extract day in required format
    day = "Day"+str(date.day)
    
    return date,year,month,day
    
# Function for SharePoint Authentication    
def SharePoint_Authentication(website,username,password):
    # Sharepoint Authentication
    authcookie = Office365(website, username, password).GetCookies()
    site = Site(website+'/sites/Developer', version=Version.v365, authcookie=authcookie)
    return site

# Function to copy files from SharePoint to S3 bucket
def upload_files_S3(allfiles,sharepoint_path,bucket_name,dest_path):
    
    # Create a S3 client to upload the object
    s3client = boto3.client('s3')
    # allfiles variable is having list of dictionaries, 
    # each dictionary is associated with each file  
    for i in range(len(allfiles)):      # len(allfiles) gives no. of files in folder
        
        response = s3client.put_object(
            
            # file to be pulled from sharepoint
            # allfiles[i]['Name'] specifies filename
            Body= sharepoint_path.get_file(allfiles[i]['Name']),   
            
            # Specify the bucket name
            Bucket=bucket_name,   
            
            # Specify the destination path
            Key=dest_path+allfiles[i]['Name']
        )

# Function to get credentials from AWS Secrets Manager
def get_credentials(secret_name,username_key,password_key):
    smclient = boto3.client('secretsmanager')
    response = smclient.get_secret_value(SecretId=secret_name)
    credentials = json.loads(response['SecretString'])
    username = credentials[username_key]
    password = credentials[password_key]
    return username, password

################   END OF USER DEFINED FUNCTIONS  #####################

def lambda_handler(event, context):
    # get credentials from AWS Secrets Manager
    username,password = get_credentials('Sharepoint_Credentials','sharepoint_username','sharepoint_password')

    site = SharePoint_Authentication(website,username,password)
    
    # Get todays date    
    date,year,month,day = get_todays_date()
    
    # common path required is stored in the variable com_path
    com_path = year+'/'+month+'/'+day
    
    # Path to access files from sharepoint
    sharepoint_path = site.Folder('Shared Documents/'+com_path)
    
    # Gives the list of dictionaries having information about files
    allfiles = sharepoint_path.files
    
    # Directory structure required in s3 bucket
    dest_path = com_path+'/'
    
    # Upload all files to s3
    upload_files_S3(allfiles,sharepoint_path,bucket_name,dest_path)