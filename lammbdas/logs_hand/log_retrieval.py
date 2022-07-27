import json
import boto3
import random
import re 
import time
import datetime
import zipfile


customers = ["suncor", "fedex", "ford", "fordmx"]
component_names = ["ocpp", "portal", "cron"]
streams = {}    

SECRET_ARN = "arn:aws:secretsmanager:us-west-2:740241666524:secret:Historical_Archiving_LF_Secret"
SECRET_REGION = "us-west-2"

def get_secret():
    global SECRET_ARN, SECRET_REGION

    secret_name = SECRET_ARN
    region_name = SECRET_REGION

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
    except ClientError as e:
        raise e

    if 'SecretString' in get_secret_value_response:
        return json.loads(get_secret_value_response['SecretString'])
    else:
        return None


def lambda_handler(event, context):
    # TODO implement
    global streams
    streams = {}
    AWS_REGION = "us-west-1"

    client = boto3.client('logs', region_name=AWS_REGION)
    #tz = pytz.timezone('US/Pacific')
    now = datetime.datetime.now(datetime.timezone.utc)
    today_start = datetime.datetime(now.year, now.month, now.day, 0, 0, 0, 0)
    yesterday_start = today_start - datetime.timedelta(days=1)
    yesterday_starttime = int(yesterday_start.timestamp()*1000)
    today_end = datetime.datetime(now.year, now.month, now.day, 23, 59, 59, 0)
    yesterday_end = today_end - datetime.timedelta(days=1)
    yesterday_endtime = int(yesterday_end.timestamp()*1000)
    print(today_start)
    print(today_end)
    print(yesterday_start)
    print(yesterday_end)
   
   
    LOG_GROUP_NAME='/aws/containerinsights/btc-prod/application'
   
    print(f'streams before {streams}')        
    
    Describe_logstreams(LOG_GROUP_NAME, yesterday_starttime, yesterday_endtime)
    print(f'streams after {streams}')
    
    split_log_groups_by_cust_comp_limit(LOG_GROUP_NAME,streams,yesterday_starttime,yesterday_endtime,yesterday_start)
    

def Describe_logstreams(log_group_name, yesterday_starttime, yesterday_endtime):
    client = boto3.client('logs')
    next_token = ''
    response = {}
        
    while True:
        if next_token == '':
            response = client.describe_log_streams(
                 logGroupName=log_group_name,
                 orderBy='LastEventTime',
                 descending=True
                
            )
        else:
            response = client.describe_log_streams(
                logGroupName=log_group_name,
                 orderBy='LastEventTime',
                 descending=True,
                
                nextToken=next_token
            )
        should_stop = Process_Logs(response,yesterday_starttime,yesterday_endtime)
        if 'nextToken' in response and should_stop == False:    
            next_token = response['nextToken']
        else:
            break           
        
        
        
        
#filterlogs logStreamNames: [ "string" ] has a limit of 100 elements per list, thats why in case that happens in needs to be devided in chunks of 100's

def split_log_groups_by_cust_comp_limit(LOG_GROUP_NAME,streams,yesterday_starttime,yesterday_endtime,yesterday_start):
    for i in customers:
        if i in streams:
            #print(f'customer  {i}')
            for com in component_names:
                if com in streams[i]:
                    print(com)
                    Streamsby_cust_comp = streams[i][com]                
                    split_lists = [Streamsby_cust_comp[x:x+100] for x in range(0, len(Streamsby_cust_comp), 100)] 
                    #print(split_lists)
                    retrieve_events(LOG_GROUP_NAME,split_lists,yesterday_starttime,yesterday_endtime,i,com,yesterday_start)
                    
                    
    
def retrieve_events(log_group_name,split_lists, yesterday_starttime,yesterday_endtime,customer,component,yesterday_start):
    client = boto3.client('logs')
    LogsInfoList = []      
    count = 0
    count2 = 0
    for streamgroup in split_lists:
       # print (streamgroup)
        next_token = ''
        response = {}
        while True:
                if next_token == '':
                    response = client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=streamgroup,
                    startTime=yesterday_starttime,
                    endTime=yesterday_endtime)
           
                else:
                    response = client.filter_log_events(
                    logGroupName=log_group_name,
                    logStreamNames=streamgroup,
                    startTime=yesterday_starttime,
                    endTime=yesterday_endtime,
                    nextToken=next_token)
                #print (response['events'])       
                for i in response['events']:
                    mesg = i['message']
                    tmstp = i['timestamp']
                    logOnly = json.loads(mesg)
                    LogsInfoList.append((logOnly['log'], tmstp))
                    #LogsInfoList.append(logOnly['log'])
                    count = count + 1
                    count2 = count2 + 1
                if 'nextToken' in response:
                    next_token = response['nextToken']   
                   # print(f'Records by call {count2}  ')
                    count2 = 0
                else:
                    break
                
    Textfile = f"{customer}_primary_{component}_{yesterday_start.strftime('%Y_%m_%d')}_logs.txt"
    Logfile = '/tmp/' + Textfile        
    LogsInfo = open(Logfile,"w+")
    #LogsInfoList.sort(key=str.lower)
    LogsInfoList.sort(key=lambda y: y[1])
    for line in LogsInfoList:
        LogsInfo.write(line[0])
    LogsInfo.close()

    secret = get_secret()
    if not secret:
        print('no secret')
        return
    
    secretkey = secret['CRON_DEV_API_KEY']
    
    s3 = boto3.client('s3', aws_access_key_id='AKIA2YWOXOHOC4BDJ4MU', aws_secret_access_key=secretkey, region_name="us-west-1")
    
    bucket = 'btc-mon-logs-dev'
    print(f'Records to write {count}')     
    if count > 0:
        file_name = f"{customer}_primary_{component}_{yesterday_start.strftime('%Y_%m_%d')}_logs.zip"
        print(yesterday_start.month)
        folder = f'{customer}/primary/{component}/{yesterday_start.year}/{yesterday_start.month}/'
        resp = s3.list_objects(Bucket = bucket, Prefix=folder )
        exists = False
        if 'Contents' in resp:
            exists = True
        else:
            response = s3.put_object(Bucket = bucket, Body = '', Key = folder )
        print(f'here{folder.format(file_name)}')    
        object_file = folder + file_name
        print(object_file)
        file = zipfile.ZipFile('/tmp/zipfile.zip', 'w').write(f'/tmp/{Textfile}',arcname=Textfile)
        s3.upload_file('/tmp/zipfile.zip' , bucket, object_file)
        
    
    

def Process_Logs(response,yesterday_starttime,yesterday_endtime):
    
    Groupoflogs = response['logStreams']
    should_stop = False 
    count = 0
    for item in Groupoflogs:
        count = count + 1
        if item['lastEventTimestamp'] < yesterday_starttime:
            should_stop = True
            return should_stop
        elif ((item['lastEventTimestamp'] >= yesterday_starttime and item['creationTime'] <= yesterday_endtime)
              or
             (item['lastEventTimestamp'] <= yesterday_endtime and item['creationTime'] > yesterday_endtime)):   
            #It's a relevant Logstream     
             component_found = False
             for component in component_names:
                 FirstStr = re.compile(fr'{component}-deployment')
                 if FirstStr.search(item['logStreamName']):
                    component_found = True
                    break
             if component_found:
                 for customer in customers:
                     SecndStr = re.compile(fr'_prod-{customer}_{component}')
                     if SecndStr.search(item['logStreamName']):
                              create_relevant_streams(customer,component,item['logStreamName'])
                              break
    #print(f'streamchecked {count}')
    return should_stop      
    
    
def create_relevant_streams(customer,component,StreamName):
    
    if customer not in streams:
        streams[customer] = {}
        streams[customer][component] = []
        streams[customer][component].append(StreamName)
    elif component not in streams[customer]:
        streams[customer][component] = []
        streams[customer][component].append(StreamName)
    else:
        streams[customer][component].append(StreamName)
    