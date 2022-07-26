# 1. For the log group in which you are interested get the names of all streams order by last event

ALL_CUSTOMERS = ['suncor', 'ford', 'fedex']

def is_relevant_stream(name):
    for x in ALL_CUSTOMERS:
        pass

'''
    streams[customer_name] = {
        'OCPP': [...],
        'CRON': [...],
        'PORTAL': [...]
    }
'''
stream_names = {}
should_stop = False

# Extract all names of all streams which might be relevant for my run
# (i.e. they need to have events which happened yesterday)
# The LF runs today at 00:00:00 for archiving yesterday's data
# Criteria is: find all those streams which had log events happening yesterday

while not should_stop:
    response = client.describe_log_streams(
        logGroupName='string',
        orderBy='LastEventTime',
        descending=True,
        nextToken='string',
        limit=123
    )

    if not response:
     break

    for item in response:
        if item['LastEventTime'] < Yesterday:
            should_stop = True
            break
        elif item['CreatedAtTime'] <= Yesterday and item['LastEventTime'] >= Yesterday:
            if is_relevant_stream(item['stream_name']):
                customer_name = extract_customer(item['stream_name'])
                stream_type = extract_stream_type(item['stream_name'])
                streams[customer_name] = {
                    'OCPP': [...],
                    'CRON': [...],
                    'PORTAL': [...]
                }

# stream_names will have the names of all the streams which have datapoints relevent to me
for customer in streams:
    result = process_ocpp(customer)
    save_result_to_s3(customer, component_type, date, result)

    process_cron(customer)
    process_portal(customer)

    client.filter_log_events(
        logGroupName='string',
        logStreamNames=customer['OCPP']
        startTime=Yesterday at 00:00:00,
        endTime=Yesterday at 23:59:59,
        nextToken='string',
        interleaved=True
    )
