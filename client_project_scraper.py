#!/usr/bin/env python
# coding: utf-8

# In[50]:


import requests
import pandas as pd
import os
import datetime as dt
import time
import boto3


# In[51]:


#define S3 resources
s3 = boto3.resource('s3')
metro_bucket = 'day1metroapi'


# In[52]:


# function to get vehicle locations by route
def get_vehicles_byroute(routenum):
    #set bucket directory
    subfolder = dt.datetime.today().strftime('%Y-%m-%d') + '/'
    
    #make request with route number
    resp = requests.get('http://api.metro.net/agencies/lametro/routes/%s/vehicles/' % routenum)
    
    #check if call is successful
    if resp.status_code != requests.codes.ok:
        return 
    
    #store json response as data
    data = resp.json()
    
    #convert json to dataframe
    routedata = pd.DataFrame(data['items'])
    
    #get current time
    now = dt.datetime.now()
    
    #add current time to as a value to dataframe "call_time"
    routedata['call_time'] = now
    
    #convert df back to json for storage
    routedata = routedata.to_json()
    
    #save JSON to S3 bucket
    s3.Object(metro_bucket, str(subfolder) + str(routenum) + '_' + now.strftime('%Y-%m-%d-%H-%M-%S') + '.json').put(Body=routedata)
    
    return 


# In[58]:


#function to call multipe routes
def get_routes(*routes):
    for route in routes:
        get_vehicles_byroute(routenum = route)
        time.sleep(3)
        
    return
        


# In[59]:


#run function for 13 hours (7am-8pm)
end_time = dt.datetime.now() + dt.timedelta(minutes=780)

while True:
    get_routes(212,780,217,2,10,16,17,18,20,720,108,754)
    
    #request every 2 minutes compensated for time between routes
    sleep_time = 45 - 3 * 12
    time.sleep(sleep_time)
    
    #break after 13 hours
    if dt.datetime.now() >= end_time:
        break

