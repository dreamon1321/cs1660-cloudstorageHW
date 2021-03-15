#!/usr/bin/env python
# coding: utf-8

# In[66]:

import boto3


# In[67]:


s3 = boto3.resource('s3',
                   aws_access_key_id = 'enter-your-access-key',
                   aws_secret_access_key = 'enter-your-secret-access-key'
)


# In[68]:


try:
    s3.create_bucket(Bucket = 'amichaelbucket', 
                     CreateBucketConfiguration = {
                         'LocationConstraint': 'us-west-2'}
    )
    print('bucket creation was successful')
except:
    print('you already created this bucket!')


# In[69]:


bucket = s3.Bucket("amichaelbucket")


# In[70]:


bucket.Acl().put(ACL = 'public-read')


# In[71]:


body = open('test.txt', 'rb')


# In[72]:


o = s3.Object('amichaelbucket', 'exp1test').put(Body = body)


# In[73]:


s3.Object('amichaelbucket', 'exp1test').Acl().put(ACL = 'public-read')


# In[74]:


dyndb = boto3.resource('dynamodb',
                      region_name = 'us-west-2',
                      aws_access_key_id = 'enter-your-access-key',
                      aws_secret_access_key = 'enter-your-secret-access-key'
)


# In[75]:


try:
    table = dyndb.create_table(
            TableName = 'ExperimentDataTable',
            KeySchema = [
                {
                    'AttributeName': 'PartitionKey',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'RowKey',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions = [
                {
                    'AttributeName': 'PartitionKey',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'RowKey',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput = {
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
    )
except:
    table = dyndb.Table("ExperimentDataTable")


# In[76]:


table.meta.client.get_waiter('table_exists').wait(TableName = 'ExperimentDataTable')


# In[77]:


print(table.item_count)


# In[78]:


import csv


# In[79]:


with open(r'C:\Users\andre\Desktop\Spring2021\CloudComp\cs1660-cloudstorageHW\experiments.csv', 'r', encoding="utf-8") as csvfile:
    csvf = csv.reader(csvfile, delimiter = ',', quotechar = '|')
    for item in csvf:
        print(item)
        if '.csv' in str(item[4]):
            body = open('C:\\Users\\andre\\Desktop\\Spring2021\\CloudComp\\cs1660-cloudstorageHW\\datafiles\\'+item[4],'rb')
            s3.Object('amichaelbucket', item[4]).put(Body = body)
            md = s3.Object('amichaelbucket', item[4]).Acl().put(ACL='public-read')
            url = "https://s3-us-west-2.amazonaws.com/amichaelbucket/"+item[4]
            metadata_item = {
                'PartitionKey': item[0],
                'RowKey': item[1],
                'description': item[4],
                'date': item[2],
                'url': url
            }
        try:
            table.put_item(Item = metadata_item)
        except:
            print('item may already be here')


# In[80]:


response = table.get_item(
    Key = {
        'PartitionKey': 'experiment2',
        'RowKey': 'data2'
    }
)
item = response['Item']
print(item)


# In[81]:


response

