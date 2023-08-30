from datetime import datetime
from pickle import TRUE
from pathlib import Path
from flask import Flask, render_template, request, redirect, url_for, send_from_directory

import boto3, json, os, re, requests
 
app = Flask(__name__)

@app.route('/')
def index():
    print('Request for index page received')

    #dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    #aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])
    aws_secret_access_key=os.environ['AKIARHTAJAMP6IARJKMK'])    
    table = dynamodb.Table('humanDB')
    currDatas = table.scan()
    listData = currDatas['Items']

    size=0
    for currItem in listData:
        size=size+1

    return render_template('index.html', size=size)
    #return render_template('index.html')

@app.route('/favicon.ico')
def favicon():
    return send_from_directory(os.path.join(app.root_path, 'static'),
                               'favicon.ico', mimetype='image/vnd.microsoft.icon')

@app.route('/load', methods=['GET'])
def load():
    print('Refresh database')       
    
    filename = 'load.txt'
    # download the file from link 
    url = 'https://css490.blob.core.windows.net/lab4/input.txt'
    response = requests.get(url)

    with open(filename, 'wb') as f:
        f.write(response.content)

    print(f"File {filename} downloaded.")

    s3 = boto3.resource("s3")
    buckets = s3.buckets.all()
    bucketName="436prog4data"

    exist = 1
    for bucket in buckets:
        if bucketName == bucket.name:
            exist=0
    if exist==1:
        s3.create_bucket(Bucket=bucketName, CreateBucketConfiguration={"LocationConstraint":"us-west-2"})
        print("create new bucket with name: "+bucketName)
        
    bucket =  s3.Bucket(bucketName)

    file_name = os.path.join(os.path.dirname(filename), filename)
    bucket = s3.Bucket(bucketName)

    response = bucket.upload_file(file_name, filename)

    s3_client = boto3.client('s3')
    response = s3_client.put_object_acl(ACL='public-read', Bucket=bucketName, Key='load.txt')

    filename = 'data.txt'

    # download the file from link 
    url = 'https://436prog4data.s3.us-west-2.amazonaws.com/load.txt'
    response = requests.get(url)

    with open(filename, 'wb') as f:
        f.write(response.content)

    # convert from text to json
    # dictionary where the lines from text will be stored
    dict1 = {}

    # creating dictionary
    with open(filename) as fh:
 
        # count variable for human creation
        l = 1
     
        for line in fh:
         
            # reading line by line from the text file
            description = list(re.split(r'\s+',line.strip()))
 
            # for automatic creation of id for each employee
            sno ='hum'+str(l)

            # intermediate dictionary
            dict2 = {}

            # creating dictionary for each human
            dict2["firstname"]= description[1]
            dict2["lastname"]= description[0]

            # loop variable
            i = 2

            while i<len(description):
                    key, val = description[i].strip().split("=")
                    # creating dictionary for each employee
                    dict2[key]= val
                    i = i + 1
             
            # appending the record of each employee to
            # the main dictionary
 
            dict1[sno]= dict2
            l = l + 1

    # creating json file       
    ofile = open("data.json", "w")
    json.dump(dict1, ofile, indent = 4)
    ofile.close()

    #dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    tableName = 'humanDB'

    # Check if the table exists
    #tableL = boto3.client('dynamodb', region_name='us-west-2').list_tables()['TableNames']
    tableL = boto3.client('dynamodb', region_name='us-west-2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY']).list_tables()['TableNames']


    if tableName not in tableL:
        print(f"DynamoDB table {tableName} does not exist.")

        # create new table
        keySchema = [
            {'AttributeName': 'firstname', 'KeyType': 'HASH'},
            {'AttributeName': 'lastname', 'KeyType': 'RANGE'}
        ]
        attributeDefinitions = [
            {
                'AttributeName': 'firstname',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'lastname',
                'AttributeType': 'S'
            }
        ]
        provisionedThroughput = {
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
        table = dynamodb.create_table(
            TableName=tableName,
            KeySchema=keySchema,
            AttributeDefinitions=attributeDefinitions,
            ProvisionedThroughput=provisionedThroughput
        )

        try:
            table = dynamodb.Table(tableName)
        except dynamodb.meta.client.exceptions.ResourceNotFoundException:
            pass
        table.meta.client.get_waiter('table_exists').wait(TableName=tableName)

        table = dynamodb.Table('humanDB')

        with open('data.json') as f:
                itemsJ = json.load(f)

        # iterate through the data and put each item into the table
        for key, value in itemsJ.items():
            item = {}
            for sub_key, sub_value in value.items():
                    item[sub_key] = sub_value
            #print(item)
            table.put_item(Item=item)
        print('Data loaded into DynamoDB table.') 

    else:
        table = dynamodb.Table('humanDB')
        currDatas = table.scan()
        listData = currDatas['Items']

        with open('data.json') as f:
                itemsJ = json.load(f)
        
        for key, value in itemsJ.items():
            newItem = {}
            for sub_key, sub_value in value.items():
                newItem[sub_key] = sub_value
            #print('new')
            print(newItem)
            #print('-curr-')
            exist = False
            for currItem in listData:
                #print(currItem)
                #print(currItem['firstname'] +' '+currItem['lastname']+ " vs "+ newItem['firstname']+' '+newItem['lastname'])
                if currItem['firstname'].lower()==newItem['firstname'].lower() and currItem['lastname'].lower()==newItem['lastname'].lower():
                    for key in newItem:
                        if key not in currItem or newItem[key] != currItem[key]:
                            print('UPDATE ATRIBUTE')
                            response = table.update_item(
                                Key={
                                    'firstname': currItem['firstname'], 
                                    'lastname': currItem['lastname']
                                },
                                UpdateExpression='SET '+key+' = :newVal',
                                ExpressionAttributeValues={
                                    ':newVal': newItem[key]
                                },
                                ReturnValues="UPDATED_NEW"
                            )
                    exist = True
            if exist == False:
                print('ADD NEW ITEM')
                #print(newItem)
                table.put_item(Item=newItem)
            print('--------------')
    
    currDatas = table.scan()
    listData = currDatas['Items']

    size=0
    for currItem in listData:
        size=size+1

    return render_template('index.html', size=size)
    #return redirect(url_for('index'))

@app.route('/clear', methods=['GET'])
def clear():
    print('Clear database')  
    
    #dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    tableName = 'humanDB'

    table = dynamodb.Table(tableName)
    currDatas = table.scan()
    listData = currDatas['Items']

    print("BEFORE")
    for currItem in listData:
        print(currItem)

    for currItem in listData:
        response = table.delete_item(
            Key={
                'firstname': currItem['firstname'], 
                'lastname': currItem['lastname']
            }
        )

    print("AFTER")
    currDatas = table.scan()
    listData = currDatas['Items']

    size=0
    for currItem in listData:
        size=size+1
    
    return render_template('index.html', size=size)
    #return redirect(url_for('index'))

@app.route('/result', methods=['POST'])
def result():
    fname = request.form.get('fname')
    lname = request.form.get('lname')

    #dynamodb = boto3.resource('dynamodb', region_name='us-west-2')
    dynamodb = boto3.resource('dynamodb', region_name='us-west-2', aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'])

    tableName = 'humanDB'

    table = dynamodb.Table(tableName)
    currDatas = table.scan()
    listData = currDatas['Items']

    returnS = ""

    size=0
    for currItem in listData:
        size=size+1

    if fname or lname:
       if fname and lname:
            print('Search request received with fname lname = %s' % fname, lname)

            if size==0:
                returnS = "The database is empty!"
                return render_template('result.html', fname = fname, lname = lname, returnS = returnS)

            count=0            
            for currItem in listData:
                if currItem['firstname'].lower()==fname.lower() and currItem['lastname'].lower()==lname.lower():
                    returnS+=currItem['firstname']+" "+currItem['lastname']
                    for key in currItem:
                        if key!='firstname' and key!='lastname':
                            returnS+=" - "+key+": "+currItem[key]
                            count=count+1
                    returnS=returnS+"\n"

            if count==0:
                returnS = "There's nobody name "+fname+" "+lname
            
            return render_template('result.html', fname = fname, lname = lname, returnS = returnS)

       elif fname:
            print('Search request received with fname = %s' % fname)

            if size==0:
                returnS = "The database is empty!"
                return render_template('result.html', fname = fname, lname = 'X', returnS = returnS)

            count=0            
            for currItem in listData:
                if currItem['firstname'].lower()==fname.lower():
                    returnS+=currItem['firstname']+" "+currItem['lastname']
                    for key in currItem:
                        if key!='firstname' and key!='lastname':
                            returnS+=" - "+key+": "+currItem[key]
                            count=count+1
                    returnS=returnS+"\n"

            if count==0:
                returnS = "There's nobody that has first name "+fname

            return render_template('result.html', fname = fname, lname = 'X', returnS = returnS)
       else:
            print('Search request received with lname = %s' % lname)

            if size==0:
                returnS = "The database is empty!"
                return render_template('result.html', fname = 'X', lname = lname, returnS = returnS)

            count=0            
            for currItem in listData:
                if currItem['lastname'].lower()==lname.lower():
                    returnS+=currItem['firstname']+" "+currItem['lastname']
                    for key in currItem:
                        if key!='firstname' and key!='lastname':
                            returnS+=" - "+key+": "+currItem[key]
                            count=count+1
                    returnS=returnS+"\n" 

            if count==0:
                returnS = "There's nobody that has last name "+lname

            return render_template('result.html', fname = 'X', lname = lname, returnS = returnS)
    else:
       print('Request for hello page received with no name or blank name -- redirecting')
       return redirect(request.host_url)


if __name__ == '__main__':
   app.run()