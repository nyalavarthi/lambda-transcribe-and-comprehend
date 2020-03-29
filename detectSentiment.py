from __future__ import print_function
from botocore.vendored import requests
import json
import time
import urllib.parse
import boto3
import os.path
import random 

dynamodb = boto3.client('dynamodb')

# Author : Narendra Yalavarthi
# Description : 
# 1. Listens to S3 bucket put events 
# 2. Converts the audio file into text using transcribe service 
# 3. Detects the sentiment from the text using comprehend service
# 4. Sends email using SNS service
# 5. Save results to a DynamoDB table

# Initialize SNS client for US EST
session = boto3.Session(
    region_name="us-east-1"
)
sns_client = session.client('sns')

#sns = boto3.client('sns')

transcribe = boto3.client('transcribe')


s3 = boto3.client('s3')

def lambda_handler(event, context):
     # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    print('Bucket name : ', bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print('Key name : ', key)
    extension = os.path.splitext(key)[1][1:]
    file_noext = os.path.splitext(key)[1][0:]
    job_name = "Transcribe_"+file_noext
    
    print('extension ', extension)
    
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        stream = response['Body'].read()
        
        job_uri =  "https://s3.amazonaws.com/"+bucket+"/"+key
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat=extension,
            LanguageCode='en-US'
        )
        print('started transcrription job ')
        while True:
            status = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            if status['TranscriptionJob']['TranscriptionJobStatus'] in ['COMPLETED', 'FAILED']:
                break
            print("Not ready yet...")
            time.sleep(5)
        print(status)
        
        resText = requests.get(status['TranscriptionJob']['Transcript']['TranscriptFileUri']).json()['results']['transcripts'][0]['transcript']
        print( resText )
        
        #detect mood of the conversation using AWS Comprehend
        comprehend = boto3.client(service_name='comprehend', region_name='us-east-1')
        print('Calling DetectSentiment')
        json_text  = comprehend.detect_sentiment(Text=resText, LanguageCode='en')
        #print(json.dumps(json_text, sort_keys=True, indent=4))
        print('End of DetectSentiment\n')
        sentiment = json_text['Sentiment']
        #Send SNS notification 
        sns_client.publish(
            TopicArn = 'arn:aws:sns:us-east-1:XXXXXXX:Narendra-email',
            Subject = 'AWS - Connect found an unhappy customer: ' ,
            Message =  'Message :' + resText + '\n\n' + 'Sentiment : ' + sentiment
        )
         # TODO implement
        print('Sentiment is : ' + sentiment)
        key_id = str(random.randrange(0, 201, 2))
        #save results to a DymanoDB table
        dynamodb.put_item(TableName='connect-call-transcription', Item={'call_id':{'S': key_id}, 'file_name' : {'S' : key} ,'callTranscription':{'S' : resText},'sentiment':{'S' : sentiment}})

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e    
    
    finally:
        #delete the existing job
        try:
            response = transcribe.delete_transcription_job(
                TranscriptionJobName = job_name
            )
        except Exception as e1:
            print(e1)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!, Sentiment is ' +  json_text['Sentiment'])
    }
