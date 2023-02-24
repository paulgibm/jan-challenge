#!/usr/bin/env python3
import pika, sys, os
import boto3
import json
from zipfile import ZipFile
import io
import uuid

S3_EVENT_OBJ_CREATE="s3:ObjectCreated:Put"
S3_BCKT_UNPACKED="unpacked"


def main():

    s3 = boto3.client(                                      
        's3',                                               
        endpoint_url='http://minio-service:9000',
        aws_access_key_id='kxe679DiEl8n8jl7',
        aws_secret_access_key='AwPyMba1QgJdJf36Efw1eQDEKntvh4ZL',
        verify=False                                        
    )
    
    connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
    channel = connection.channel()

    channel.queue_declare(queue='unpacker-queue')

    def callback(ch, method, properties, body):
        bodyObj = json.loads(body.decode())
        eventType = bodyObj['EventName']
        print("eventType=" + eventType)
        if eventType == S3_EVENT_OBJ_CREATE:
            for record in bodyObj['Records']:
                bucketName = record['s3']['bucket']['name']
                objKey = record['s3']['object']['key']
                try:
                    #print("bucket=" + bucketName);
                    #print("objKey=" + objKey);
                    s3Obj = s3.get_object(Bucket=bucketName, Key=objKey)
                    #print("## s3Obj ##");
                    print(s3Obj)

                    print("Processing " + bucketName + "/" + objKey + "...")
                    #print("  Bucket name: " + bucketName);
                    #print("  Object Key: " + objKey);
                    #print("  Message: " + "Received");

                    s3ObjZip = ZipFile(io.BytesIO(s3Obj['Body'].read()))
                    #print("## s3ObjZip ##");
                    #print(s3ObjZip)
                    
                    #print(objKey + " contains the following files:")
                    for fileName in s3ObjZip.namelist():
                        fileContents = s3ObjZip.open(fileName).read()
                        s3.put_object(Body=fileContents, Bucket=S3_BCKT_UNPACKED, Key=fileName)
                        print(fileName + " added to unpacked bucket");

                except Exception as e:
                    print("Error:")
                    print("  Bucket name: " + bucketName);
                    print("  Object Key: " + objKey);
                    print("  Message: " + str(e));
                    print(e)
                    
        else:
            print("Not a create event. Event=" + eventType)

    channel.basic_consume(queue='unpacker-queue',
                      auto_ack=True,
                      on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    channel.start_consuming()

if __name__ == '__main__':
    try:
        main()

    except KeyboardInterrupt:
        print('Interrupted')
        try:
            sys.exit(0)
        except SystemExit:
            os._exit(0)
