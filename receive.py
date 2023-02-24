#!/usr/bin/env python3
import pika, sys, os
import boto3
import json

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
        event = json.loads(body.decode())
        for record in event['Records']:
            bucketName = record['s3']['bucket']['name']
            objKey = record['s3']['object']['key']
            print("bucket=" + bucketName);
            print("objKey=" + objKey);

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
