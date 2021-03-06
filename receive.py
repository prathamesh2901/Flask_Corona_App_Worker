
import pika
import pymysql
import json
import concurrent.futures
from os import environ

DB_URL = environ["DB_URL"]
DB_USER = environ["DB_USER"]
DB_PASSWORD = environ["DB_PASSWORD"]
DB_NAME = environ["DB_NAME"]
MQ_USER = environ["MQ_USER"]
MQ_PASSWORD = environ["MQ_PASSWORD"]
MQ_URL = environ["MQ_URL"]

def callback(ch, method, properties, body):
    print(method.routing_key)
    print(" [x] Received %r" % body)
    body_str = body.decode("utf-8")
    print(body_str)
    data = json.loads(body)
    print(data)
    conn = pymysql.connect(
      user=DB_USER,
      password=DB_PASSWORD,
      host=DB_URL,
      port=3306)
    cur = conn.cursor()

    query = "REPLACE INTO {0}.{1}(name, cases, deaths, recoveries, date) VALUES (%s, %s, %s, %s, %s)".format(DB_NAME,method.routing_key)

    cur.execute(query,( data["name"],data["cases"],data["deaths"],data["recoveries"],data["date"]))
    conn.commit()
    conn.close()

def proc(que):
    credentials = pika.PlainCredentials(MQ_USER, MQ_PASSWORD)
    connection = pika.BlockingConnection(pika.ConnectionParameters(MQ_URL, credentials=credentials))
    channel = connection.channel()
    channel.queue_declare(queue=que,durable=True)

    channel.basic_consume(queue=que, auto_ack=True, on_message_callback=callback)

    print(' [*] Waiting for messages. To exit press CTRL+C')
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()

with concurrent.futures.ProcessPoolExecutor() as executor:
    ques = ['countries','states']
    for que in ques:
        executor.submit(proc, que)
