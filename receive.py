
import pika
import pymysql
import json
import concurrent.futures


def callback(ch, method, properties, body):
    print(method.routing_key)
    print(" [x] Received %r" % body)
    body_str = body.decode("utf-8")
    print(body_str)
    data = json.loads(body)
    print(data)
    conn = pymysql.connect(
      user="pratham2901",
      password="Corona99",
      host="db",
      port=3306)
    cur = conn.cursor()

    query = "INSERT INTO corona.{0}(name, cases, deaths, recoveries, date) VALUES (%s, %s, %s, %s, %s)".format(method.routing_key)

    cur.execute(query,( data["name"],data["cases"],data["deaths"],data["recoveries"],data["date"]))
    conn.commit()
    conn.close()

def proc(que):
    credentials = pika.PlainCredentials('pratham2901', 'Corona99')
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',credentials=credentials))
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

