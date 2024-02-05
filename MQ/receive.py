import psycopg2
import pika

def receive_data_from_queue():
    try:
        # Подключение к RabbitMQ
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='integration_queue')

        # Подключение к базе данных
        dest_conn = psycopg2.connect(user="postgres",
                                       password="postgres",
                                       host="10.12.0.35",
                                       port="5434",
                                       database="postgres")
        dest_cursor = dest_conn.cursor()

        # Получение данных из RabbitMQ
        def callback(ch, method, properties, body):
            print("Received from queue:", body)
            record = eval(body.decode())
            # Сохранение данных в destination_table
            dest_cursor.execute("INSERT INTO tester2 VALUES (%s, %s, %s)", record)
            dest_conn.commit()

        channel.basic_consume(queue='integration_queue',
                              on_message_callback=callback,
                              auto_ack=True)

        print('Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()

        # Закрытие соединений
        dest_cursor.close()
        dest_conn.close()
        connection.close()

    except Exception as error:
        print("Error while connecting to RabbitMQ or PostgreSQL:", error)

receive_data_from_queue()
