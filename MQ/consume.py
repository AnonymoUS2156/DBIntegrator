import pika
import psycopg2

# Подключение к RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Подключение к базе данных PostgreSQL
conn = psycopg2.connect("dbname=postgres user=postgres password=postgres host=10. port=5434")
cur = conn.cursor()

# Обработчик сообщений
def callback(ch, method, properties, body):
    # Получение данных из сообщения
    data = body.decode('utf-8').split(',')

    # Проверка типа события (insert, update, delete) по первому элементу в сообщении
    event_type = data[0]

    # Обработка различных типов событий
    if event_type == "INSERT":
        # Выполнение операции INSERT в базе данных
        cur.execute("INSERT INTO table2 (ID, NAME, COST) VALUES (%s, %s, %s)", (data[1], data[2], data[3]))

    elif event_type == "UPDATE":
        # Выполнение операции UPDATE в базе данных
        cur.execute("UPDATE table2 SET NAME = %s, COST = %s WHERE ID = %s", (data[2], data[3], data[1]))

    elif event_type == "DELETE":
        # Выполнение операции DELETE в базе данных
        cur.execute("DELETE FROM table2 WHERE ID = %s", (data[1],))

    conn.commit()

    # Подтверждение обработки сообщения
    ch.basic_ack(delivery_tag=method.delivery_tag)

# Подписка на очередь RabbitMQ
channel.basic_consume(queue='rabbitmq_queue', on_message_callback=callback)

# Начать прослушивание очереди
channel.start_consuming()
