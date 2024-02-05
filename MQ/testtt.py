import psycopg2
import time
from consume import send_message

source_conn = psycopg2.connect(user="postgres",
                               password="postgres",
                               host="10.12.0.35",
                               port="5433",
                               database="postgres")
source_cursor = source_conn.cursor()

# Получение начального значения первичного ключа
source_cursor.execute('SELECT MAX(ID) FROM tester')
lastPK = source_cursor.fetchone()[0]

print(lastPK)
while True:
    # Получение текущего максимального значения первичного ключа и соответствующих строк данных
    source_cursor.execute('SELECT ID, NAME, COST FROM tester WHERE ID < %s ORDER BY ID', (lastPK,))

    new_rows = source_cursor.fetchall()
    
    if new_rows:
        print("Новые строки обнаружены:")
        print("|ID|Name|Cost|")
        print("--------------------")
        for row in new_rows:
            curPK, name, cost = row
            print("|{curPK}|{name}|{cost}|".format(curPK=curPK, name=name, cost=cost))
            lastPK = curPK
            
            # В этом месте можно вызвать функцию для отправки сообщения в RabbitMQ
            message = f"New data received: ID={curPK}, Name={name}, Cost={cost}"
            send_message(message)
    else:
        print("Новых строк не обнаружено.")
    
    time.sleep(5)

# Закрытие соединения
source_cursor.close()
source_conn.close()
