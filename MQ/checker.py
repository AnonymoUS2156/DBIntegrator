import psycopg2
import time
from consume import send_message

source_conn = psycopg2.connect(user="p**",
                               password="p**",
                               host="10.**.0.**",
                               port="54**",
                               database="po**")

source_cursor = source_conn.cursor()

# Получение начального значения первичного ключа
source_cursor.execute('SELECT MAX(ID) FROM tester')
lastPK = source_cursor.fetchone()[0]


try:
    while True:
        # Получение текущего максимального значения первичного ключа и соответствующих строк данных
        source_cursor.execute('SELECT ID, NAME, COST FROM tester WHERE ID > %s ORDER BY ID', (lastPK,))
        new_rows = source_cursor.fetchall()

        # Проверка наличия новых данных
        if new_rows:
            print("Выявлено добавление:")
            print("|ID|Name|Cost|")
            print("--------------------")
            for row in new_rows:
                curPK, name, cost = row
                print("|{curPK}|{name}|{cost}|".format(curPK=curPK, name=name, cost=cost))
                lastPK = curPK

                message = f"New data received: ID={curPK}, Name={name}, Cost={cost}"
                send_message(message)
        else:
            print("Изменений не выявлено, продолжаю наблюдение.")
            print("Для закрытия программы нажмите комбинацию клавиш CTRL+C")

        time.sleep(3)
except KeyboardInterrupt:
    print("\nProgram stopped by user.")

# Закрытие соединения
source_cursor.close()
source_conn.close()
