# main.py
import receive
import consume
import checker

def main():
    checker()  # Запускаем процесс обновления времени
    receive.start_receiving_process()  # Запускаем процесс приема данных из RabbitMQ
    consume.start_consuming_process()  # Запускаем процесс обработки принятых данных

if __name__ == "__main__":
    main()
