import logging
from confluent_kafka import Consumer
from message import MessageDeserializer


logger = logging.getLogger(__name__)


def check_messages(timeout: float = 30.0):
    """
    Начать проверку сообщений
    """
    deserializer = MessageDeserializer()
    # настройка консьюмера – адрес сервера
    conf = {
        "bootstrap.servers": "localhost:9094",
        "group.id": "pull_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "fetch.min.bytes": 1024, # минимальный объём данных (в байтах), который консьюмер должен получить за один запрос к брокеру Kafka.
        "fetch.wait.max.ms": int(timeout * 1000) # Максимальное время ожидания
    }
    # создание консьюмера
    consumer = Consumer(conf)

    # Подписка на топик
    consumer.subscribe(["messages"])
    # чтение сообщений в бесконечном цикле
    try:
        while True:
            # получение сообщений
            msg = consumer.poll(timeout=timeout)

            if msg is None:
                continue
            if msg.error():
                logger.error(f"Ошибка: {msg.error()}")
                continue
            try:
                deserialized_msg = deserializer(msg.value())
            except Exception as e:
                logger.error(f"Ошибка десериализации: {str(e)}")
                continue
            logger.info(f"Получено сообщение: key={msg.key()}, value={deserialized_msg}, offset={msg.offset()}")
            consumer.commit(asynchronous=False)
    finally:
        # Закрытие консьюмера
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # начать проверку сообщений каждые 30 сек (по дефолту)
    check_messages()
