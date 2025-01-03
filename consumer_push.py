import logging

from confluent_kafka import Consumer
from message import MessageDeserializer


logger = logging.getLogger(__name__)


def check_messages():
    """
    Начать проверку сообщений
    """
    deserializer = MessageDeserializer()
    # Настройка консьюмера – адрес сервера
    conf = {
        "bootstrap.servers": "localhost:9094",
        "group.id": "push_group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
    }
    # создание консьюмера
    consumer = Consumer(conf)

    # подписка на топик
    consumer.subscribe(["messages"])
    # чтение сообщений в бесконечном цикле
    try:
        while True:
            # получение сообщений
            msg = consumer.poll(0.1)

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
    finally:
        # Закрытие консьюмера
        consumer.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # начать проверку сообщений
    check_messages()
