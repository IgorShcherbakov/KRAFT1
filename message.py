from confluent_kafka.serialization import Serializer, Deserializer


class Message:
    """Класс для объекта сообщения.
    
    Attributes:
        id (int): идентификатор сообщения
        priority (int): приоритет сообщения
        subject (str): тема сообщения
        body (str): текст сообщения
        
    Example:
        >>> msg = Message(1, 10, "2023-01-01", "Тестовое сообщение")
        >>> print(msg.id)
        1
    """
    def __init__(self, id: int, priority: int, subject: str, body: str):
        self.id = id
        self.priority = priority
        self.subject = subject
        self.body = body

    def __str__(self) -> str:
        return f"id: {self.id}, priority: {self.priority}, subject: {self.subject}, body: {self.body}"


class MessageSerializer(Serializer):
    """
    Сериализатор для объектов Message
    """
    def __call__(self, obj: Message, ctx=None):
        id_bytes = obj.id.to_bytes(4, byteorder="big")

        priority_bytes = obj.priority.to_bytes(4, byteorder="big")

        subject_bytes = obj.subject.encode("utf-8")
        subject_size = len(subject_bytes)

        body_bytes = obj.body.encode("utf-8")
        body_size = len(body_bytes)

        result = id_bytes

        result += priority_bytes

        result += subject_size.to_bytes(4, byteorder="big")
        result += subject_bytes

        result += body_size.to_bytes(4, byteorder="big")
        result += body_bytes

        return result


class MessageDeserializer(Deserializer):
    """
    Десериализатор для объектов Message
    """
    def __call__(self, value: bytes, ctx=None):
        if value is None:
            return None

        id_bytes = value[0:4]
        id_value = int.from_bytes(id_bytes, byteorder="big")

        priority_bytes = value[4:8]
        priority_value = int.from_bytes(priority_bytes, byteorder="big")

        subject_size = int.from_bytes(value[8:12], byteorder="big")
        subject_bytes = value[12:12 + subject_size]
        subject_value = subject_bytes.decode("utf-8")

        body_size = int.from_bytes(value[12 + subject_size:16 + subject_size], byteorder="big")
        body_bytes = value[16 + subject_size:16 + subject_size + body_size]
        body_value = body_bytes.decode("utf-8")

        return Message(id_value, priority_value, subject_value, body_value)
