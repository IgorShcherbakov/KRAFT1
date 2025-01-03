# KRAFT1 
Итоговый проект первого модуля (Настройка кластера и реализация продюсера с двумя консьюмерами).

## Установка
1. Клонировать репозиторий
2. Создать виртуальное окружение
```bash
python -m venv venv
```
3. Активировать виртуальное окружение
```bash
venv\Scripts\Activate.ps1
```
4. Установить зависимости
```bash
pip install -r requirements.txt
```
5. Запустить Kafka в Docker
```bash
docker-compose up -d
```
6. Подключиться к контейнеру и создать топик
```bash
kafka-topics.sh --create --topic messages --bootstrap-server localhost:9092 --partitions 3 --replication-factor 2
```

### Примеры использования
```bash
# запуск producer
python producer.py

# запуск consumer с pull-моделью
python consumer_pull.py

# запуск consumer с push-моделью
python consumer_push.py
```

После запуска можно заметить что продюсер генерирует сообщения каждые 5 сек, консьюмер с моделью push сразу же забирает сгенерированные сообщения, а консьюмер с моделью pull собирает по несколько сообщений каждые 30 сек.