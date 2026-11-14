# producer (Kareem)
Put `producer.py` here. It should send Hacker News posts to Kafka `topic1` at `localhost:9092`
and save a copy, one message per line, to a file path given as an argument (for example `../data/hn_messages.txt`).
The streaming app and training script accept plain text lines or JSON lines with `title` and/or `text`.
