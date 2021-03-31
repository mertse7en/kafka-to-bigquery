import os, json
from consumer import ConsumerClass


def load_config():
    with open("config/config.json", encoding='utf-8') as config_contents: 
        config = json.load(config_contents)
        for cnf in config.keys():
            os.environ[cnf] = config[cnf]


if __name__ == '__main__':
    load_config()
    consumer = ConsumerClass()
    consumer.consume_msg()
