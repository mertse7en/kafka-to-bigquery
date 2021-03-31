import datetime
import sys, os
import pandas as pd

from confluent_kafka import Consumer,KafkaError
from confluent_kafka import Message

from bigquery_manager import BigQuerryManager
from tables import schema


class ConsumerClass():
    def __init__(self):
        self.biqquery_manager = BigQuerryManager()
        self.today = datetime.date.today()

    def consume_msg(self):
        c = Consumer({'bootstrap.servers': "bootstrap_server1,server2......",
        'group.id': "foo",
        "session.timeout.ms": 6000,
        'auto.offset.reset': 'latest'}) # Consumer starts consuming either ealiest offset or latest offset.
        
        c.subscribe(['merto_mart'])
        try:
            while True:
                df = pd.DataFrame(columns = ["name", "surname", "age"])
                empty_list = list()

                msg = c.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                print('Received message: {}, message offset: {}, topicname: {}'.format(msg.value().decode('utf-8'), msg.offset(), msg.topic()))
                
                for i in str(msg.value().decode('utf-8')).split(","):
                    print(i)
                    empty_list.append(i)
                df = df.append(pd.Series(empty_list, index=df.columns), ignore_index=True)
                df["date"] = self.today
                
                if(df.shape[1]!= 0):
                    self.biqquery_manager.push_to_bq(df, schema, 'kafka_test')
                    print("pushed succesfully")

                c.commit()
        except KeyboardInterrupt:
            sys.stderr.write('%% Aborted by user\n')

        finally:            
            c.close()


# c.poll returns a message object and takes timeout parameter -- c.poll(1.0)

# To read messages from a start offset to an end offset, you first need to use seek() to move the consumer at the desired starting location and then poll() until you hit the desired end offset.

    