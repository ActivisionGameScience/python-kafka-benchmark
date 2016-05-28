import time

from tqdm import tqdm
from pykafka import KafkaClient

bootstrap_servers = 'localhost:9092'

def pykafka_producer_performance(use_rdkafka=False): 
    client = KafkaClient(hosts=bootstrap_servers)
    topic = client.topics[b'pykafka-test']
    p = topic.get_producer(use_rdkafka=use_rdkafka)

    t_produce_start = time.time()
    msgs_produced = 0
    msgs_backpressure = 0

    msgcnt = 1000000
    msgsize = 100
    msg_pattern = 'test.py performance'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize].encode()

    with tqdm(total=msgcnt) as pbar:
        for i in range(msgcnt):
            p.produce(msg_payload)
            msgs_produced += 1
            if msgs_produced % 1000 == 0:
                 pbar.update(1000)
        p.stop()
        pbar.update(0)

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))


def pykafka_consumer_performance(use_rdkafka=False): 
    client = KafkaClient(hosts=bootstrap_servers)
    topic = client.topics[b'pykafka-test']
    c = topic.get_simple_consumer(use_rdkafka=use_rdkafka)

    target_count = 1000000
    msg_count = 0
    
    with tqdm(total=target_count) as pbar:
        while True:
            msg = c.consume()
            if msg:
                msg_count += 1

                if msg_count % 1000 == 0:
                    pbar.update(1000)

            if msg_count >= target_count:
                break


from kafka import KafkaProducer

def python_kafka_producer_performance():
    print("python-kafka producer test")
    p = KafkaProducer(bootstrap_servers=bootstrap_servers)

    t_produce_start = time.time()
    msgs_produced = 0
    msgs_backpressure = 0

    topic = 'python-kafka-topic'
    msgcnt = 1000000
    msgsize = 100
    msg_pattern = 'test.py performance'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize].encode()

    with tqdm(total=msgcnt) as pbar:

        for i in range(msgcnt):
            p.send(topic, msg_payload)
            msgs_produced += 1
            if msgs_produced % 1000 == 0:
                 pbar.update(1000)
        p.flush()

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))

from kafka import KafkaConsumer

def python_kafka_consumer_performance():
    print("python-kafka consumer test")
    topic = 'python-kafka-topic'

    consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers)
    target_count = 1000000
    msg_count = 0
    
    t_produce_start = time.time()

    with tqdm(total=target_count) as pbar:
        consumer.subscribe([topic])
        for msg in consumer:
            msg_count += 1
            if msg_count % 1000 == 0:
                pbar.update(1000)
            if msg_count >= target_count:
                break

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    print('# consumer %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (msgs_consumed, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))

if __name__ == "__main__":

    # producer_proformance(use_rdkafka=False)

    # time.sleep(10)
    #pykafka_producer_proformance(use_rdkafka=True)

    # pykafka_consumer_proformance(use_rdkafka=False)
    # pykafka_consumer_proformance(use_rdkafka=True)

    python_kafka_producer_performance()
    python_kafka_consumer_performance()
