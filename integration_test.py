#!/usr/bin/env python
#
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


""" Test script for confluent_kafka module """

import confluent_kafka
import re
import time
import uuid
import sys

try:
    from progress.bar import Bar
    with_progress = True
except ImportError as e:
    with_progress = False

# Kafka bootstrap server(s)
bootstrap_servers = 'localhost'

    
class MyTestDr(object):
    """ Producer: Delivery report callback """

    def __init__(self, silent=False):
        super(MyTestDr, self).__init__()
        self.msgs_delivered = 0
        self.bytes_delivered = 0
        self.silent = silent

    @staticmethod
    def _delivery(err, msg, silent=False):
        if err:
            print('Message delivery failed (%s [%s]): %s' % \
                  (msg.topic(), str(msg.partition()), err))
            return 0
        else:
            if not silent:
                print('Message delivered to %s [%s] at offset [%s]: %s' % \
                      (msg.topic(), msg.partition(), msg.offset(), msg.value()))
            return 1

    def delivery(self, err, msg):
        if err:
            print('Message delivery failed (%s [%s]): %s' % \
                  (msg.topic(), str(msg.partition()), err))
            return
        elif not self.silent:
            print('Message delivered to %s [%s] at offset [%s]: %s' % \
                  (msg.topic(), msg.partition(), msg.offset(), msg.value()))
        self.msgs_delivered += 1
        self.bytes_delivered += len(msg)


def confluent_verify_producer_performance(with_dr_cb=True):
    """ Time how long it takes to produce and delivery X messages """
    conf = {'bootstrap.servers': bootstrap_servers}

    p = confluent_kafka.Producer(**conf)

    topic = 'test'
    msgcnt = 1000000
    msgsize = 100
    msg_pattern = 'test.py performance'
    msg_payload = (msg_pattern * int(msgsize / len(msg_pattern)))[0:msgsize]

    dr = MyTestDr(silent=True)

    t_produce_start = time.time()
    msgs_produced = 0
    msgs_backpressure = 0
    print('# producing %d messages to topic %s' % (msgcnt, topic))

    if with_progress:
        bar = Bar('Producing', max=msgcnt)
    else:
        bar = None

    for i in range(0, msgcnt):
        try:
            if with_dr_cb:
                p.produce('test', value=msg_payload, callback=dr.delivery)
            else:
                p.produce('test', value=msg_payload)
        except BufferError as e:
            # Local queue is full (slow broker connection?)
            msgs_backpressure += 1
            if bar is not None and (msgs_backpressure % 1000) == 0:
                bar.next(n=0)
            p.poll(0)
            continue

        if bar is not None and (msgs_produced % 5000) == 0:
            bar.next(n=5000)
        msgs_produced += 1
        p.poll(0)

    t_produce_spent = time.time() - t_produce_start

    bytecnt = msgs_produced * msgsize

    if bar is not None:
        bar.finish()

    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))
    print('# %d messages not produce()d due to backpressure (local queue full)' % msgs_backpressure)

    print('waiting for %d/%d deliveries' % (len(p), msgs_produced))
    # Wait for deliveries
    p.flush()
    t_delivery_spent = time.time() - t_produce_start


    print('# producing %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (msgs_produced, bytecnt / (1024*1024), t_produce_spent,
           msgs_produced / t_produce_spent,
           (bytecnt/t_produce_spent) / (1024*1024)))

    # Fake numbers if not using a dr_cb
    if not with_dr_cb:
        print('# not using dr_cb')
        dr.msgs_delivered = msgs_produced
        dr.bytes_delivered = bytecnt

    print('# delivering %d messages (%.2fMb) took %.3fs: %d msgs/s, %.2f Mb/s' % \
          (dr.msgs_delivered, dr.bytes_delivered / (1024*1024), t_delivery_spent,
           dr.msgs_delivered / t_delivery_spent,
           (dr.bytes_delivered/t_delivery_spent) / (1024*1024)))
    print('# post-produce delivery wait took %.3fs' % \
          (t_delivery_spent - t_produce_spent))


def print_commit_result (err, partitions):
    if err is not None:
        print('# Failed to commit offsets: %s: %s' % (err, partitions))
    else:
        print('# Committed offsets for: %s' % partitions)



def confluent_verify_consumer_performance():
    """ Verify Consumer performance """

    conf = {'bootstrap.servers': bootstrap_servers,
            'group.id': uuid.uuid1(),
            'session.timeout.ms': 6000,
            'default.topic.config': {
                'auto.offset.reset': 'earliest'
            }}

    c = confluent_kafka.Consumer(**conf)

    def my_on_assign (consumer, partitions):
        print('on_assign:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.assign(partitions)

    def my_on_revoke (consumer, partitions):
        print('on_revoke:', len(partitions), 'partitions:')
        for p in partitions:
            print(' %s [%d] @ %d' % (p.topic, p.partition, p.offset))
        consumer.unassign()

    c.subscribe(["test"])

    max_msgcnt = 1000000
    bytecnt = 0
    msgcnt = 0

    print('Will now consume %d messages' % max_msgcnt)
    while True:
        # Consume until EOF or error

        msg = c.poll(timeout=20.0)
        if msg is None:
            raise Exception('Stalled at %d/%d message, no new messages for 20s' %
                            (msgcnt, max_msgcnt))

        if msg.error():
            if msg.error().code() == confluent_kafka.KafkaError._PARTITION_EOF:
                # Reached EOF for a partition, ignore.
                continue
            else:
                raise confluent_kafka.KafkaException(msg.error())


        bytecnt += len(msg)
        msgcnt += 1

        if msgcnt == 1:
            t_first_msg = time.time()
        if msgcnt >= max_msgcnt:
            break

    if msgcnt > 0:
        t_spent = time.time() - t_first_msg
        print('%d messages (%.2fMb) consumed in %.3fs: %d msgs/s, %.2f Mb/s' % \
              (msgcnt, bytecnt / (1024*1024), t_spent, msgcnt / t_spent,
               (bytecnt / t_spent) / (1024*1024)))

    print('closing consumer')
    c.close()



if __name__ == '__main__':

    if len(sys.argv) > 1:
        bootstrap_servers = sys.argv[1]

    print('Using confluent_kafka module version %s (0x%x)' % confluent_kafka.version())
    print('Using librdkafka version %s (0x%x)' % confluent_kafka.libversion())

    print('=' * 30, 'Verifying Producer performance (without dr_cb)', '=' * 30)
    confluent_verify_producer_performance(with_dr_cb=False)

    print('=' * 30, 'Verifying Consumer performance', '=' * 30)
    confluent_verify_consumer_performance()

    print('=' * 30, 'Done', '=' * 30)


