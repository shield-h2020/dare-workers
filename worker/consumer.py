'''
    Retrieve ``list`` objects from Kafka cluster.
'''

import logging

from datetime     import datetime
from kafka        import KafkaConsumer
from kafka.common import TopicPartition
from serializer   import deserialize


class Consumer(KafkaConsumer):
    '''
        Consume records from a Kafka cluster.

        The consumer will transparently handle the failure of servers in the Kafka
    cluster and adapt as topic-partitions are created or migrated between brokers. It
    also interacts with the assigned Kafka Group Coordinator node to allow multiple
    consumers to load balance consumption of topics.

        Configuration parameters are described in more detail at
    https://kafka.apache.org/documentation/#newconsumerconfigs
    '''

    def __init__(self, **kwargs):
        self._logger = logging.getLogger('SHIELD.WORKER.CONSUMER')
        super(Consumer, self).__init__(**kwargs)

    def assign(self, topic, partitions):
        '''
            Manually assign a list of TopicPartitions to this consumer.

        :param topic     : Topic where the messages are published.
        :param partitions: List of partition numbers where the messages are delivered.
        '''
        self._logger.info('Assignments for this instance; Topic: {0}, Partitions: {1}'
            .format(topic, partitions))
        super(Consumer, self).assign([TopicPartition(topic, x) for x in partitions])

    def parse(self, record):
        '''
            Parse consumer's record and return a pair of the deserialized data among
        with the datetime of the first record.

        :param record: A received :class:`kafka.consumer.fetcher.ConsumerRecord` object.
        :returns     : A pair of the datetime and a list of ``str`` objects.
        :rtype       : ``tuple``
        '''
        self._logger.info('Received segment from Kafka cluster. [Topic: {0}, Partition: {1}]'
            .format(record.topic, record.partition))

        self._logger.debug('[Offset: {0}, Partition: {1}, Checksum: {2}]'
            .format(record.offset, record.partition, record.checksum))

        try:
            return (datetime.utcfromtimestamp(float(record.timestamp) / 1000),
                    deserialize(record.value))

        except Exception as exc:
            self._logger.error('[{0}] {1}'.format(exc.__class__.__name__, exc.message))

        return None

    def poll(self, timeout_ms=10000, max_records=None):
        '''
            Fetch data from assigned topics/partitions.

        :param timeout_ms : Milliseconds spent waiting in poll if data is not available
                            in the buffer. If 0, returns immediately with any records
                            that are available currently in the buffer, else returns
                            empty. Must not be negative.
        :param max_records: The maximum number of records returned in a single call.
        :returns          : List of records since the last fetch for the subscribed list
                            of topics and partitions.
        :rtype            : ``list``
        '''
        messages = []
        records  = super(Consumer, self).poll(timeout_ms, max_records)

        [messages.extend(x) for x in records.values()]
        return [self.parse(x) for x in messages]
