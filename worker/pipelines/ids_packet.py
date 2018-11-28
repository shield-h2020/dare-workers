'''
    Methods to be used during the streaming process.
'''

from datetime import datetime

INDEX   = 6
SEGTYPE = 'ids packet segments'

def create_dstream(ssc, zk_quorum, group_id, topics):
    '''
        Create an input stream that pulls ids packet messages from Kafka.

    :param ssc      : :class:`pyspark.streaming.context.StreamingContext` object.
    :param zk_quorum: Zookeeper quorum (host[:port],...).
    :param group_id : The group id for this consumer.
    :param topics   : Dictionary of topic -> numOfPartitions to consume. Each
                      partition is consumed in its own thread.
    :returns        : The schema of this :class:`DataFrame`.
    :rtype          : :class:`pyspark.sql.types.StructType`
    '''
    from pyspark.streaming.kafka import KafkaUtils
    from ..serializer            import deserialize

    dstream = KafkaUtils.createStream(ssc, zk_quorum, group_id, topics,
                keyDecoder=lambda x: x, valueDecoder=deserialize)

    return dstream.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: x.split(','))

def stream_parser(fields):
    '''
        Parsing and normalization of data in preparation for import.

    :param fields: Column fields of a row.
    :returns     : A list of typecast-ed fields, according to the schema table.
    :rtype       : ``list``
    '''
    dt = datetime.utcfromtimestamp(float(fields[7]))
    return [
        fields[1],
        int(fields[2]), int(fields[3]), int(fields[4]), int(fields[5]), int(fields[6]),
        float(fields[7]),
        int(dt.year), int(dt.month), int(dt.day), int(dt.hour)
    ]

def struct_type():
    '''
        Return the data type that represents a row from the received data list.
    '''
    from pyspark.sql.types import (StructType, StructField, StringType, ShortType,
                                    IntegerType, FloatType)

    return StructType([
        StructField('data', StringType(), True),
        StructField('event_id', IntegerType(), True),
        StructField('event_second', IntegerType(), True),
        StructField('length', IntegerType(), True),
        StructField('linktype', ShortType(), True),
        StructField('sensor_id', IntegerType(), True),
        StructField('unix_tstamp', FloatType(), True),
        StructField('y', ShortType(), True), StructField('m', ShortType(), True),
        StructField('d', ShortType(), True), StructField('h', ShortType(), True)
    ])
