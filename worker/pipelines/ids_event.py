'''
    Methods to be used during the streaming process.
'''

from datetime import datetime

INDEX   = 17
SEGTYPE = 'ids event segments'

def create_dstream(ssc, zk_quorum, group_id, topics):
    '''
        Create an input stream that pulls ids event messages from Kafka.

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
    dt = datetime.utcfromtimestamp(float(fields[18]))

    return [
        int(fields[1]),
        fields[2],
        int(fields[3]),
        fields[4],
        int(fields[5]), int(fields[6]), int(fields[7]), int(fields[8]), int(fields[9]),
            int(fields[10]), int(fields[11]), int(fields[12]),
        long(fields[13]),
        int(fields[14]),
        long(fields[15]),
        fields[16],
        0 if fields[17] == 'None' else int(fields[17]),
        float(fields[18]),
        int(dt.year), int(dt.month), int(dt.day), int(dt.hour)
    ]

def struct_type():
    '''
        Return the data type that represents a row from the received data list.
    '''
    from pyspark.sql.types import (StructType, StructField, StringType, ShortType,
                                    IntegerType, LongType, FloatType)

    return StructType([
        StructField('blocked', ShortType(), True),
        StructField('classification', StringType(), True),
        StructField('classification_id', IntegerType(), True),
        StructField('destination_ip', StringType(), True),
        StructField('dport_icode', IntegerType(), True),
        StructField('event_id', IntegerType(), True),
        StructField('generator_id', IntegerType(), True),
        StructField('impact', IntegerType(), True),
        StructField('impact_flag', ShortType(), True),
        StructField('priority', IntegerType(), True),
        StructField('protocol', IntegerType(), True),
        StructField('sensor_id', IntegerType(), True),
        StructField('signature_id', LongType(), True),
        StructField('signature_revision', IntegerType(), True),
        StructField('sport_itype', LongType(), True),
        StructField('source_ip', StringType(), True),
        StructField('vlan_id', IntegerType(), True),
        StructField('unix_tstamp', FloatType(), True),
        StructField('y', ShortType(), True), StructField('m', ShortType(), True),
        StructField('d', ShortType(), True), StructField('h', ShortType(), True)
    ])
