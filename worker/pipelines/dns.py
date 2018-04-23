'''
    Methods to be used during the streaming process.
'''

from datetime import datetime

SEGTYPE = 'pcap segments'

def create_dstream(ssc, zk_quorum, group_id, topics):
    '''
        Create an input stream that pulls pcap messages from Kafka.

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
    dt = datetime.fromtimestamp(float(fields[2]))

    return [
        '{0}, {1}'.format(fields[0], fields[1]),
        long(float(fields[2])),
        int(fields[3]),
        fields[5], fields[4], fields[6], fields[8],
        0 if fields[7] == '' else int(fields[7]),
        0 if fields[8] == '' else int(fields[9]),
        fields[10],
        dt.year, dt.month, dt.day, dt.hour
    ]

def struct_type():
    '''
        Return the data type that represents a row from the received data list.
    '''
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                                    LongType, ShortType)
    return StructType([
        StructField('frame_time', StringType(), True),
        StructField('unix_tstamp', LongType(), True),
        StructField('frame_len', IntegerType(), True),
        StructField('ip_dst', StringType(), True),
        StructField('ip_src', StringType(), True),
        StructField('dns_qry_name', StringType(), True),
        StructField('dns_qry_class', StringType(), True),
        StructField('dns_qry_type', IntegerType(), True),
        StructField('dns_qry_rcode', IntegerType(), True),
        StructField('dns_a', StringType(), True),
        StructField('y', ShortType(), True), StructField('m', ShortType(), True),
        StructField('d', ShortType(), True), StructField('h', ShortType(), True)
    ])
