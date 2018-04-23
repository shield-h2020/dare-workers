'''
    Methods to be used during the streaming process.
'''

from datetime import datetime

SEGTYPE = 'netflow segments'

def create_dstream(ssc, zk_quorum, group_id, topics):
    '''
        Create an input stream that pulls netflow messages from Kafka.

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
    unix_tstamp = datetime.strptime(fields[0], '%Y-%m-%d %H:%M:%S').strftime('%s')

    return [
        fields[0],
        long(unix_tstamp),
        int(fields[1]), int(fields[2]), int(fields[3]), int(fields[4]), int(fields[5]),
            int(fields[6]), float(fields[7]),
        fields[8], fields[9],
        int(fields[10]), int(fields[11]),
        fields[12], fields[13],
        int(fields[14]), int(fields[15]),
        long(fields[16]), long(fields[17]), long(fields[18]), long(fields[19]),
        int(fields[20]), int(fields[21]), int(fields[22]), int(fields[23]),
            int(fields[24]), int(fields[25]),
        fields[26],
        int(fields[1]), int(fields[2]), int(fields[3]), int(fields[4]),
    ]

def struct_type():
    '''
        Return the data type that represents a row from the received data list.
    '''
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                                    LongType, FloatType, ShortType)
    return StructType([
        StructField('treceived', StringType(), True),
        StructField('unix_tstamp', LongType(), True),
        StructField('tryear', IntegerType(), True),
        StructField('trmonth', IntegerType(), True),
        StructField('trday', IntegerType(), True),
        StructField('trhour', IntegerType(), True),
        StructField('trminute', IntegerType(), True),
        StructField('trsecond', IntegerType(), True),
        StructField('tdur', FloatType(), True),
        StructField('sip', StringType(), True),
        StructField('dip', StringType(), True),
        StructField('sport', IntegerType(), True),
        StructField('dport', IntegerType(), True),
        StructField('proto', StringType(), True),
        StructField('flag', StringType(), True),
        StructField('fwd', IntegerType(), True),
        StructField('stos', IntegerType(), True),
        StructField('ipkt', LongType(), True),
        StructField('ibyt', LongType(), True),
        StructField('opkt', LongType(), True),
        StructField('obyt', LongType(), True),
        StructField('input', IntegerType(), True),
        StructField('output', IntegerType(), True),
        StructField('sas', IntegerType(), True),
        StructField('das', IntegerType(), True),
        StructField('dtos', IntegerType(), True),
        StructField('dir', IntegerType(), True),
        StructField('rip', StringType(), True),
        StructField('y', ShortType(), True), StructField('m', ShortType(), True),
        StructField('d', ShortType(), True), StructField('h', ShortType(), True)
    ])
