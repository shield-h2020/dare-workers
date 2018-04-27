'''
    Methods to be used during the streaming process.
'''

from datetime import datetime
from shlex    import shlex

INDEX   = 0
SEGTYPE = 'proxy-log segments'

def _analyze(line):
    '''
        A lexical analyzer for simple shell-like syntaxes. Split given line into fields.

    :param line: Line to split.
    :returs    : List of fields.
    :rtype     : ``list``
    '''
    lex                  = shlex(line)
    lex.quotes           = '"'
    lex.whitespace_split = True
    lex.commenters       = ''
    return list(lex)

def create_dstream(ssc, zk_quorum, group_id, topics):
    '''
        Create an input stream that pulls proxy-log messages from Kafka.

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

    return dstream.map(lambda x: x[1]).flatMap(lambda x: x).map(lambda x: _analyze(x))

def stream_parser(fields):
    '''
        Parsing and normalization of data in preparation for import.

    :param fields: Column fields of a row.
    :returns     : A list of typecast-ed fields, according to the schema table.
    :rtype       : ``list``
    '''
    if len(fields) <= 1:
        return []

    dt = datetime.strptime('{0} {1}'.format(fields[0], fields[1]), '%Y-%m-%d %H:%M:%S')

    uripath  = fields[17] if len(fields[17]) > 1 else ''
    uriquery = fields[18] if len(fields[18]) > 1 else ''

    return [
        fields[0], fields[1], fields[3], fields[15], fields[12], fields[20], fields[13],
        long(fields[2]),
        fields[4], fields[5], fields[6], fields[7], fields[8], fields[9], fields[10],
            fields[11], fields[14], fields[16], fields[17], fields[18], fields[19], fields[21],
        int(fields[22]), int(fields[23]),
        fields[24], fields[25], fields[26],
        '{0}{1}{2}'.format(fields[15], uripath, uriquery),
        str(dt.year), str(dt.month).zfill(2), str(dt.day).zfill(2), str(dt.hour).zfill(2)
    ]

def struct_type():
    '''
        Return the data type that represents a row from the received data list.
    '''
    from pyspark.sql.types import (StructType, StructField, StringType, IntegerType,
                                    LongType)
    return StructType([
        StructField('p_date', StringType(), True),
        StructField('p_time', StringType(), True),
        StructField('clientip', StringType(), True),
        StructField('host', StringType(), True),
        StructField('reqmethod', StringType(), True),
        StructField('useragent', StringType(), True),
        StructField('resconttype', StringType(), True),
        StructField('duration', LongType(), True),
        StructField('username', StringType(), True),
        StructField('authgroup', StringType(), True),
        StructField('exceptionid', StringType(), True),
        StructField('filterresult', StringType(), True),
        StructField('webcat', StringType(), True),
        StructField('referer', StringType(), True),
        StructField('respcode', StringType(), True),
        StructField('action', StringType(), True),
        StructField('urischeme', StringType(), True),
        StructField('uriport', StringType(), True),
        StructField('uripath', StringType(), True),
        StructField('uriquery', StringType(), True),
        StructField('uriextension', StringType(), True),
        StructField('serverip', StringType(), True),
        StructField('scbytes', IntegerType(), True),
        StructField('csbytes', IntegerType(), True),
        StructField('virusid', StringType(), True),
        StructField('bcappname', StringType(), True),
        StructField('bcappoper', StringType(), True),
        StructField('fulluri', StringType(), True),
        StructField('y', StringType(), True), StructField('m', StringType(), True),
        StructField('d', StringType(), True), StructField('h', StringType(), True)
    ])
