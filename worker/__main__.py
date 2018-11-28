#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Listen to a particular topic of the Kafka cluster and write incoming records to a
Hive table, via Spark2 Streaming module.
'''

import json
import logging
import os
import pipelines
import sys

from argparse          import ArgumentParser, HelpFormatter
from pyspark           import SparkContext
from pyspark.sql       import HiveContext
from pyspark.streaming import StreamingContext
from utils             import authenticate, get_logger
from version           import __version__

def parse_args():
    '''
        Parse command-line options found in 'args' (default: sys.argv[1:]).

    :returns: On success, a namedtuple of Values instances.
    '''
    parser   = ArgumentParser(
        prog='worker',
        description='Streaming Worker reads from Kafka cluster and writes to Hive '
            'through the Spark2 streaming module.',
        epilog='END',
        formatter_class=lambda prog: HelpFormatter(prog, max_help_position=40, width=80),
        usage='worker [OPTIONS]... -t <pipeline> --topic <topic> -p <partition>')

    # .................................set optional arguments
    parser._optionals.title = 'Optional Arguments'

    parser.add_argument('-a', '--app-name', metavar='STRING',
        help='name of the Spark Job to display on the cluster web UI')

    parser.add_argument('-b', '--batch-duration', default=30, type=int, metavar='INTEGER',
        help='time interval (in seconds) at which streaming data will be divided into batches')

    parser.add_argument('-c', '--config-file', metavar='FILE', type=file,
        help='path of configuration file', default=os.path.expanduser('~/.worker.json'))

    parser.add_argument('-g', '--group-id', metavar='STRING',
        help='name of the consumer group to join for dynamic partition assignment')

    parser.add_argument('-l', '--log-level', default='INFO', metavar='STRING',
        help='determine the level of the logger')

    parser.add_argument('-v', '--version', action='version', version='%(prog)s {0}'
        .format(__version__))

    # .................................set required arguments
    required = parser.add_argument_group('Required Arguments')

    required.add_argument('-p', '--partition', required=True, metavar='INTEGER',
        help='partition number to consume')

    required.add_argument('--topic', required=True, metavar='STRING',
        help='topic to listen for new messages')

    required.add_argument('-t', '--type', choices=pipelines.__all__, required=True,
        help='type of the data that will be ingested', metavar='STRING')

    return parser.parse_args()

def store(rdd, hsc, table, topic, schema=None, segtype='segments', index=0):
    '''
        Interface for saving the content of the streaming :class:`DataFrame` out into
    Hive storage.

    :param rdd    : The content as a :class:`pyspark.RDD` of :class:`Row`.
    :param hsc    : A variant of Spark SQL that integrates with data stored in Hive.
    :param table  : The specified table in Hive database.
    :param topic  : Name of the topic to listen for incoming segments.
    :param schema : The schema of this :class:`DataFrame` as a
                    :class:`pyspark.sql.types.StructType`.
    :param segtype: The type of the received segments.
    :param index  : Index of array that identify the segment.
    '''
    logger = logging.getLogger('SHIELD.WORKER.STREAMING')

    if rdd.isEmpty():
        logger.info(' ---- LISTENING KAFKA TOPIC: {0} ---- '.format(topic))
        return

    hsc.setConf('hive.exec.dynamic.partition', 'true')
    hsc.setConf('hive.exec.dynamic.partition.mode', 'nonstrict')

    logger.info('Received {0} from topic. [Rows: {1}]'.format(segtype, rdd.count()))
    logger.info('Create distributed collection for partition "{0}".'
        .format(rdd.first()[index]))

    df = hsc.createDataFrame(rdd, schema)
    df.write.format('parquet').mode('append').insertInto(table)
    logger.info(' **** REGISTRATION COMPLETED **** ')


class StreamingWorker:
    '''
        StreamingWorker is responsible for listening to a particular topic of the Kafka
    cluster, consuming incoming messages and writing data to the Hive database.

    :param app_name      : A name for your job, to display on the cluster web UI.
    :param batch_duration: The time interval (in seconds) at which streaming data will be
                           divided into batches.
    '''

    def __init__(self, app_name, batch_duration):
        self._logger       = logging.getLogger('SHIELD.WORKER.STREAMING')
        self._logger.info('Initializing Streaming Worker...')

        # .............................connect to Spark cluster
        self._context      = SparkContext(appName=app_name)
        self._logger.info('Connect to Spark Cluster as job "{0}" and broadcast variables'
            ' on it.'.format(app_name))

        # .............................create a new streaming context
        self._streaming    = StreamingContext(self._context, batchDuration=batch_duration)
        self._logger.info('Streaming data will be divided into batches of {0} seconds.'
            .format(batch_duration))

        # .............................read from ``hive-site.xml``
        self._hive_context = HiveContext(self._context)
        self._logger.info('Read Hive\'s configuration to integrate with data stored in it.')

    def start(self, datatype, db_name, zk_quorum, group_id, topic, partition):
        '''
            Start the ingestion.

        :param datatype : Type of the data that will be ingested.
        :param db_name  : Name of the database in Hive, where the data will be stored.
        :param zk_quorum: Zookeeper quorum (host[:port],...).
        :param group_id : The group id for this consumer.
        :param topic    : Topic to listen for new messages.
        :param partition: Partition number to consume.
        '''
        import worker.pipelines

        module  = getattr(worker.pipelines, datatype)
        table   = '{0}.{1}'.format(db_name, datatype)
        dstream = module.create_dstream(self._streaming, zk_quorum, group_id,
                    { topic: partition })
        schema  = module.struct_type()

        dstream.map(lambda x: module.stream_parser(x))\
            .filter(lambda x: bool(x))\
            .foreachRDD(lambda x:
                store(x, self._hive_context, table, topic, schema,
                    module.SEGTYPE, module.INDEX))

        self._streaming.start()
        self._logger.info('Start the execution of the streams.')
        self._streaming.awaitTermination()

    @classmethod
    def run(cls, **kwargs):
        '''
            Main command-line entry point.

        :param cls: The class as implicit first argument.
        '''
        try:
            conf  = json.loads(kwargs.pop('config_file').read())
            topic = kwargs.pop('topic')

            # .........................set up logger
            get_logger('SHIELD.WORKER', kwargs.pop('log_level'))

            # .........................check kerberos authentication
            if os.getenv('KRB_AUTH'):
                authenticate(conf['kerberos'])

            # .........................instantiate StreamingWorker
            worker = cls(kwargs.pop('app_name') or topic, kwargs.pop('batch_duration'))

            # .........................start StreamingWorker
            worker.start(
                kwargs.pop('type'),
                conf['database'],
                conf['zkQuorum'],
                kwargs.pop('group_id') or topic,
                topic,
                int(kwargs.pop('partition')))

        except SystemExit: raise
        except:
            sys.excepthook(*sys.exc_info())
            sys.exit(1)

if __name__ == '__main__': StreamingWorker.run(**parse_args().__dict__)
