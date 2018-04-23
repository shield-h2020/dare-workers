#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
    Main command-line entry point for SimpleWorker.
'''

import json
import logging
import os
import shutil
import signal
import sys
import tempfile
import time

from argparse        import ArgumentParser, HelpFormatter
from consumer        import Consumer
from multiprocessing import current_process, cpu_count, Pool
from utils           import authenticate, get_logger, popen
from version         import __version__

def init_child():
    '''
        Initialize new process from multiprocessing module's Pool.
    '''
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def parse_args():
    '''
        Parse command-line options found in 'args' (default: sys.argv[1:]).

    :returns: On success, a namedtuple of Values instances.
    '''
    parser   = ArgumentParser(
        prog='s-worker',
        description='Simple Worker listens to a partition/topic of the Kafka cluster and '
            'stores incoming records to the HDFS.',
        epilog='END',
        formatter_class=lambda prog: HelpFormatter(prog, max_help_position=42, width=90),
        usage='s-worker [OPTIONS]... --topic <topic> -p <partition> -d <HDFS folder>')

    # .................................set optional arguments
    parser._optionals.title = 'Optional Arguments'

    parser.add_argument('-c', '--config-file', metavar='FILE', type=file,
        help='path of configuration file', default=os.path.expanduser('~/.worker.json'))

    parser.add_argument('-i', '--interval', default=10000,  metavar='INTEGER', type=int,
        help='milliseconds spent waiting in poll if data is not available in the buffer')

    parser.add_argument('-l', '--log-level', default='INFO', metavar='STRING',
        help='determine the level of the logger')

    parser.add_argument('--parallel-processes', default=cpu_count(), type=int,
        help='number of the parallel processes', metavar='INTEGER')

    parser.add_argument('-v', '--version', action='version', version='%(prog)s {0}'
        .format(__version__))

    # .................................set required arguments
    required = parser.add_argument_group('Required Arguments')

    required.add_argument('-d', '--hdfs-directory', required=True, metavar='STRING',
        help='destination folder in HDFS')

    required.add_argument('-p', '--partition', required=True, metavar='INTEGER',
        help='partition number to consume')

    required.add_argument('--topic', required=True, metavar='STRING',
        help='topic to listen for new messages')

    return parser.parse_args()

def store(hpath, segment, tmpdir):
    '''
        Store retrieved record to HDFS.

    :param hpath  : Destination folder in HDFS.
    :param segment: Incoming segment from Kafka cluster.
    :param tmpdir : Path to temporary directory on local file system.
    '''
    logger = logging.getLogger('SHIELD.WORKER.{0}.STORE'.format(current_process().name))

    try:
        timestamp = segment[0].strftime('%Y%m%d%H%M')
        logger.info('Store segment "{0}" to HDFS...'.format(timestamp))

        localfile = os.path.join(tmpdir, timestamp)
        with open(localfile, 'w') as fp:
            [fp.write(x + '\n') for x in segment[1]]

        logger.info('Store segment locally: {0}'.format(localfile))

        # .............................put local file to HDFS
        popen('hdfs dfs -put {0} {1}'.format(localfile, hpath), raises=True)
        logger.info('File stored at "{0}/{1}".'.format(hpath, timestamp))

        os.remove(localfile)
    except Exception as exc:
        logger.error('[{0}] {1}'.format(exc.__class__.__name__, exc.message))


class SimpleWorker:
    '''
        SimpleWorker is responsible for listening to a particular partition/topic of the
    Kafka cluster, consuming incoming messages and storing data to the HDFS.

    :param interval : Milliseconds spent waiting in poll if data is not available in the
                      buffer.
    :param processes: Number of the parallel processes.
    :param topic    : Topic to listen for new messages.
    :param partition: Partition number to consume.
    '''

    def __init__(self, interval, processes, topic, partition, **consumer):
        self._logger          = logging.getLogger('SHIELD.SIMPLE.WORKER')
        self._logger.info('Initializing Simple Worker  process...')

        self._interval        = interval
        self._isalive         = True
        self._processes       = processes

        # .............................init Kafka Consumer
        self.Consumer         = Consumer(**consumer)
        self.Consumer.assign(topic, [int(partition)])

        # .............................set up local staging area
        self._tmpdir          = tempfile.mkdtemp(prefix='_SW.', dir=tempfile.gettempdir())
        self._logger.info('Use directory "{0}" as local staging area.'.format(self._tmpdir))

        # .............................define a process pool object
        self._pool            = Pool(self._processes, init_child)
        self._logger.info('Master Collector will use {0} parallel processes.'
            .format(self._processes))

        signal.signal(signal.SIGUSR1, self.kill)
        self._logger.info('Initialization completed successfully!')

    def __del__(self):
        '''
            Called when the instance is about to be destroyed.
        '''
        if hasattr(self, '_tmpdir'):
            self._logger.info('Clean up temporary directory "{0}".'.format(self._tmpdir))
            shutil.rmtree(self._tmpdir)

    def kill(self):
        '''
            Receive signal for termination from an external process.
        '''
        self._logger.info('Receiving a kill signal from an external process...')
        self._isalive = False

    @classmethod
    def run(cls):
        '''
            Main command-line entry point.

        :param cls: The class as implicit first argument.
        '''
        try:
            args = parse_args()
            conf = json.loads(args.config_file.read())

            # .........................set up logger
            get_logger('SHIELD', args.log_level)

            # .........................check kerberos authentication
            if os.getenv('KRB_AUTH'):
                authenticate(conf['kerberos'])

            # .........................instantiate Simple Worker
            worker = cls(args.interval, args.parallel_processes, args.topic,
                        args.partition, **conf['consumer'])

            worker.start(args.hdfs_directory)
        except SystemExit: raise
        except:
            sys.excepthook(*sys.exc_info())
            sys.exit(1)

    def start(self, hpath):
        '''
            Start Simple Worker.

        :param hpath: Destination folder in HDFS.
        '''
        self._logger.info('Start Simple Worker process!')
        self._logger.info('Messages will be stored under "{0}".'.format(hpath))

        try:
            while self._isalive:
                for record in self.Consumer.poll(self._interval):
                    if not record: continue
                    self._pool.apply_async(store, args=(hpath, record, self._tmpdir))

        except KeyboardInterrupt: pass
        finally:
            self._pool.close()
            self._pool.join()

            self.Consumer.close()
            self._logger.info('Stop Simple Worker process.')

if __name__ == '__main__': SimpleWorker.run()
