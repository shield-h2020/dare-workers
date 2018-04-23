#!/usr/bin/env python

import os

from setuptools     import setup, find_packages
from worker.version import __version__

with open('README.md') as fp:
    README = fp.read()

setup(
    name='s-worker',
    version=__version__,
    author='Space Hellas S.A.',
    author_email='ggar@space.gr',
    url='https://www.space.gr/',
    license='Apache License 2.0',
    description='Simple Worker listens to a partition/topic of the Kafka cluster and '
        'stores incoming records to the HDFS.',
    long_description=README,
    keywords="spot shield worker s-worker",
    packages=find_packages(exclude=['worker/pipelines']),
    install_requires=open('requirements.txt').read().strip().split('\n'),
    entry_points={ 'console_scripts': ['s-worker = worker.simple:SimpleWorker.run'] },
    data_files=[(os.path.expanduser('~'), ['.worker.json'])]
)
