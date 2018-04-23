Streaming Worker
========================================================================================================================================
The role of the Streaming Worker is to listen to a particular topic of the Kafka cluster and consume the incoming messages. Streaming data is divided into batches (according to a time interval). These batches are deserialized by the Worker, according to the supported Avro schema, parsed and registered in the corresponding table of Hive.<br />
Streaming Worker can only run on the central infrastructure and it can be deployed in local, client or cluster mode.

## Prerequisites
Dependencies in Python:
* [avro](https://avro.apache.org/) - Data serialization system.

Dependencies in Linux OS:
* [pip](https://pypi.org/project/pip/) - Python package manager.
* [virtualenv](https://pypi.org/project/virtualenv/) - Tool to create isolated Python environments.
* [zip](http://manpages.ubuntu.com/manpages/precise/man1/zip.1.html) - Package and compress (archive) files.

## Configuration
The first time you execute the `run.sh` script, it copies the `.worker.json` configuration file under your home directory. For this reason, it is recommended to setup your configuration file before you execute the script.

    {
      "consumer": {
        "bootstrap_servers": ["kafka_server:kafka_port"],
        "group_id": ""
      },
      "database": "dbname",
      "kerberos": {
        "kinit": "/usr/bin/kinit",
        "principal": "user",
        "keytab": "/opt/security/user.keytab"
      },
      "zkQuorum": "zk_server:zk_port"
    }

* _"consumer"_: This section contains configuration parameters for the `KafkaConsumer`.
  * _"bootstrap_servers"_: 'host[:port]' string (or list of 'host[:port]' strings) that the consumer should contact to bootstrap initial cluster metadata.
  * _"group_id": The name of the consumer group to join for dynamic partition assignment (if enabled), and to use for fetching and committing offsets.
* _"database"_: Name of Hive database where all the ingested data will be stored.
* _"kerberos"_: [Kerberos](https://web.mit.edu/kerberos/) is a network authentication protocol and in this section you can define its parameters. To enable this feature, you also need to set the environment variable `KRB_AUTH`.
* _"zkQuorum"_: The connection string for the zookeeper connection in the form host:port. Multiple URLs can be given to allow fail-over.

The section _"consumer"_ is required **only** for the simple worker.

**Example of Configuration file**<br />
An example of the configuration file for `flow` is given below:

    {
      "database": "spotdb",
      "kerberos": {
        "kinit": "/usr/bin/kinit",
        "principal": "spotuser",
        "keytab": "/opt/security/spotuser.keytab"
      },
      "zkQuorum": "cloudera01:2181"
    }

## Start Streaming Worker

### Print Usage Message
Before you start using the Streaming Worker, you should print the usage message and check the available options.

    usage: worker [OPTIONS]... -t <pipeline> --topic <topic> -p <partition>

    Streaming Worker reads from Kafka cluster and writes to Hive through the Spark2
    streaming module.

    Optional Arguments:
     -h, --help                            show this help message and exit
     -a STRING, --app-name STRING          name of the Spark Job to display on the
     -b INTEGER, --batch-duration INTEGER  time interval (in seconds) at which
                                           streaming data will be divided into
                                           batches
     -c FILE, --config-file FILE           path of configuration file
     -g STRING, --group-id STRING          name of the consumer group to join for
                                           dynamic partition assignment
     -l STRING, --log-level STRING         determine the level of the logger
     -v, --version                         show program's version number and exit

Required Arguments:
     -p INTEGER, --partition INTEGER       partition number to consume
     --topic STRING                        topic to listen for new messages
     -t STRING, --type STRING              type of the data that will be ingested

END

The only mandatory arguments for the Streaming Worker are the topic, the partition and the type of the pipeline (`flow`, `dns` or `proxy`). Streaming Worker _does not create a new topic_, so you have to pass _an existing one_. By default, it loads configuration parameters from the `~/.d-collector.json` file, but you can override it with `-c FILE, --config-file FILE` option.

### Run `run.sh` Script
To start Streaming Worker:<br />
  `./run.sh -t "pipeline_configuration" --topic "my_topic" -p "num_of_partition"`

Some examples are given below:<br />
1. `./run.sh -t flow --topic SPOT-INGEST-TEST-TOPIC -p 1`<br />
2. `./run.sh -t flow --topic SPOT-INGEST-TEST-TOPIC -p 1 2>/tmp/spark2-submit.log`<br />
3. `./run.sh -t dns --topic SPOT-INGEST-DNS-TEST-TOPIC -p 2 -a AppIngestDNS -b 10 -g DNS-GROUP`

Simple Worker
=======================================================================================================================================
Simple worker is responsible for listening to a partition/topic of the Kafka cluster, consuming incoming messages and storing into HDFS.

### Prerequisites
Dependencies in Python:
* [avro](https://avro.apache.org/) - Data serialization system.
* [kafka-python](https://github.com/dpkp/kafka-python) - Python client for the Apache Kafka distributed stream processing system.

Dependencies in Linux OS:
* [pip](https://pypi.org/project/pip/) - Python package manager.

### Installation
Installation of the Simple Worker requires a user with `sudo` privileges.
1. Install packages from the given requirements file:<br />
  `sudo -H pip install -r requirements.txt`

2. Install package:<br />
  `sudo python setup.py install --record install-files.txt`

To uninstall Simple Worker, just delete installation files:<br />
  `cat install-files.txt | sudo xargs rm -rf`

If you want to avoid granting `sudo` permissions to the user (or keeping isolated the current installation from the rest of the system), use the `virtualenv` package.

1. Install `virtualenv` package as `root` user:<br />
  `sudo apt-get -y install python-virtualenv`

2. Switch to your user and create an isolated virtual environment:<br />
  `virtualenv --no-site-packages venv`

3. Activate the virtual environment and install source distribution:<br />
  `source venv/bin/activate`<br />
  `pip install -r requirements.txt`<br />
  `python setup.py install`

### Configuration
Simple Worker uses the same configuration file as the Streaming Worker. The required sections are the _"consumer"_ and _"kerberos"_ sections.

## Start Simple Worker

### Print Usage Message
Before you start using the Simple Worker, you should print the usage message and check the available options.

    usage: s-worker [OPTIONS]... --topic <topic> -p <partition> -d <HDFS folder>

    Simple Worker listens to a partition/topic of the Kafka cluster and stores incoming
    records to the HDFS.

    Optional Arguments:
      -h, --help                          show this help message and exit
      -c FILE, --config-file FILE         path of configuration file
      -i INTEGER, --interval INTEGER      milliseconds spent waiting in poll if data is not
                                          available in the buffer
      -l STRING, --log-level STRING       determine the level of the logger
      --parallel-processes INTEGER        number of the parallel processes
      -v, --version                       show program's version number and exit

    Required Arguments:
      -d STRING, --hdfs-directory STRING  destination folder in HDFS
      -p INTEGER, --partition INTEGER     partition number to consume
      --topic STRING                      topic to listen for new messages

END

The only mandatory arguments for the Simple Worker are the destination folder in HDFS, the number of the partition and the Kafka topic. Simple Worker _does not create a new topic_, so you have to pass _an existing one_. By default, it loads configuration parameters from the `~/.worker.json` file, but you can override it with `-c FILE, --config-file FILE` option.

### Run `s-worker` Command
To start Simple Worker:<br />
  `s-worker --topic "my_topic" -p "num_of_partition" -d "path_to_HDFS"`

Some examples are given below:<br />
1. `s-worker --topic SPOT-INGEST-TEST-TOPIC -p 0 -d /user/spotuser/flow/stage`
2. `s-worker --topic SPOT-INGEST-DNS-TEST-TOPIC -p 0 -d /user/spotuser/dns/stage --parallel-processes 8 -i 30`
3. `s-worker --topic SPOT-INGEST-TEST-TOPIC -p 2 -d /user/spotuser/flow/stage -l DEBUG`

### Acknowledgement
The work for this contribution has received funding from the European Union's Horizon 2020 research and innovation programme under grant agreement No700199.
