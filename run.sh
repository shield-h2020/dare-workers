#!/bin/bash
# -*- coding: utf-8 -*-

CURRENT_DIR=`pwd`
PYFILES_DIR='pyfiles'
SPARKCONFIG='spark2-submit.conf'
SPARKSUBMIT='spark2-submit'

create_pyfiles_dir() {
    mkdir ${PYFILES_DIR}

    virtualenv --no-site-packages /tmp/venv
    source /tmp/venv/bin/activate

    pip install -r requirements.txt
    cd /tmp/venv/lib/python*/site-packages
    zip -r ${CURRENT_DIR}/${PYFILES_DIR}/avro.zip avro

    cd ${CURRENT_DIR}
    zip -r ${PYFILES_DIR}/worker.zip worker

    deactivate
    rm -r /tmp/venv
}

# .....................................check Spark version
${SPARKSUBMIT} --version > /dev/null 2>&1
if [ ! $? -eq 0 ]; then
    echo "Environment Error: command \"${SPARKSUBMIT}\" does not exist"
    echo "Abort"
    echo
fi

# .....................................load spark configuration file
if [ ! -f "${SPARKCONFIG}" ]; then
    echo "Environment Error: configuration file \"${SPARKCONFIG}\" is missing"
    echo "Abort"
    echo
fi

# .....................................check configuration file
if [ ! -f ~/.worker.json ]; then
    cp .worker.json ~/
fi

. /etc/spot.conf
. ${SPARKCONFIG}

# .....................................package dependencies to be loaded by Spark job
if [ ! -d "${PYFILES_DIR}" ]; then
    create_pyfiles_dir
fi

# .....................................build command
COMMAND="${SPARKSUBMIT}"

[ ! -z "${SPK_MASTER}" ]      && COMMAND="${COMMAND} --master ${SPK_MASTER}"
[ ! -z "${SPK_DEPLOY_MODE}" ] && COMMAND="${COMMAND} --deploy-mode ${SPK_DEPLOY_MODE}"
[ ! -z "${SPK_EXEC}" ]        && COMMAND="${COMMAND} --num-executors ${SPK_EXEC}"
[ ! -z "${SPK_DRIVER_MEM}" ]  && COMMAND="${COMMAND} --driver-memory ${SPK_DRIVER_MEM}"
[ ! -z "${SPK_EXEC_CORES}" ]  && COMMAND="${COMMAND} --conf spark.executor.cores=${SPK_EXEC_CORES}"
[ ! -z "${SPK_EXEC_MEM}" ]    && COMMAND="${COMMAND} --conf spark.executor.memory=${SPK_EXEC_MEM}"

VAR=`find ${PYFILES_DIR} -type f | tr "\n" ","`
COMMAND="${COMMAND} --py-files ${VAR::-1} worker/__main__.py"

# .....................................execute command
${COMMAND} $@
