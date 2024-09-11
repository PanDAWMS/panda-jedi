ARG PYTHON_VERSION=3.11.6

FROM docker.io/almalinux:9

ARG PYTHON_VERSION

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y gcc make httpd-devel less git wget logrotate procps which psmisc \
    bzip2-devel libffi-devel zlib-devel openssl-devel readline-devel

# install python
RUN mkdir /tmp/python && cd /tmp/python && \
    wget https://www.python.org/ftp/python/${PYTHON_VERSION}/Python-${PYTHON_VERSION}.tgz && \
    tar -xzf Python-*.tgz && rm -f Python-*.tgz && \
    cd Python-* && \
    ./configure --enable-shared --enable-optimizations --with-lto && \
    make altinstall && \
    echo /usr/local/lib > /etc/ld.so.conf.d/local.conf && ldconfig && \
    cd / && rm -rf /tmp/pyton

# install postgres
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-9-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install --nogpgcheck -y postgresql16
RUN  yum clean all && rm -rf /var/cache/yum


# install Oracle Instant Client and tnsnames.ora
RUN wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-basic-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-basic-linuxx64.rpm -y && \
    wget https://download.oracle.com/otn_software/linux/instantclient/oracle-instantclient-sqlplus-linuxx64.rpm -P /tmp/ && \
    yum install /tmp/oracle-instantclient-sqlplus-linuxx64.rpm -y

# Grab the latest version of the Oracle tnsnames.ora file
RUN ln -fs /data/panda/tnsnames.ora /etc/tnsnames.ora

# setup venv with pythonX.Y
RUN python$(echo ${PYTHON_VERSION} | sed -E 's/\.[0-9]+$//') -m venv /opt/panda
RUN /opt/panda/bin/pip install --no-cache-dir -U pip
RUN /opt/panda/bin/pip install --no-cache-dir -U setuptools
RUN /opt/panda/bin/pip install --no-cache-dir -U gnureadline
RUN /opt/panda/bin/pip install --no-cache-dir -U oracledb
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]"
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .
RUN /opt/panda/bin/pip install --no-cache-dir .[postgres]
RUN /opt/panda/bin/pip install --no-cache-dir rucio-clients

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_jedi.cfg.rpmnew /etc/panda/panda_jedi.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/sysconfig/panda_jedi /etc/sysconfig/panda_jedi

RUN mkdir -p /etc/rc.d/init.d
RUN ln -s /opt/panda/etc/rc.d/init.d/panda_jedi /etc/rc.d/init.d/panda-jedi
RUN ln -s /etc/sysconfig/panda_server /opt/panda/etc/panda/panda_server.sysconfig

RUN mkdir -p /data/atlpan
RUN mkdir -p /data/panda
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /run/httpd/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

RUN ln -fs /data/panda/idds.cfg /opt/panda/etc/idds/idds.cfg
RUN ln -fs /data/panda/rucio.cfg /opt/panda/etc/rucio.cfg
RUN ln -fs /data/panda/jedi_mq_config.json /opt/panda/etc/panda/jedi_mq_config.json
RUN ln -fs /data/panda/jedi_msg_proc_config.json /opt/panda/etc/panda/jedi_msg_proc_config.json
RUN ln -fs /data/panda/panda_mbproxy_config.json /opt/panda/etc/panda/panda_mbproxy_config.json

# to run with non-root PID
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /var/lock
RUN chmod -R 777 /data/panda
RUN mkdir -p /etc/grid-security/certificates
RUN chmod -R 777 /etc/grid-security/certificates
RUN chmod -R 777 /var/lib/logrotate

ENV PANDA_LOCK_DIR /var/run/panda
RUN mkdir -p ${PANDA_LOCK_DIR} && chmod 777 ${PANDA_LOCK_DIR}

# make a wrapper script to launch services and periodic jobs in non-root container
RUN echo $'#!/bin/bash \n\
set -m \n\
/data/panda/init-jedi \n\
/data/panda/run-jedi-crons & \n\
/etc/rc.d/init.d/panda-jedi start \n ' > /etc/rc.d/init.d/run-jedi-services

RUN chmod +x /etc/rc.d/init.d/run-jedi-services

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"
