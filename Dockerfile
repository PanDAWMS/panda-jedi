FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less git wget logrotate
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install -y postgresql14
RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install -U pip
RUN /opt/panda/bin/pip install -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]"
RUN mkdir /tmp/src
WORKDIR /tmp/src
COPY . .
RUN /opt/panda/bin/python setup.py sdist; /opt/panda/bin/pip install `ls dist/p*.tar.gz`[postgres]
RUN /opt/panda/bin/pip install rucio-clients

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_jedi.cfg.rpmnew /etc/panda/panda_jedi.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/sysconfig/panda_jedi /etc/sysconfig/panda_jedi

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
