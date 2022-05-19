FROM docker.io/centos:7

RUN yum update -y
RUN yum install -y epel-release
RUN yum install -y python3 python3-devel httpd httpd-devel gcc gridsite less git
RUN yum install -y https://download.postgresql.org/pub/repos/yum/reporpms/EL-7-x86_64/pgdg-redhat-repo-latest.noarch.rpm
RUN yum install -y postgresql14
RUN python3 -m venv /opt/panda
RUN /opt/panda/bin/pip install -U pip
RUN /opt/panda/bin/pip install -U setuptools
RUN adduser atlpan
RUN groupadd zp
RUN usermod -a -G zp atlpan
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-server.git#egg=panda-server[postgres]"
RUN /opt/panda/bin/pip install "git+https://github.com/PanDAWMS/panda-jedi.git#egg=panda-jedi[postgres]"
RUN /opt/panda/bin/pip install rucio-clients

RUN mkdir -p /etc/panda
RUN mkdir -p /etc/idds
RUN mv /opt/panda/etc/panda/panda_common.cfg.rpmnew /etc/panda/panda_common.cfg
RUN mv /opt/panda/etc/panda/panda_server.cfg.rpmnew /etc/panda/panda_server.cfg
RUN mv /opt/panda/etc/panda/panda_jedi.cfg.rpmnew /etc/panda/panda_jedi.cfg
RUN mv /opt/panda/etc/idds/idds.cfg.client.template /opt/panda/etc/idds/idds.cfg
RUN mv /opt/panda/etc/panda/panda_server.sysconfig.rpmnew /etc/sysconfig/panda_server
RUN mv /opt/panda/etc/sysconfig/panda_jedi /etc/sysconfig/panda_jedi

RUN ln -s /opt/panda/etc/rc.d/init.d/panda_jedi /etc/rc.d/init.d/panda-jedi

RUN mkdir -p /data/atlpan
RUN mkdir -p /var/log/panda/wsgisocks
RUN mkdir -p /run/httpd/wsgisocks
RUN chown -R atlpan:zp /var/log/panda

# to run with non-root PID
RUN chmod -R 777 /var/log/panda
RUN chmod -R 777 /home/atlpan
RUN chmod -R 777 /var/lock

CMD exec /bin/bash -c "trap : TERM INT; sleep infinity & wait"

