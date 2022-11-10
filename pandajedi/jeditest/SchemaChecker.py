"""
Checking DB schema version for PanDA JEDI.
If there is an issue and the pandaEmailNotification var is
defined in panda_config, it will send an email notification.
"""

from pandajedi.jediconfig import jedi_config
from pandaserver.config import panda_config
from pandajedi.jedicore.JediDBProxy import DBProxy
from pandaserver.srvcore.MailUtils import MailUtils
from pandajedi.jedicore import JediDBSchemaInfo

from packaging import version

proxyS = DBProxy()
proxyS.connect(jedi_config.db.dbhost, jedi_config.db.dbpasswd, jedi_config.db.dbuser, jedi_config.db.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'JEDI'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0]

serverDBVersion = JediDBSchemaInfo.JediDBSchemaInfo().method()

if version.parse(dbVersion) >= version.parse(serverDBVersion):
    print ("True")
else:
    print ("False")
    if 'pandaEmailNotification' in panda_config.__dict__:
        msgBody = 'There is an issue with ' + panda_config.pserveralias + '. PanDA DB schema installed is ' + dbVersion + \
                  ' while PanDA JEDI requires version ' + serverDBVersion + \
                  ' to be installed. Please check the official docs for instructions on how to upgrade the schema.'
        MailUtils().send(panda_config.pandaEmailNotification,
                         'PanDA DB Version installed is not correct for JEDI running on ' + panda_config.pserveralias, msgBody)
