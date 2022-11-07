"""
checking DB schema version for PanDA JEDI
"""

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.JediDBProxy import DBProxy

from pandajedi.jedicore import JediDBSchemaInfo

from packaging import version

proxyS = DBProxy()
proxyS.connect(jedi_config.db.dbhost, jedi_config.db.dbpasswd, jedi_config.db.dbuser, jedi_config.db.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'JEDI'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0]

serverDBVersion = JediDBSchemaInfo.JediDBSchemaInfo().method()

if version.parse(dbVersion) >= version.parse(serverDBVersion):
    return ("True")
else:
    return ("False")
