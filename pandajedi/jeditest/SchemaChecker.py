"""
checking DB schema version for PanDA JEDI
"""

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.JediDBProxy import DBProxy

from pandajedi.jedicore import JediDBSchemaInfo

#panda_config.dbhost = 'panda-postgres'
proxyS = DBProxy()
proxyS.connect(jedi_config.db.dbhost, jedi_config.db.dbpasswd, jedi_config.db.dbuser, jedi_config.db.dbname)

sql = "select major || '.' || minor || '.' || patch from ATLAS_PANDA.pandadb_version where component = 'SCHEMA'"

res = proxyS.querySQL(sql)
dbVersion = res[0][0].split('.')

# covert string to list of ints to compare numbers
dbVersionIntegers = list(map(int, dbVersion))

minimumSchema = JediDBSchemaInfo.JediDBSchemaInfo().method()
serverDBVersion = minimumSchema.split('.')

# covert string to list of ints to compare numbers
serverDBVersionIntegers = list(map(int, serverDBVersion))

if dbVersionIntegers[0] == serverDBVersionIntegers[0] and dbVersionIntegers[1] == serverDBVersionIntegers[1]:
    if dbVersionIntegers[2] > serverDBVersionIntegers[2]:
        print ("False")
    else:
        print ("True")
else:
    print ("False")
