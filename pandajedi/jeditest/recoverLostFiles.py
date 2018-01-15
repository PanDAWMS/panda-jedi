from pandaserver.config import panda_config
from pandaserver.taskbuffer.TaskBuffer import taskBuffer
from pandaserver.userinterface import Client

import sys
line = sys.argv[1]
datasetName,lostFiles = line.split(':')
lostFiles = set(lostFiles.split(','))

taskBuffer.init(panda_config.dbhost,panda_config.dbpasswd,nDBConnection=1)

res,jediTaskID = taskBuffer.resetFileStatusInJEDI('pandasrv',True,datasetName,lostFiles,[])
if not res:
    print "database error"
else:
    print "  retry jediTaskID={0}".format(jediTaskID)
    print " ", Client.retryTask(jediTaskID)
