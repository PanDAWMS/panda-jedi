# DB API for JEDI

from pandajedi.jediconfig import jedi_config

from pandaserver import taskbuffer
import taskbuffer.TaskBuffer
import JediDBProxyPool
from Interaction import CommandReceiveInterface

# use customized proxy pool
taskbuffer.TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool

class JediTaskBuffer(taskbuffer.TaskBuffer.TaskBuffer,CommandReceiveInterface):
    # constructor
    def __init__(self,conn):
        CommandReceiveInterface.__init__(self,conn)
        taskbuffer.TaskBuffer.TaskBuffer.__init__(self)
        taskbuffer.TaskBuffer.TaskBuffer.init(self,jedi_config.dbhost,
                                              jedi_config.dbpasswd,
                                              nDBConnection=1)

    def testIF(self):
        print 123
        return True
    
