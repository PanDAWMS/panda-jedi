from pandaserver import taskbuffer
import taskbuffer.DBProxyPool

# use customized proxy
from . import JediDBProxy
taskbuffer.DBProxyPool.DBProxy = JediDBProxy

class DBProxyPool(taskbuffer.DBProxyPool.DBProxyPool):

    # constructor
    def __init__(self,dbhost,dbpasswd,nConnection,useTimeout=False):
        taskbuffer.DBProxyPool.DBProxyPool.__init__(self,dbhost,dbpasswd,
                                                    nConnection,useTimeout)
