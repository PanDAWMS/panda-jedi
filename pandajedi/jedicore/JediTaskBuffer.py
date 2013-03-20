# DB API for JEDI

from pandaserver import taskbuffer
import taskbuffer.TaskBuffer
import JediDBProxyPool

# use customized proxy pool
taskbuffer.TaskBuffer.DBProxyPool = JediDBProxyPool.DBProxyPool

class JediTaskBuffer(taskbuffer.TaskBuffer.TaskBuffer):
    # constructor
    def __init__(self):
        taskbuffer.TaskBuffer.TaskBuffer.__init__(self)

