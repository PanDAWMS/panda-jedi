import time
import Queue
import threading
import multiprocessing

import DDMCommandObject
import DDMReturnObject
from DDMExceptionObject import *


# method class
class _MethodClass(object):
    # constructor
    def __init__(self,methodName,vo,connectionQueue):
        self.methodName = methodName
        self.vo = vo
        self.connectionQueue = connectionQueue

    # method emulation
    def __call__(self,*args,**kwargs):
        commandObj = DDMCommandObject.DDMCommandObject(self.methodName,
                                                 args,
                                                 kwargs)
        # get pipe
        pipe = self.connectionQueue.get()
        # send command
        pipe.send(commandObj)
        # get response
        ret = pipe.recv()
        # release pipe
        self.connectionQueue.put(pipe)
        # return
        if ret.statusCode == DDMReturnObject.SC_SUCCEEDED:
            return ret.returnValue
        else:
            if ret.statusCode == DDMReturnObject.SC_FAILED:
                retException = DDMTemporaryError
            else:
                retException = DDMFatalError
            raise retException,'VO=%s %s' % (self.vo,ret.errorValue)
        

# VO interface
class _VOInterface(object):
    # constructor
    def __init__(self,vo,connectionQueue):
        self.vo = vo
        self.connectionQueue = connectionQueue

    # factory method
    def __getattr__(self,methodName):
        return _MethodClass(methodName,self.vo,self.connectionQueue)
            
    

# interface to DDM
class DDMInterface:
    
    # constructor
    def __init__(self):
        self.interfaceMap = {}


    # launch child processes to interact with DDM
    def launchChild(self,channel,moduleName,className):
        try:
            # import module
            mod = __import__(moduleName)
            # get class
            cls = getattr(mod,className)
            # start child process
            cls(channel).start()
        except:
            pass


    # setup interface
    def setupInterface(self):    
        maxSize = 3
        vo = 'atlas'
        moduleName = 'AtlasDDMClient'
        className  = 'AtlasDDMClient'
        # make queue to keep pipes
        connectionQueue = Queue.Queue(maxSize)
        for i in range(maxSize):
            # make pipe
            parent_conn, child_conn = multiprocessing.Pipe()
            # keep pipe in queue
            connectionQueue.put(parent_conn)
            # start child process
            p = multiprocessing.Process(target=self.launchChild,
                                        args=(child_conn,moduleName,className))
            p.start()
        # add VO interface    
        self.interfaceMap[vo] = _VOInterface(vo,connectionQueue)
        print "start"


    # get interface with VO
    def getInterface(self,vo):
        if self.interfaceMap.has_key(vo):
            return self.interfaceMap[vo]
        # not found
        return None


if __name__ == '__main__':
    dif = DDMInterface()
    dif.setupInterface()
    atlasIF = dif.getInterface('atlas')
    atlasIF.test()
