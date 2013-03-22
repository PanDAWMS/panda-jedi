import sys
import Queue
import threading
import multiprocessing

from pandajedi.jediconfig import jedi_config

import Interaction


# process class
class _ProcessClass(object):
    # constructor
    def __init__(self,process,connection):
        self.process = process
        self.connection = connection
        self.nused = 0

                     
# method class
class _MethodClass(object):
    # constructor
    def __init__(self,className,methodName,vo,connectionQueue,voIF):
        self.className = className
        self.methodName = methodName
        self.vo = vo
        self.connectionQueue = connectionQueue
        self.voIF = voIF

    # method emulation
    def __call__(self,*args,**kwargs):
        commandObj = Interaction.DDMCommandObject(self.methodName,
                                                  args,kwargs)
        # exceptions
        retException = None
        strException = None
        # get child process
        child_process = self.connectionQueue.get()
        # get pipe
        pipe = child_process.connection
        try:
            # send command
            print 1
            pipe.send(commandObj)
            print 2
            # wait response
            timeoutPeriod = 180
            if not pipe.poll(timeoutPeriod):
                raise Interaction.DDMTimeoutError,"didn't return response for %ssec" % timeoutPeriod
            print 3
            # get response
            ret = pipe.recv()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            retException = Interaction.DDMTemporaryError
            strException = 'VO=%s type=%s : %s.%s %s' % \
                           (self.vo,errtype.__name__,self.className,self.methodName,errvalue)
        # increment nused
        child_process.nused += 1
        # kill old or problematic process
        if child_process.nused > 500 or retException != None:
            # close connection
            pipe.close()
            # terminate child process
            child_process.process.terminate()
            child_process.process.join()
            # make new child process
            self.voIF.launchChild()
        else:
            # release child process
            self.connectionQueue.put(child_process)
        # raise exception
        if retException != None:
            raise retException,strException
        # return
        if ret.statusCode == Interaction.SC_SUCCEEDED:
            return ret.returnValue
        else:
            if ret.statusCode == Interaction.SC_FAILED:
                retException = Interaction.DDMTemporaryError
            else:
                retException = Interaction.DDMFatalError
            raise retException,'VO=%s %s' % (self.vo,ret.errorValue)
        

# VO interface
class _VOInterface(object):
    # constructor
    def __init__(self,vo,maxChild,moduleName,className):
        self.vo = vo
        self.maxChild = maxChild
        self.connectionQueue = Queue.Queue(maxChild)
        self.moduleName = moduleName
        self.className  = className
        

    # factory method
    def __getattr__(self,attrName):
        return _MethodClass(self.className,attrName,self.vo,self.connectionQueue,self)


    # launcher for child processe
    def launcher(self,channel):
        try:
            # import module
            mod = __import__(self.moduleName)
            # get class
            cls = getattr(mod,self.className)
            # start child process
            cls(channel).start()
        except:
            pass
            

    # launch child processes to interact with DDM
    def launchChild(self):
        # make pipe
        parent_conn, child_conn = multiprocessing.Pipe()
        # make child process
        child_process = multiprocessing.Process(target=self.launcher,
                                                args=(child_conn,))
        # keep process in queue        
        processObj = _ProcessClass(child_process,parent_conn)
        self.connectionQueue.put(processObj)
        # start child process
        child_process.start()


    # initialize
    def initialize(self):
        for i in range(self.maxChild):
            self.launchChild()
            
            
    

# interface to DDM
class DDMInterface:
    
    # constructor
    def __init__(self):
        self.interfaceMap = {}


    # setup interface
    def setupInterface(self):
        # parse config
        for configStr in jedi_config.ddmifconfig.split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            print configStr
            # check format
            try:
                vo = items[0]
                maxSize = int(items[1])
                moduleName = items[2]
                className  = items[3]
            except:
                # TODO add config error message
                continue
            # add VO interface
            voIF = _VOInterface(vo,maxSize,moduleName,className)
            voIF.initialize()
            self.interfaceMap[vo] = voIF


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
