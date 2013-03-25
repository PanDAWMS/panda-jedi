import os
import sys
import Queue
import signal
import threading
import multiprocessing
import multiprocessing.reduction

from pandajedi.jediconfig import jedi_config

import Interaction


# process class
class _ProcessClass(object):
    # constructor
    def __init__(self,pid,connection):
        self.pid = pid
        self.nused = 0
        # reduce connection to make it picklable
        self.reduced_pipe = multiprocessing.reduction.reduce_connection(connection)

    # get connection
    def connection(self):
        # rebuild connection
        return self.reduced_pipe[0](*self.reduced_pipe[1])


                     
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
        pipe = child_process.connection()
        try:
            # send command
            pipe.send(commandObj)
            # wait response
            timeoutPeriod = 180
            if not pipe.poll(timeoutPeriod):
                raise Interaction.DDMTimeoutError,"didn't return response for %ssec" % timeoutPeriod
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
            os.kill(child_process.pid,signal.SIGKILL)
            os.waitpid(child_process.pid)
            # make new child process
            self.voIF.launchChild()
        else:
            # remake process object to avoid deadlock due to rebuilding of connection 
            processObj = _ProcessClass(child_process.pid,pipe)
            self.connectionQueue.put(processObj)
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
        #self.connectionQueue = Queue.Queue(maxChild)
        self.connectionQueue = multiprocessing.Queue(maxChild)
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
        processObj = _ProcessClass(child_process.pid,parent_conn)
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
    def dummyClient(dif):
        print "client test"
        dif.getInterface('atlas').test()
        print 'client done'

    dif = DDMInterface()
    dif.setupInterface()
    print "master test"
    atlasIF = dif.getInterface('atlas')
    atlasIF.test()
    print "master done"
    p = multiprocessing.Process(target=dummyClient,
                                args=(dif,))
    p.start()
    p.join()
