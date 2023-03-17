import os
import sys
import time
import types
import signal
import datetime
import multiprocessing
import multiprocessing.reduction

try:
    from multiprocessing.connection import reduce_connection
except ImportError:
    from multiprocessing.reduction import reduce_connection

from six import iteritems

# patch multiprocessing
from . import JediPatch

#import multiprocessing
#logger = multiprocessing.log_to_stderr()
#logger.setLevel(multiprocessing.SUBDEBUG)

###########################################################
#
# status codes for IPC
#

# class for status code
class StatusCode(object):
    def __init__(self,value):
        self.value = value

    def __str__(self):
        return "%s" % self.value

    # comparator
    def __eq__(self,other):
        try:
            return self.value == other.value
        except Exception:
            return False

    def __ne__(self,other):
        try:
            return self.value != other.value
        except Exception:
            return True



# mapping to accessors
statusCodeMap = {'SC_SUCCEEDED': StatusCode(0),
                 'SC_FAILED'   : StatusCode(1),
                 'SC_FATAL'    : StatusCode(2),
                 }


# install the list of status codes to a class
def installSC(cls):
    for sc,val in iteritems(statusCodeMap):
        setattr(cls,sc,val)


# install SCs in this module
installSC(sys.modules[ __name__ ])


###########################################################
#
# classes for IPC
#

# log message with timestamp
def dumpStdOut(sender,message):
    timeNow = datetime.datetime.utcnow()
    print("{0} {1}: INFO    {2}".format(str(timeNow),sender,message))



# object class for command
class CommandObject(object):

    # constructor
    def __init__(self,methodName,argList,argMap):
        self.methodName = methodName
        self.argList = argList
        self.argMap = argMap



# object class for response
class ReturnObject(object):

    # constructor
    def __init__(self):
        self.statusCode  = None
        self.errorValue  = None
        self.returnValue = None



# process class
class ProcessClass(object):
    # constructor
    def __init__(self,pid,connection):
        self.pid = pid
        self.nused = 0
        self.usedMemory = 0
        self.nMemLookup = 20
        # reduce connection to make it picklable
        self.reduced_pipe = reduce_connection(connection)

    # get connection
    def connection(self):
        # rebuild connection
        return self.reduced_pipe[0](*self.reduced_pipe[1])

    # reduce connection
    def reduceConnection(self,connection):
        self.reduced_pipe = reduce_connection(connection)

    # get memory usage
    def getMemUsage(self):
        # update memory info
        if self.nused % self.nMemLookup == 0:
            try:
                # read memory info from /proc
                t = open('/proc/{0}/status'.format(self.pid))
                v = t.read()
                t.close()
                value = 0
                for line in v.split('\n'):
                    if line.startswith('VmRSS'):
                        items = line.split()
                        value = int(items[1])
                        if items[2] in ['kB','KB']:
                            value /= 1024
                        elif items[2] in ['mB','MB']:
                            pass
                        break
                self.usedMemory = value
            except Exception:
                pass
            return self.usedMemory
        else:
            return None


# method class
class MethodClass(object):
    # constructor
    def __init__(self,className,methodName,vo,connectionQueue,voIF):
        self.className = className
        self.methodName = methodName
        self.vo = vo
        self.connectionQueue = connectionQueue
        self.voIF = voIF
        self.pipeList = []

    # method emulation
    def __call__(self,*args,**kwargs):
        commandObj = CommandObject(self.methodName,
                                   args,kwargs)
        nTry = 3
        for iTry in range(nTry):
            # exceptions
            retException = None
            strException = None
            try:
                stepIdx = 0
                # get child process
                child_process = self.connectionQueue.get()
                # get pipe
                stepIdx = 1
                pipe = child_process.connection()
                # get ack
                stepIdx = 2
                timeoutPeriodACK = 30
                if not pipe.poll(timeoutPeriodACK):
                    raise JEDITimeoutError("did not get ACK for %ssec" % timeoutPeriodACK)
                ack = pipe.recv()
                # send command
                stepIdx = 3
                pipe.send(commandObj)
                # wait response
                stepIdx = 4
                timeoutPeriod = 600
                timeNow = datetime.datetime.utcnow()
                if not pipe.poll(timeoutPeriod):
                    raise JEDITimeoutError("did not get response for %ssec" % timeoutPeriod)
                regTime = datetime.datetime.utcnow() - timeNow
                if regTime > datetime.timedelta(seconds=60):
                    dumpStdOut(self.className,
                              'methodName={} took {}.{:03d} sec in pid={}'.format(self.methodName,
                                                                                  regTime.seconds,
                                                                                  int(regTime.microseconds / 1000),
                                                                                  child_process.pid))
                # get response
                stepIdx = 5
                ret = pipe.recv()
                # set exception type based on error
                stepIdx = 6
                if ret.statusCode == SC_FAILED:
                    retException = JEDITemporaryError
                elif ret.statusCode == SC_FATAL:
                    retException = JEDIFatalError
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                retException = errtype
                argStr = 'args=%s kargs=%s' % (str(args),str(kwargs))
                strException = 'VO=%s type=%s stepIdx=%s : %s.%s %s %s' % \
                               (self.vo,errtype.__name__,stepIdx,
                                self.className,self.methodName,errvalue,
                                argStr[:200])
            # increment nused
            child_process.nused += 1
            # memory check
            largeMemory = False
            memUsed = child_process.getMemUsage()
            if memUsed is not None:
                memStr= 'pid={0} memory={1}MB'.format(child_process.pid,memUsed)
                if memUsed > 1.5*1024:
                    largeMemory = True
                    memStr += ' exceeds memory limit'
                    dumpStdOut(self.className,memStr)
            # kill old or problematic process
            if child_process.nused > 1000 or retException not in [None,JEDITemporaryError,JEDIFatalError] or \
                    largeMemory:
                dumpStdOut(self.className,'methodName={0} ret={1} nused={2} {3} in pid={4}'.format(self.methodName,retException,
                                                                                                   child_process.nused,
                                                                                                   strException,
                                                                                                   child_process.pid))
                # close connection
                try:
                    pipe.close()
                except Exception:
                    pass
                # terminate child process
                try:
                    dumpStdOut(self.className,'killing pid={0}'.format(child_process.pid))
                    os.kill(child_process.pid,signal.SIGKILL)
                    dumpStdOut(self.className,'waiting pid={0}'.format(child_process.pid))
                    os.waitpid(child_process.pid,0)
                    dumpStdOut(self.className,'terminated pid={0}'.format(child_process.pid))
                except Exception:
                    errtype,errvalue = sys.exc_info()[:2]
                    if 'No child processes' not in str(errvalue):
                        dumpStdOut(self.className,'failed to terminate {0} with {1}:{2}'.format(child_process.pid,
                                                                                                errtype,errvalue))
                # make new child process
                self.voIF.launchChild()
            else:
                # reduce process object to avoid deadlock due to rebuilding of connection
                child_process.reduceConnection(pipe)
                self.connectionQueue.put(child_process)
            # success, fatal error, or maximally attempted
            if retException in [None,JEDIFatalError] or (iTry+1 == nTry):
                break
            # sleep
            time.sleep(1)
        # raise exception
        if retException is not None:
            if strException is None:
                strException = 'VO={0} {1}'.format(self.vo,ret.errorValue)
            raise retException(strException)
        # return
        if ret.statusCode == SC_SUCCEEDED:
            return ret.returnValue
        else:
            raise retException('VO=%s %s' % (self.vo,ret.errorValue))



# interface class to send command
class CommandSendInterface(object):
    # constructor
    def __init__(self,vo,maxChild,moduleName,className):
        self.vo = vo
        self.maxChild = maxChild
        self.connectionQueue = multiprocessing.Queue(maxChild)
        self.moduleName = moduleName
        self.className  = className


    # factory method
    def __getattr__(self,attrName):
        return MethodClass(self.className,attrName,self.vo,self.connectionQueue,self)


    # launcher for child processe
    def launcher(self,channel):
        # import module
        mod = __import__(self.moduleName)
        for subModuleName in self.moduleName.split('.')[1:]:
            mod = getattr(mod,subModuleName)
        # get class
        cls = getattr(mod,self.className)
        # start child process
        msg = "start {0} with pid={1}".format(self.className,os.getpid())
        dumpStdOut(self.moduleName,msg)
        timeNow = datetime.datetime.utcnow()
        try:
            cls(channel).start()
        except Exception:
            errtype,errvalue = sys.exc_info()[:2]
            dumpStdOut(self.className,'launcher crashed with {0}:{1}'.format(errtype,errvalue))



    # launch child processes to interact with DDM
    def launchChild(self):
        # make pipe
        parent_conn, child_conn = multiprocessing.Pipe()
        # make child process
        child_process = multiprocessing.Process(target=self.launcher,
                                                args=(child_conn,))
        # start child process
        child_process.daemon = True
        child_process.start()
        # keep process in queue
        processObj = ProcessClass(child_process.pid,parent_conn)
        # sync to wait until object in the child process is instantiated
        pipe = processObj.connection()
        pipe.recv()
        processObj.reduceConnection(pipe)
        # ready
        self.connectionQueue.put(processObj)


    # initialize
    def initialize(self):
        for i in range(self.maxChild):
            self.launchChild()



# interface class to receive command
class CommandReceiveInterface(object):

    # constructor
    def __init__(self,con):
        self.con = con
        self.cacheMap = {}


    # make key for cache
    def makeKey(self,className,methodName,argList,argMap):
        try:
            tmpKey = '{0}:{1}:'.format(className,methodName)
            for argItem in argList:
                tmpKey += '{0}:'.format(str(argItem))
            for argKey,argVal in iteritems(argMap):
                tmpKey += '{0}={1}:'.format(argKey,str(argVal))
            tmpKey = tmpKey[:-1]
            return tmpKey
        except Exception:
            return None


    # main loop
    def start(self):
        # sync
        self.con.send('ready')
        # main loop
        while True:
            # send ACK
            self.con.send('ack')
            # get command
            commandObj = self.con.recv()
            # make return
            retObj = ReturnObject()
            # get class name
            className = self.__class__.__name__
            # check method name
            if not hasattr(self,commandObj.methodName):
                # method not found
                retObj.statusCode = self.SC_FATAL
                retObj.errorValue = 'type=AttributeError : %s instance has no attribute %s' % \
                    (className,commandObj.methodName)
            else:
                try:
                    # use cache
                    useCache = False
                    doExec = True
                    if 'useResultCache' in commandObj.argMap:
                        # get time range
                        timeRange = commandObj.argMap['useResultCache']
                        # delete from args map
                        del commandObj.argMap['useResultCache']
                        # make key for cache
                        tmpCacheKey = self.makeKey(className,commandObj.methodName,commandObj.argList,commandObj.argMap)
                        if tmpCacheKey is not None:
                            useCache = True
                            # cache is fresh
                            if tmpCacheKey in self.cacheMap and \
                               self.cacheMap[tmpCacheKey]['utime']+datetime.timedelta(seconds=timeRange) > datetime.datetime.utcnow():
                                tmpRet = self.cacheMap[tmpCacheKey]['value']
                                doExec = False
                    # exec
                    if doExec:
                        # get function
                        functionObj = getattr(self,commandObj.methodName)
                        # exec
                        tmpRet = functionObj(*commandObj.argList, **commandObj.argMap)
                    if isinstance(tmpRet,StatusCode):
                        # only status code was returned
                        retObj.statusCode = tmpRet
                    elif (isinstance(tmpRet, tuple) or isinstance(tmpRet, list)) \
                       and len(tmpRet) > 0 and isinstance(tmpRet[0],StatusCode):
                            retObj.statusCode = tmpRet[0]
                            # status code + return values
                            if len(tmpRet) > 1:
                                if retObj.statusCode == self.SC_SUCCEEDED:
                                    if len(tmpRet) == 2:
                                        retObj.returnValue = tmpRet[1]
                                    else:
                                        retObj.returnValue = tmpRet[1:]
                                else:
                                    if len(tmpRet) == 2:
                                        retObj.errorValue = tmpRet[1]
                                    else:
                                        retObj.errorValue = tmpRet[1:]
                    else:
                        retObj.statusCode = self.SC_SUCCEEDED
                        retObj.returnValue = tmpRet
                except Exception:
                    errtype,errvalue = sys.exc_info()[:2]
                    # failed
                    retObj.statusCode = self.SC_FATAL
                    retObj.errorValue = 'type=%s : %s.%s : %s' % \
                                        (errtype.__name__,className,
                                         commandObj.methodName,errvalue)
                # cache
                if useCache and doExec and retObj.statusCode == self.SC_SUCCEEDED:
                    self.cacheMap[tmpCacheKey] = {'utime':datetime.datetime.utcnow(),
                                                  'value':tmpRet}
            # return
            self.con.send(retObj)


# install SCs
installSC(CommandReceiveInterface)




###########################################################
#
# exceptions for IPC
#

# exception for temporary error
class JEDITemporaryError(Exception):
    pass


# exception for fatal error
class JEDIFatalError(Exception):
    pass


# exception for timeout error
class JEDITimeoutError(Exception):
    pass


# exception for no child error
class JEDINoChildError(Exception):
    pass
