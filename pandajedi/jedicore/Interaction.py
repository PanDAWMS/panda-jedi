import os
import sys
import types
import multiprocessing
import multiprocessing.reduction


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
        return self.value == other.value

    def __ne__(self,other):
        return self.value != other.value


    
# mapping to accessors   
statusCodeMap = {'SC_SUCCEEDED': StatusCode(0),
                 'SC_FAILED'   : StatusCode(1),
                 'SC_FATAL'    : StatusCode(2),
                 }


# install the list of status codes to a class
def installSC(cls):
    for sc,val in statusCodeMap.iteritems():
        setattr(cls,sc,val)


# install SCs in this module
installSC(sys.modules[ __name__ ])

        
###########################################################
#
# classes for IPC
#

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
        # reduce connection to make it picklable
        self.reduced_pipe = multiprocessing.reduction.reduce_connection(connection)

    # get connection
    def connection(self):
        # rebuild connection
        return self.reduced_pipe[0](*self.reduced_pipe[1])


                     
# method class
class MethodClass(object):
    # constructor
    def __init__(self,className,methodName,vo,connectionQueue,voIF):
        self.className = className
        self.methodName = methodName
        self.vo = vo
        self.connectionQueue = connectionQueue
        self.voIF = voIF

    # method emulation
    def __call__(self,*args,**kwargs):
        commandObj = CommandObject(self.methodName,
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
                raise JEDITimeoutError,"didn't return response for %ssec" % timeoutPeriod
            # get response
            ret = pipe.recv()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            retException = JEDITemporaryError
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
            processObj = ProcessClass(child_process.pid,pipe)
            self.connectionQueue.put(processObj)
        # raise exception
        if retException != None:
            raise retException,strException
        # return
        if ret.statusCode == SC_SUCCEEDED:
            return ret.returnValue
        else:
            if ret.statusCode == SC_FAILED:
                retException = JEDITemporaryError
            else:
                retException = JEDIFatalError
            raise retException,'VO=%s %s' % (self.vo,ret.errorValue)
        


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
        processObj = ProcessClass(child_process.pid,parent_conn)
        self.connectionQueue.put(processObj)
        # start child process
        child_process.start()


    # initialize
    def initialize(self):
        for i in range(self.maxChild):
            self.launchChild()



# interface class to receive command
class CommandReceiveInterface(object):

    # constructor
    def __init__(self,con):
        self.con = con


    # main loop    
    def start(self):
        while True:
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
                    # get function
                    functionObj = getattr(self,commandObj.methodName)
                    # exec
                    tmpRet = apply(functionObj,commandObj.argList,commandObj.argMap)
                    if isinstance(tmpRet,StatusCode):
                        # only status code was returned
                        retObj.statusCode = tmpRet
                    elif (isinstance(tmpRet,types.TupleType) or isinstance(tmpRet,types.ListType)) \
                       and len(tmpRet) > 0 and isinstance(tmpRet[0],StatusCode):
                            retObj.statusCode = tmpRet[0]
                            # status code + return values
                            if len(tmpRet) > 1:
                                retObj.returnValue = tmpRet[1:]
                    else:        
                        retObj.statusCode = self.SC_SUCCEEDED
                        retObj.returnValue = tmpRet
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    # failed
                    retObj.statusCode = self.SC_FATAL
                    retObj.errorValue = 'type=%s : %s.%s : %s' % \
                                        (errtype.__name__,className,
                                         commandObj.methodName,errvalue)
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



