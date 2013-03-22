import sys
import types

import Interaction

# base class to interact with DDM
class DDMClientBase(object):

    # constructor
    def __init__(self,con):
        self.con = con


    # main loop    
    def start(self):
        while True:
            # get command
            commandObj = self.con.recv()
            # make return
            retObj = Interaction.DDMReturnObject()
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
                    if isinstance(tmpRet,Interaction.StatusCode):
                        # only status code was returned
                        retObj.statusCode = tmpRet
                    elif (isinstance(tmpRet,types.TupleType) or isinstance(tmpRet,types.ListType)) \
                       and len(tmpRet) > 0:
                        retObj.statusCode = tmpRet[0]
                        # status code + return values
                        if len(tmpRet) > 1:
                            retObj.returnValue = tmpRet[1:]
                    else:        
                        raise TypeError,'unformatted return'
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    # failed
                    retObj.statusCode = self.SC_FATAL
                    retObj.errorValue = 'type=%s : %s.%s %s' % \
                                        (errtype.__name__,className,
                                         commandObj.methodName,errvalue)
            # return
            self.con.send(retObj)


# install SCs
Interaction.installSC(DDMClientBase)
