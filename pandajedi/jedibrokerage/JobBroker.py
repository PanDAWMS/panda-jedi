import sys

from pandajedi.jediconfig import jedi_config

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# factory class for job brokerage
class JobBroker:

    # constructor
    def __init__(self,vo,sourceLabel):
        self.vo = vo
        self.sourceLabel = sourceLabel
        self.impl = None


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk):
        return self.impl.doBrokerage(taskSpec,cloudName,inputChunk)

    
    # initialize
    def initialize(self,ddmIF,taskBufferIF):
        # parse config
        for configStr in jedi_config.jobbroker.modconfig.split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            # check format
            try:
                vo          = items[0]
                sourceLabel = items[1]
                moduleName  = items[2]
                className   = items[3]
            except:
                logger.error('wrong config definition : %s' % configStr)
                continue
            # import
            if vo in [self.vo,'any'] and sourceLabel in [self.sourceLabel,'any']:
                try:
                    # import module
                    mod = __import__(moduleName)
                    for subModuleName in moduleName.split('.')[1:]:
                        mod = getattr(mod,subModuleName)
                    # get class
                    cls = getattr(mod,className)
                    # start child process
                    self.impl = cls(ddmIF,taskBufferIF)
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    logger.error('failed to import impl due to %s %s' % \
                                 (errtype.__name__,errvalue))
                break
        # impl is undefined
        if self.impl == None:
            logger.error('impl is undefined')
            return False
        # return
        return True
    
            
        
                                                            
            
