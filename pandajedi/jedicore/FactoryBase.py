import sys
from MsgWrapper import MsgWrapper

_factoryModuleName = __name__.split('.')[-1]

# base class for factory
class FactoryBase:

    # constructor
    def __init__(self,vo,sourceLabel,logger,modConfig):
        self.vo = vo
        self.sourceLabel = sourceLabel
        self.modConfig = modConfig
        self.logger = MsgWrapper(logger,_factoryModuleName)
        self.impl = None
        

    # initialize
    def initialize(self,*args):
        # parse config
        for configStr in self.modConfig.split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            # check format
            try:
                vo          = items[0]
                sourceLabel = items[1]
                moduleName  = items[2]
                className   = items[3]
            except:
                self.logger('wrong config definition : {0}'.format(configStr))
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
                    self.impl = cls(*args)
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    self.logger.error('failed to import impl due to {0} {1}'.format(errtype.__name__,errvalue))
                break
        # impl is undefined
        if self.impl == None:
            self.logger.error('impl is undefined')
            return False
        # return
        return True
    
            
        
                                                            
            
