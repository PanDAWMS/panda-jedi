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
        self.implMap = {}
        self.className = None
        

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
                    self.className = className
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


    # initialize all modules
    def initializeMods(self,*args):
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
            # check vo and sourceLabel if specified
            if not self.vo in [None,'any'] and self.vo != vo:
                continue
            if not self.sourceLabel in [None,'any'] and self.sourceLabel != sourceLabel:
                continue
            # import
            try:
                # import module
                mod = __import__(moduleName)
                for subModuleName in moduleName.split('.')[1:]:
                    mod = getattr(mod,subModuleName)
                # get class
                cls = getattr(mod,className)
                # instantiate
                impl = cls(*args)
                # append
                if not self.implMap.has_key(vo):
                    self.implMap[vo] = {}
                self.implMap[vo][sourceLabel] = impl
            except:
                errtype,errvalue = sys.exc_info()[:2]
                self.logger.error('failed to import impl due to {0} {1}'.format(errtype.__name__,errvalue))
        # return
        return True


    # get implementation for vo and sourceLabel. Only work with initializeMods()
    def getImpl(self,vo,sourceLabel):
        if self.implMap.has_key(vo):
            # match VO
            voImplMap = self.implMap[vo]
        elif self.implMap.has_key('any'):
            # catch all
            voImplMap =self.implMap['any']
        else:
            return None
        if voImplMap.has_key(sourceLabel):
            # match sourceLabel
            return voImplMap[sourceLabel]
        elif voImplMap.has_key('any'):
            # catch all
            return voImplMap['any']
        else:
            return None
            
        
                                                            
    # get class name of imple
    def getClassName(self,vo=None,sourceLabel=None):
        if self.className != None:
            return self.className
        impl = self.getImpl(vo,sourceLabel)
        if impl == None:
            return None
        return impl.__class__.__name__
