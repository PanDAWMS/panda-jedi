import sys
from MsgWrapper import MsgWrapper

_factoryModuleName = __name__.split('.')[-1]

# base class for factory
class FactoryBase:

    # constructor
    def __init__(self,vos,sourceLabels,logger,modConfig):
        if isinstance(vos,list):
            self.vos = vos
        else:
            try:
                self.vos = vos.split('|')
            except:
                self.vos = [vos]
        if isinstance(sourceLabels,list):
            self.sourceLabels = sourceLabels
        else:
            try:    
                self.sourceLabels = sourceLabels.split('|')
            except:
                self.sourceLabels = [sourceLabels]
        self.modConfig = modConfig
        self.logger = MsgWrapper(logger,_factoryModuleName)
        self.implMap = {}
        self.className = None
        

    # initialize all modules
    def initializeMods(self,*args):
        # parse config
        for configStr in self.modConfig.split(','):
            configStr = configStr.strip()
            items = configStr.split(':')
            # check format
            try:
                vos          = items[0].split('|')
                sourceLabels = items[1].split('|')
                moduleName   = items[2]
                className    = items[3]
            except:
                self.logger('wrong config definition : {0}'.format(configStr))
                continue
            # loop over all VOs
            for vo in vos:
                # loop over all labels
                for sourceLabel in sourceLabels: 
                    # check vo and sourceLabel if specified
                    if not vo in ['','any'] and \
                            not vo in self.vos and \
                            not None in self.vos and \
                            not 'any' in self.vos:
                        continue
                    if not sourceLabel in ['','any'] and \
                            not sourceLabel in self.sourceLabels and \
                            not None in self.sourceLabels and \
                            not 'any' in self.sourceLabels:
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
            
        
                                                            
    # get class name of impl
    def getClassName(self,vo=None,sourceLabel=None):
        impl = self.getImpl(vo,sourceLabel)
        if impl == None:
            return None
        return impl.__class__.__name__
