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
        self.classMap = {}
        

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
                try:
                    subTypes = items[4].split('|')
                except:
                    subTypes = ['any']
            except:
                self.logger.error('wrong config definition : {0}'.format(configStr))
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
                    # loop over all sub types
                    for subType in subTypes:
                        # import
                        try:
                            # import module
                            self.logger.info("vo={0} label={1} subtype={2}".format(vo,sourceLabel,subType))
                            self.logger.info("importing {0}".format(moduleName))
                            mod = __import__(moduleName)
                            for subModuleName in moduleName.split('.')[1:]:
                                mod = getattr(mod,subModuleName)
                            # get class
                            self.logger.info("getting class {0}".format(className))
                            cls = getattr(mod,className)
                            # instantiate
                            self.logger.info("instantiating")
                            impl = cls(*args)
                            # set vo
                            impl.vo = vo
                            impl.prodSourceLabel = sourceLabel
                            # append
                            if not self.implMap.has_key(vo):
                                self.implMap[vo] = {}
                                self.classMap[vo] = {}
                            if not self.implMap[vo].has_key(sourceLabel):
                                self.implMap[vo][sourceLabel] = {}
                                self.classMap[vo][sourceLabel] = {}
                            self.implMap[vo][sourceLabel][subType] = impl
                            self.classMap[vo][sourceLabel][subType] = cls
                            self.logger.info("{0} is ready for {1}:{2}:{3}".format(cls,vo,sourceLabel,subType))
                        except:
                            errtype,errvalue = sys.exc_info()[:2]
                            self.logger.error(
                                'failed to import {mn}.{cn} for vo={vo} label={lb} subtype={st} due to {et} {ev}'.format(et=errtype.__name__,
                                                                                                                         ev=errvalue,
                                                                                                                         st=subType,
                                                                                                                         vo=vo,
                                                                                                                         lb=sourceLabel,
                                                                                                                         cn=className,
                                                                                                                         mn=moduleName)
                                )
                            raise ImportError, 'failed to import {0}.{1}'.format(moduleName,className)
        # return
        return True


    # get implementation for vo and sourceLabel. Only work with initializeMods()
    def getImpl(self,vo,sourceLabel,subType='any',doRefresh=True):
        # check VO
        if self.implMap.has_key(vo):
            # match VO
            voImplMap = self.implMap[vo]
        elif self.implMap.has_key('any'):
            # catch all
            voImplMap = self.implMap['any']
        else:
            return None
        # check sourceLabel
        if voImplMap.has_key(sourceLabel):
            # match sourceLabel
            srcImplMap = voImplMap[sourceLabel]
        elif voImplMap.has_key('any'):
            # catch all
            srcImplMap = voImplMap['any']
        else:
            return None
        # check subType
        if srcImplMap.has_key(subType):
            # match subType
            tmpImpl = srcImplMap[subType]
            if doRefresh:
                tmpImpl.refresh()
            return tmpImpl
        elif srcImplMap.has_key('any'):
            # catch all
            tmpImpl = srcImplMap['any']
            if doRefresh:
                tmpImpl.refresh()
            return tmpImpl
        else:
            return None


    # instantiate implementation for vo and sourceLabel. Only work with initializeMods()
    def instantiateImpl(self,vo,sourceLabel,subType,*args):
        # check VO
        if self.classMap.has_key(vo):
            # match VO
            voImplMap = self.classMap[vo]
        elif self.classMap.has_key('any'):
            # catch all
            voImplMap = self.classMap['any']
        else:
            return None
        # check sourceLabel
        if voImplMap.has_key(sourceLabel):
            # match sourceLabel
            srcImplMap = voImplMap[sourceLabel]
        elif voImplMap.has_key('any'):
            # catch all
            srcImplMap = voImplMap['any']
        else:
            return None
        # check subType
        if srcImplMap.has_key(subType):
            # match subType
            impl = srcImplMap[subType](*args)
            impl.vo = vo
            impl.prodSourceLabel= sourceLabel
            return impl
        elif srcImplMap.has_key('any'):
            # catch all
            impl = srcImplMap['any'](*args)
            impl.vo = vo
            impl.prodSourceLabel= sourceLabel
            return impl
        else:
            return None
            
        
                                                            
    # get class name of impl
    def getClassName(self,vo=None,sourceLabel=None):
        impl = self.getImpl(vo,sourceLabel,doRefresh=False)
        if impl == None:
            return None
        return impl.__class__.__name__
