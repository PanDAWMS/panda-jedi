import re
import sys

from pandajedi.jedicore import Interaction
from TaskRefinerBase import TaskRefinerBase

from pandaserver.dataservice import DataServiceUtils


# brokerage for ATLAS production
class AtlasProdTaskRefiner (TaskRefinerBase):

    # constructor
    def __init__(self,taskBufferIF,ddmIF):
        TaskRefinerBase.__init__(self,taskBufferIF,ddmIF)



    # extract common parameters
    def extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule):
        # set ddmBackEnd
        if not 'ddmBackEnd' in taskParamMap:
            taskParamMap['ddmBackEnd'] = 'rucio'
        TaskRefinerBase.extractCommon(self,jediTaskID,taskParamMap,workQueueMapper,splitRule)



    # main
    def doRefine(self,jediTaskID,taskParamMap):
        # make logger
        tmpLog = self.tmpLog
        tmpLog.debug('start taskType={0}'.format(self.taskSpec.taskType))
        try:
            self.doBasicRefine(taskParamMap)
            # set nosplit+repeat for DBR
            for datasetSpec in self.inSecDatasetSpecList:
                if DataServiceUtils.isDBR(datasetSpec.datasetName):
                    datasetSpec.attributes = 'repeat,nosplit'
            # enable consistency check
            if not self.taskSpec.parent_tid in [None,self.taskSpec.jediTaskID]:
                for datasetSpec in self.inMasterDatasetSpec:
                    if datasetSpec.isMaster() and datasetSpec.type == 'input':
                        datasetSpec.enableCheckConsistency()
            # append attempt number
            for tmpKey,tmpOutTemplateMapList in self.outputTemplateMap.iteritems():
                for tmpOutTemplateMap in tmpOutTemplateMapList:
                    outFileTemplate = tmpOutTemplateMap['filenameTemplate']
                    if re.search('\.\d+$',outFileTemplate) == None and not outFileTemplate.endswith('.panda.um'):
                        tmpOutTemplateMap['filenameTemplate'] = outFileTemplate + '.1'
            # set destination if nessesary
            for datasetSpec in self.outDatasetSpecList:
                storageToken = DataServiceUtils.getDestinationSE(datasetSpec.storageToken)
                if storageToken != None:
                    tmpSiteList = self.ddmIF.getInterface(self.taskSpec.vo).getSitesWithEndPoint(storageToken,self.siteMapper,'production')
                    if tmpSiteList == []:
                        raise RuntimeError,'cannot find online siteID associated to {0}'.format(storageToken)
                    datasetSpec.destination = tmpSiteList[0]
            # set to register datasets
            #self.taskSpec.setToRegisterDatasets()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            tmpLog.error('doBasicRefine failed with {0}:{1}'.format(errtype.__name__,errvalue))
            raise errtype,errvalue
        tmpLog.debug('done')
        return self.SC_SUCCEEDED
            
    
