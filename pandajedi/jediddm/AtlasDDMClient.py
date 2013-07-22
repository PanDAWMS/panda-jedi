import re
import os
import sys
import lfc
import socket
import random

from pandajedi.jedicore.MsgWrapper import MsgWrapper

from DDMClientBase import DDMClientBase

from dq2.clientapi.DQ2 import DQ2
from dq2.info import TiersOfATLAS
from dq2.common.DQConstants import DatasetState, Metadata
from dq2.clientapi.DQ2 import \
    DQUnknownDatasetException, \
    DQDatasetExistsException
from dq2.container.exceptions import DQContainerExistsException


from pandaserver.dataservice import DataServiceUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# class to access to ATLAS DDM
class AtlasDDMClient(DDMClientBase):

    # constructor
    def __init__(self,con):
        # initialize base class
        DDMClientBase.__init__(self,con)
        # the list of fatal error
        self.fatalErrors = [DQUnknownDatasetException]



    # get files in dataset
    def getFilesInDataset(self,datasetName,skipDuplicate=True):
        methodName = 'getFilesInDataset'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listFilesInDataset(datasetName)
            if tmpRet == ():
                fileMap = {}
            else:
                fileMap = tmpRet[0]
                # skip duplicated files
                if skipDuplicate:
                    newFileMap = {}
                    baseLFNmap = {}
                    for tmpGUID,valMap in fileMap.iteritems():
                        # extract base LFN and attempt number
                        lfn = valMap['lfn']
                        baseLFN = re.sub('(\.(\d+))$','',lfn)
                        attNr = re.sub(baseLFN+'\.*','',lfn)
                        if attNr == '':
                            # without attempt number
                            attNr = -1
                        else:
                            attNr = int(attNr)
                        # compare attempt numbers    
                        addMap = False    
                        if baseLFNmap.has_key(baseLFN):
                            # use larger attempt number
                            oldMap = baseLFNmap[baseLFN]
                            if oldMap['attNr'] < attNr:
                                del newFileMap[oldMap['guid']]
                                addMap = True
                        else:
                            addMap = True
                        # append    
                        if addMap:    
                            baseLFNmap[baseLFN] = {'guid':tmpGUID,
                                                   'attNr':attNr}
                            newFileMap[tmpGUID] = valMap
                    # use new map
                    fileMap = newFileMap        
            return self.SC_SUCCEEDED,fileMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errStr = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            return errCode,'{0} : {1}'.format(methodName,errStr)



    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        methodName = 'listDatasetReplicas'
        try:
            # get DQ2 API            
            dq2=DQ2()
            if not datasetName.endswith('/'):
                # get file list
                tmpRet = dq2.listDatasetReplicas(datasetName,old=False)
                return self.SC_SUCCEEDED,tmpRet
            else:
                # list of attributes summed up
                attrList = ['total','found']
                retMap = {}
                # get constituent datasets
                dsList = dq2.listDatasetsInContainer(datasetName)
                for tmpName in dsList:
                    tmpRet = dq2.listDatasetReplicas(tmpName,old=False)
                    # loop over all sites
                    for tmpSite,tmpValMap in tmpRet.iteritems():
                        # add site
                        if not retMap.has_key(tmpSite):
                            retMap[tmpSite] = [{}]
                        # loop over all attributes
                        for tmpAttr in attrList:
                            # add default
                            if not retMap[tmpSite][-1].has_key(tmpAttr):
                                retMap[tmpSite][-1][tmpAttr] = 0
                            # sum
                            try:
                                retMap[tmpSite][-1][tmpAttr] += int(tmpValMap[-1][tmpAttr])
                            except:
                                # unknown
                                retMap[tmpSite][-1][tmpAttr] = None
                # return
                return self.SC_SUCCEEDED,retMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)



    # get site property
    def getSiteProperty(self,seName,attribute):
        methodName = 'getSiteProperty'
        try:
            return self.SC_SUCCEEDED,TiersOfATLAS.getSiteProperty(seName,attribute)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)



    # check if endpoint is NG
    def checkNGEndPoint(self,endPoint,ngList):
        for ngPatt in ngList:
            if re.search(ngPatt,endPoint) != None:
                return True
        return False    
        

                        
    # get available files
    def getAvailableFiles(self,datasetSpec,siteEndPointMap,siteMapper,ngGroup=[],checkLFC=False):
        # make logger
        methodName = 'getAvailableFiles'
        methodName += ' <datasetID={0}>'.format(datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start datasetName={0}'.format(datasetSpec.datasetName))
        try:
            # list of NG endpoints
            ngEndPoints = []
            if 1 in ngGroup:
                ngEndPoints += ['_SCRATCHDISK$','_LOCALGROUPDISK$','_USERDISK$',
                               '_DAQ$','_TMPDISK$','_TZERO$','_GRIDFTP$','MOCKTEST$']
            # get all associated endpoints
            siteAllEndPointsMap = {}
            for siteName,endPointPattList in siteEndPointMap.iteritems():
                # get all endpoints matching with patterns 
                allEndPointList = []
                for endPointPatt in endPointPattList:
                    if '*' in endPointPatt:
                        # wildcard
                        endPointPatt = endPointPatt.replace('*','.*')
                        for endPointToA in TiersOfATLAS.getAllDestinationSites():
                            if re.search('^'+endPointPatt+'$',endPointToA) != None:
                                if not endPointToA in allEndPointList:
                                    allEndPointList.append(endPointToA)
                    else:
                        # normal endpoint
                        if endPointPatt in TiersOfATLAS.getAllDestinationSites() and \
                               not endPointPatt in allEndPointList:
                            allEndPointList.append(endPointPatt)
                # get associated endpoints
                siteAllEndPointsMap[siteName] = []
                for endPoint in allEndPointList:
                    # append
                    if not endPoint in siteAllEndPointsMap[siteName]:
                        siteAllEndPointsMap[siteName].append(endPoint)
                    else:
                        # already checked
                        continue
                    # get alternate name
                    altName = TiersOfATLAS.getSiteProperty(endPoint,'alternateName')
                    if altName != None and altName != ['']:
                        for assEndPoint in TiersOfATLAS.resolveGOC({altName[0]:None})[altName[0]]:
                            if not assEndPoint in siteAllEndPointsMap[siteName] and \
                                   not self.checkNGEndPoint(assEndPoint,ngEndPoints):
                                siteAllEndPointsMap[siteName].append(assEndPoint)
            # get replica map
            tmpStat,tmpOut = self.listDatasetReplicas(datasetSpec.datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get dataset replicas with {0}'.format(tmpOut))
                raise tmpStat,tmpOut
            datasetReplicaMap = tmpOut
            # collect SE, LFC hosts, storage path, storage type
            lfcSeMap = {}
            storagePathMap = {}
            completeReplicaMap = {}
            siteHasCompleteReplica = False
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                tmpLfcSeMap = {}
                tmpStoragePathMap = {}
                for tmpEndPoint in allEndPointList:
                    # storage type
                    if TiersOfATLAS.isTapeSite(tmpEndPoint):
                        storageType = 'localtape'
                    else:
                        storageType = 'localdisk'
                    # no scan when site has complete replicas
                    if datasetReplicaMap.has_key(tmpEndPoint) and datasetReplicaMap[tmpEndPoint][-1]['found'] != None \
                       and datasetReplicaMap[tmpEndPoint][-1]['total'] == datasetReplicaMap[tmpEndPoint][-1]['found']:
                        completeReplicaMap[tmpEndPoint] = storageType
                        siteHasCompleteReplica = True
                    # no LFC scan for many-time datasets
                    if datasetSpec.isManyTime():
                        continue
                    # get LFC
                    lfcStr = TiersOfATLAS.getLocalCatalog(tmpEndPoint)
                    try:
                        lfc = lfcStr.split('/')[2].split(':')[0]
                    except:
                        tmpLog.error('faild to extract LFC from %s for %s:%s' % \
                                     (lfcStr,siteName,tmpEndPoint))
                        continue
                    # add map
                    if not tmpLfcSeMap.has_key(lfc):
                        tmpLfcSeMap[lfc] = []
                    # get SE
                    seStr = TiersOfATLAS.getSiteProperty(tmpEndPoint, 'srm')
                    tmpMatch = re.search('://([^:/]+):*\d*/',seStr)
                    if tmpMatch != None:
                        se = tmpMatch.group(1)
                        if not se in tmpLfcSeMap[lfc]:
                            tmpLfcSeMap[lfc].append(se)
                    else:
                        tmpLog.error('faild to extract SE from %s for %s:%s' % \
                                     (seStr,siteName,tmpEndPoint))
                    # get SE + path
                    seStr = TiersOfATLAS.getSiteProperty(tmpEndPoint, 'srm')
                    tmpMatch = re.search('(srm://.+)$',seStr)
                    if tmpMatch == None:
                        tmpLog.error('faild to extract SE+PATH from %s for %s:%s' % \
                                     (seStr,siteName,tmpEndPoint))
                        continue
                    # add full path to storage map
                    tmpSePath = tmpMatch.group(1)
                    tmpStoragePathMap[tmpSePath] = {'siteName':siteName,'storageType':storageType}
                    # add compact path
                    tmpSePath = re.sub('(:\d+)*/srm/[^\?]+\?SFN=','',tmpSePath)
                    tmpStoragePathMap[tmpSePath] = {'siteName':siteName,'storageType':storageType}
                # add to map to trigger LFC scan if complete replica is missing at the site
                if not siteHasCompleteReplica or checkLFC:
                    for tmpKey,tmpVal in tmpLfcSeMap.iteritems():
                        if not lfcSeMap.has_key(tmpKey):
                            lfcSeMap[tmpKey] = []
                        lfcSeMap[tmpKey] += tmpVal
                    for tmpKey,tmpVal in tmpStoragePathMap.iteritems():
                        storagePathMap[tmpKey] = tmpVal
            # collect GUIDs and LFNs
            fileMap        = {}
            lfnMap         = {}
            lfnFileSepcMap = {}
            for tmpFile in datasetSpec.Files:
                fileMap[tmpFile.GUID] = tmpFile.lfn
                lfnMap[tmpFile.lfn] = tmpFile
                lfnFileSepcMap[tmpFile.lfn] = tmpFile
            # get SURLs
            surlMap = {}
            for lfcHost,seList in lfcSeMap.iteritems():
                tmpLog.debug('lookup in LFC:{0} for {1}'.format(lfcHost,str(seList)))               
                tmpStat,tmpRetMap = self.getSURLsFromLFC(fileMap,lfcHost,seList)
                tmpLog.debug(str(tmpStat))
                if tmpStat != self.SC_SUCCEEDED:
                    raise RuntimeError,tmpRetMap
                for lfn,surls in tmpRetMap.iteritems():
                    if not surlMap.has_key(lfn):
                        surlMap[lfn] = surls
                    else:
                        surlMap[lfn] += surls
            # make return
            returnMap = {}
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                # set default return values
                if not returnMap.has_key(siteName):
                    returnMap[siteName] = {'localdisk':[],'localtape':[],'cache':[],'remote':[]}
                # loop over all files    
                tmpSiteSpec = siteMapper.getSite(siteName)                
                # check if the file is cached
                if DataServiceUtils.isCachedFile(datasetSpec.datasetName,tmpSiteSpec):
                    for tmpFileSpec in datasetSpec.Files:
                        # add to cached file list
                        returnMap[siteName]['cache'].append(tmpFileSpec)
                # complete replicas
                if not checkLFC:        
                    for tmpEndPoint in allEndPointList:
                        if completeReplicaMap.has_key(tmpEndPoint):
                            storageType = completeReplicaMap[tmpEndPoint]
                            returnMap[siteName][storageType] += datasetSpec.Files
            # loop over all available LFNs
            avaLFNs = surlMap.keys()
            avaLFNs.sort()
            for tmpLFN in avaLFNs:
                tmpFileSpec = lfnFileSepcMap[tmpLFN]                
                # loop over all SURLs
                for tmpSURL in surlMap[tmpLFN]:
                    for tmpSePath in storagePathMap.keys():
                        # check SURL
                        if tmpSURL.startswith(tmpSePath):
                            # add
                            siteName = storagePathMap[tmpSePath]['siteName']
                            storageType = storagePathMap[tmpSePath]['storageType']
                            if not tmpFileSpec in returnMap[siteName][storageType]:
                                returnMap[siteName][storageType].append(tmpFileSpec)
                            break
            # dump
            dumpStr = ''
            for siteName,storageTypeFile in returnMap.iteritems():
                dumpStr += '{0}:('.format(siteName)
                for storageType,fileList in storageTypeFile.iteritems():
                    dumpStr += '{0}:{1},'.format(storageType,len(fileList))
                dumpStr = dumpStr[:-1]
                dumpStr += ') '
            dumpStr= dumpStr[:-1]
            tmpLog.debug(dumpStr)
            # return
            tmpLog.info('done')            
            return self.SC_SUCCEEDED,returnMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed with {0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return self.SC_FAILED,'{0}.{1} {2}'.format(self.__class__.__name__,methodName,errMsg)
        

    # get SURLs from LFC
    def getSURLsFromLFC(self,files,lfcHost,storages,verbose=False):
        # randomly resolve DNS alias
        if lfcHost in ['prod-lfc-atlas.cern.ch']:
            lfcHost = random.choice(socket.gethostbyname_ex(lfcHost)[2])
        # set LFC HOST
        os.environ['LFC_HOST'] = lfcHost
        # timeout
        os.environ['LFC_CONNTIMEOUT'] = '60'
        os.environ['LFC_CONRETRY']    = '2'
        os.environ['LFC_CONRETRYINT'] = '6'
        # get PFN
        iGUID = 0
        nGUID = 5000
        pfnMap   = {}
        listGUID = []
        for guid in files.keys():
            if verbose:
                sys.stdout.write('.')
                sys.stdout.flush()
            iGUID += 1
            listGUID.append(guid)
            if iGUID % nGUID == 0 or iGUID == len(files):
                # get replica
                ret,resList = lfc.lfc_getreplicas(listGUID,'')
                if ret == 0:
                    for fr in resList:
                        if fr != None and ((not hasattr(fr,'errcode')) or \
                                           (hasattr(fr,'errcode') and fr.errcode == 0)):
                            # get host
                            match = re.search('^[^:]+://([^:/]+):*\d*/',fr.sfn)
                            if match==None:
                                continue
                            # check host
                            host = match.group(1)
                            if storages != [] and (not host in storages):
                                continue
                            # append
                            if not pfnMap.has_key(fr.guid):
                                pfnMap[fr.guid] = []
                            pfnMap[fr.guid].append(fr.sfn)
                else:
                    return self.SC_FAILED,"LFC lookup failed with %s" % lfc.sstrerror(lfc.cvar.serrno)
                # reset                        
                listGUID = []
        # collect LFNs
        retLFNs = {}
        for guid,lfn in files.iteritems():
            if guid in pfnMap.keys():
                retLFNs[lfn] = pfnMap[guid]
        # return
        return self.SC_SUCCEEDED,retLFNs



    # get dataset metadata
    def getDatasetMetaData(self,datasetName):
        # make logger
        methodName = 'getDatasetMetaData'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        try:
            # get DQ2 API
            dq2=DQ2()
            # get file list
            tmpRet = dq2.getMetaDataAttribute(datasetName,dq2.listMetaDataAttributes())
            # change dataset state to string
            if tmpRet['state'] in [DatasetState.CLOSED,DatasetState.FROZEN]:
                tmpRet['state'] = 'closed'
            elif tmpRet['state'] == DatasetState.OPEN:
                tmpRet['state'] = 'open'
            else:
                tmpRet['state'] = 'unknown'                
            tmpLog.debug(str(tmpRet))    
            return self.SC_SUCCEEDED,tmpRet
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed with {0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            errCode = self.checkError(errtype)
            return errCode,'{0}.{1} {2}'.format(self.__class__.__name__,methodName,errMsg)



    # check dataset consistency
    def checkDatasetConsistency(self,location,datasetName):
        # make logger
        methodName = 'checkDatasetConsistency'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        try:
            # get DQ2 API
            dq2=DQ2()
            # check
            tmpRet = dq2.checkDatasetConsistency(location,datasetName)
            tmpLog.debug(str(tmpRet))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed with {0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            errCode = self.checkError(errtype)
            return errCode,'{0}.{1} {2}'.format(self.__class__.__name__,methodName,errMsg)
            

        
    # check error
    def checkError(self,errType):
        if errType in self.fatalErrors:
            # fatal error
            return self.SC_FATAL
        else:
            # temprary error
            return self.SC_FAILED



    # list dataset/container
    def listDatasets(self,datasetName):
        methodName = 'listDatasets'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listDatasets(datasetName,onlyNames=True)
            return self.SC_SUCCEEDED,tmpRet.keys()
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)



    # register new dataset/container
    def registerNewDataset(self,datasetName):
        methodName = 'registerNewDataset'
        try:
            # get DQ2 API            
            dq2=DQ2()
            if not datasetName.endswith('/'):
                # register dataset
                dq2.registerNewDataset(datasetName)
            else:
                # register container
                dq2.registerContainer(datasetName)
        except DQContainerExistsException,DQDatasetExistsException: 
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)
        return self.SC_SUCCEEDED,True
            


    # list datasets in container
    def listDatasetsInContainer(self,containerName):
        methodName = 'listDatasetsInContainer'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get list
            dsList = dq2.listDatasetsInContainer(containerName)
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)

        

    # add dataset to container
    def addDatasetsToContainer(self,containerName,datasetNames):
        methodName = 'addDatasetsToContainer'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # add
            dq2.registerDatasetsInContainer(containerName,datasetNames)
            return self.SC_SUCCEEDED,True
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)


        
