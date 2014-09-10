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
    DQDatasetExistsException, \
    DQSubscriptionExistsException, \
    DQFrozenDatasetException
from dq2.container.exceptions import DQContainerExistsException
import dq2.filecatalog
from dq2.common import parse_dn
from dq2.info.client.infoClient import infoClient

try:
    from pyAMI.client import AMIClient
    from pyAMI import query as amiquery
except:
    pass

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
    def getFilesInDataset(self,datasetName,getNumEvents=False,skipDuplicate=True):
        methodName = 'getFilesInDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
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
                # get number of events in each file
                if getNumEvents:
                    try:
                        amiDatasetName = re.sub('(_tid\d+)*(_\d+)*/$','',datasetName)
                        amiclient = AMIClient()
                        for amiItem in amiquery.get_files(amiclient,amiDatasetName):
                            amiGUID = amiItem['fileGUID']
                            if fileMap.has_key(amiGUID):
                                fileMap[amiGUID]['nevents'] = long(amiItem['events'])
                    except:
                        errtype,errvalue = sys.exc_info()[:2]
                        errStr = '{0} AMI failed with {1} {2}'.format(methodName,errtype.__name__,errvalue)
                        tmpLog.warning(errStr)
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
    def getAvailableFiles(self,datasetSpec,siteEndPointMap,siteMapper,ngGroup=[],checkLFC=False,
                          checkCompleteness=True):
        # make logger
        methodName = 'getAvailableFiles'
        methodName += ' <datasetID={0}>'.format(datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start datasetName={0}'.format(datasetSpec.datasetName))
        try:
            # list of NG endpoints
            ngEndPoints = []
            if 1 in ngGroup:
                ngEndPoints += ['_SCRATCHDISK$','_LOCALGROUPDISK$','_LOCALGROUPTAPE$','_USERDISK$',
                               '_DAQ$','_TMPDISK$','_TZERO$','_GRIDFTP$','MOCKTEST$']
            if 2 in ngGroup:
                ngEndPoints += ['_LOCALGROUPTAPE$',
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
                    if not self.checkNGEndPoint(endPoint,ngEndPoints) and \
                            not endPoint in siteAllEndPointsMap[siteName]:
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
            # get files
            tmpStat,tmpOut = self.getFilesInDataset(datasetSpec.datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get file list with {0}'.format(tmpOut))
                raise tmpStat,tmpOut
            totalNumFiles = len(tmpOut)
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
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                tmpLfcSeMap = {}
                tmpStoragePathMap = {}
                tmpSiteSpec = siteMapper.getSite(siteName)
                siteHasCompleteReplica = False
                for tmpEndPoint in allEndPointList:
                    # storage type
                    if TiersOfATLAS.isTapeSite(tmpEndPoint):
                        storageType = 'localtape'
                    else:
                        storageType = 'localdisk'
                    # no scan when site has complete replicas
                    if (datasetReplicaMap.has_key(tmpEndPoint) and datasetReplicaMap[tmpEndPoint][-1]['found'] != None \
                            and datasetReplicaMap[tmpEndPoint][-1]['total'] == datasetReplicaMap[tmpEndPoint][-1]['found'] \
                            and datasetReplicaMap[tmpEndPoint][-1]['total'] >= totalNumFiles) \
                            or not checkCompleteness:
                        completeReplicaMap[tmpEndPoint] = storageType
                        siteHasCompleteReplica = True
                    # no LFC scan for many-time datasets
                    if datasetSpec.isManyTime():
                        continue
                    # get LFC
                    lfc = TiersOfATLAS.getLocalCatalog(tmpEndPoint)
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
                    if not tmpSePath in tmpStoragePathMap:
                        tmpStoragePathMap[tmpSePath] = []
                    tmpStoragePathMap[tmpSePath].append({'siteName':siteName,'storageType':storageType})
                    # add compact path
                    tmpSePathBack = tmpSePath
                    tmpSePath = re.sub('(:\d+)*/srm/[^\?]+\?SFN=','',tmpSePath)
                    if tmpSePathBack != tmpSePath:
                        if not tmpSePath in tmpStoragePathMap:
                            tmpStoragePathMap[tmpSePath] = []
                        tmpStoragePathMap[tmpSePath].append({'siteName':siteName,'storageType':storageType})
                # add to map to trigger LFC scan if complete replica is missing at the site
                if DataServiceUtils.isCachedFile(datasetSpec.datasetName,tmpSiteSpec):
                    pass
                elif not siteHasCompleteReplica or checkLFC:
                    for tmpKey,tmpVal in tmpLfcSeMap.iteritems():
                        if not lfcSeMap.has_key(tmpKey):
                            lfcSeMap[tmpKey] = []
                        lfcSeMap[tmpKey] += tmpVal
                    for tmpKey,tmpVal in tmpStoragePathMap.iteritems():
                        if not tmpKey in storagePathMap:
                            storagePathMap[tmpKey] = []
                        storagePathMap[tmpKey] += tmpVal
            # collect GUIDs and LFNs
            fileMap        = {}
            lfnMap         = {}
            lfnFileSpecMap = {}
            scopeMap       = {}
            for tmpFile in datasetSpec.Files:
                fileMap[tmpFile.GUID] = tmpFile.lfn
                lfnMap[tmpFile.lfn] = tmpFile
                if not tmpFile.lfn in lfnFileSpecMap:
                    lfnFileSpecMap[tmpFile.lfn] = []
                lfnFileSpecMap[tmpFile.lfn].append(tmpFile)
                scopeMap[tmpFile.lfn] = tmpFile.scope
            # get SURLs
            surlMap = {}
            for lfcHost,seList in lfcSeMap.iteritems():
                tmpLog.debug('lookup in LFC:{0} for {1}'.format(lfcHost,str(seList)))               
                tmpStat,tmpRetMap = self.getSURLsFromLFC(fileMap,lfcHost,seList,scopes=scopeMap)
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
                tmpFileSpecList = lfnFileSpecMap[tmpLFN]
                tmpFileSpec = tmpFileSpecList[0]
                # loop over all SURLs
                for tmpSURL in surlMap[tmpLFN]:
                    for tmpSePath in storagePathMap.keys():
                        # check SURL
                        if tmpSURL.startswith(tmpSePath):
                            # add
                            for tmpSiteDict in storagePathMap[tmpSePath]:
                                siteName = tmpSiteDict['siteName']
                                storageType = tmpSiteDict['storageType']
                                if not tmpFileSpec in returnMap[siteName][storageType]:
                                    returnMap[siteName][storageType] += tmpFileSpecList
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
    def getSURLsFromLFC(self,files,lfcHost,storages,verbose=False,scopes={}):
        try:
            # connect
            apiLFC = dq2.filecatalog.create_file_catalog(lfcHost)
            apiLFC.connect()
            # get PFN
            iGUID = 0
            nGUID = 5000
            pfnMap   = {}
            listGUID = {}
            for guid,tmpLFN in files.iteritems():
                if verbose:
                    sys.stdout.write('.')
                    sys.stdout.flush()
                iGUID += 1
                if lfcHost.startswith('rucio') and scopes.has_key(tmpLFN):
                    listGUID[guid] = scopes[tmpLFN]+':'+tmpLFN
                else:
                    listGUID[guid] = tmpLFN
                if iGUID % nGUID == 0 or iGUID == len(files):
                    # get replica
                    resReplicas = apiLFC.bulkFindReplicas(listGUID)
                    for retGUID,resValMap in resReplicas.iteritems():
                        for retSURL in resValMap['surls']:
                            # get host
                            match = re.search('^[^:]+://([^:/]+):*\d*/',retSURL)
                            if match==None:
                                continue
                            # check host
                            host = match.group(1)
                            if storages != [] and (not host in storages):
                                continue
                            # append
                            if not pfnMap.has_key(retGUID):
                                pfnMap[retGUID] = []
                            pfnMap[retGUID].append(retSURL)
                    # reset
                    listGUID = {}
            # disconnect
            apiLFC.disconnect()
        except:
            errType,errValue = sys.exc_info()[:2]
            return self.SC_FAILED,"LFC lookup failed with {0}:{1}".format(errType,errValue)
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
    def listDatasets(self,datasetName,ignorePandaDS=True):
        methodName = 'listDatasets'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listDatasets2({'name':datasetName},long=False,all=False)
            dsList = tmpRet.keys()
            if ignorePandaDS:
                tmpDsList = []
                for tmpDS in dsList:
                    if re.search('_dis\d+$',tmpDS) != None or re.search('_sub\d+$',tmpDS):
                        continue
                    tmpDsList.append(tmpDS)
                dsList = tmpDsList
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)



    # register new dataset/container
    def registerNewDataset(self,datasetName,backEnd=None,location=None):
        methodName = 'registerNewDataset'
        try:
            # get DQ2 API
            if backEnd == None:
                dq2 = DQ2()
            else:
                dq2 = DQ2(force_backend=backEnd)
            if not datasetName.endswith('/'):
                # register dataset
                if location == None or backEnd == None:
                    dq2.registerNewDataset(datasetName)
                else:
                    dq2.registerNewDataset(datasetName,rse=location)
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

        

    # expand Container
    def expandContainer(self,containerName):
        methodName = 'expandContainer'
        methodName = '{0} contName={1}'.format(methodName,containerName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            dsList = []
            # get real names
            tmpS,tmpRealNameList = self.listDatasets(containerName)
            if tmpS != self.SC_SUCCEEDED:
                tmpLog.error('failed to get real names')
                return tmpS,tmpRealNameList
            # loop over all names
            for tmpRealName in tmpRealNameList:
                # container
                if tmpRealName.endswith('/'):
                    # get contents
                    tmpS,tmpO = self.listDatasetsInContainer(tmpRealName)
                    if tmpS != self.SC_SUCCEEDED:
                        tmpLog.error('failed to get datasets in {0}'.format(tmpRealName))
                        return tmpS,tmpO
                else:
                    tmpO = [tmpRealName]
                # collect dataset names
                for tmpStr in tmpO:
                    if not tmpStr in dsList:
                        dsList.append(tmpStr)
            dsList.sort()        
            # return
            tmpLog.info('got {0}'.format(str(dsList)))
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error('failed with {0}'.format(errMsg))
            return errCode,'{0} : {1}'.format(methodName,errMsg)

        

    # add dataset to container
    def addDatasetsToContainer(self,containerName,datasetNames,backEnd=None):
        methodName = 'addDatasetsToContainer'
        try:
            # get DQ2 API
            if backEnd == None:
                dq2 = DQ2()
            else:
                dq2 = DQ2(force_backend=backEnd)
            # add
            dq2.registerDatasetsInContainer(containerName,datasetNames)
            return self.SC_SUCCEEDED,True
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'{0} : {1} {2}'.format(methodName,errtype.__name__,errvalue)



    # get latest DBRelease
    def getLatestDBRelease(self):
        methodName = 'getLatestDBRelease'
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('trying to get the latest version number of DBR')
        # get ddo datasets
        tmpStat,ddoDatasets = self.listDatasets('ddo.*')
        if tmpStat != self.SC_SUCCEEDED or ddoDatasets == {}:
            tmpLog.error('failed to get a list of DBRelease datasets from DQ2')
            return self.SC_FAILED,None
        # reverse sort to avoid redundant lookup   
        ddoDatasets.sort()
        ddoDatasets.reverse()
        # extract version number
        latestVerMajor = 0
        latestVerMinor = 0
        latestVerBuild = 0
        latestVerRev   = 0
        latestDBR = ''
        for tmpName in ddoDatasets:
            # ignore CDRelease
            if ".CDRelease." in tmpName:
                continue
            # ignore user
            if tmpName.startswith('ddo.user'):
                continue
            # use Atlas.Ideal
            if not ".Atlas.Ideal." in tmpName:
                continue
            match = re.search('\.v(\d+)(_*[^\.]*)$',tmpName)
            if match == None:
                tmpLog.warning('cannot extract version number from %s' % tmpName)
                continue
            # ignore special DBRs
            if match.group(2) != '':
                continue
            # get major,minor,build,revision numbers
            tmpVerStr = match.group(1)
            tmpVerMajor = 0
            tmpVerMinor = 0
            tmpVerBuild = 0
            tmpVerRev   = 0
            try:
                tmpVerMajor = int(tmpVerStr[0:2])
            except:
                pass
            try:
                tmpVerMinor = int(tmpVerStr[2:4])
            except:
                pass
            try:
                tmpVerBuild = int(tmpVerStr[4:6])
            except:
                pass
            try:
                tmpVerRev = int(tmpVerStr[6:])
                # use only three digit DBR
                continue
            except:
                pass
            # compare
            if latestVerMajor > tmpVerMajor:
                continue
            elif latestVerMajor == tmpVerMajor:
                if latestVerMinor > tmpVerMinor:
                    continue
                elif latestVerMinor == tmpVerMinor:
                    if latestVerBuild > tmpVerBuild:
                        continue
                    elif latestVerBuild == tmpVerBuild:
                        if latestVerRev > tmpVerRev:
                            continue
            # check if well replicated
            tmpStat,ddoReplicas = self.listDatasetReplicas(tmpName)
            if len(ddoReplicas) < 10:
                continue
            # higher or equal version
            latestVerMajor = tmpVerMajor
            latestVerMinor = tmpVerMinor
            latestVerBuild = tmpVerBuild
            latestVerRev   = tmpVerRev
            latestDBR = tmpName
        # failed
        if latestDBR == '':
            tmpLog.error('failed to get the latest version of DBRelease dataset from DQ2')
            return self.SC_FAILED,None
        tmpLog.info('use {0}'.format(latestDBR))
        return self.SC_SUCCEEDED,latestDBR



    # freeze dataset
    def freezeDataset(self,datasetName,ignoreUnknown=False):
        methodName = 'freezeDataset'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        isOK = True
        try:
            # get DQ2 API            
            dq2=DQ2()
            # freeze
            dq2.freezeDataset(datasetName)
        except DQFrozenDatasetException:
            pass
        except DQUnknownDatasetException:
            if ignoreUnknown:
                pass
            else:
                isOK = False
        except:
            isOK = False
        if isOK:
            tmpLog.info('done')
            return self.SC_SUCCEEDED,True
        else:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)



    # finger
    def finger(self,userName):
        methodName = 'finger'
        methodName = '{0} userName={1}'.format(methodName,userName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            # cleanup DN
            userName = parse_dn(userName)
            # exec
            tmpRet = infoClient().finger(userName)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0}:{1}'.format(methodName,errMsg)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,tmpRet



    # set dataset ownership
    def setDatasetOwner(self,datasetName,userName):
        methodName = 'setDatasetOwner'
        methodName = '{0} datasetName={1} userName={2}'.format(methodName,datasetName,userName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            # cleanup DN
            userName = parse_dn(userName)
            # get DQ2 API            
            dq2=DQ2()
            # set
            dq2.setMetaDataAttribute(datasetName,'owner',userName)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,True



    # set dataset metadata
    def setDatasetMetadata(self,datasetName,metadataName,metadaValue):
        methodName = 'setDatasetMetadata'
        methodName = '{0} datasetName={1} metadataName={2} metadaValue={3}'.format(methodName,datasetName,
                                                                                   metadataName,metadaValue)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            # get DQ2 API            
            dq2 = DQ2()
            # set
            dq2.setMetaDataAttribute(datasetName,metadataName,metadaValue)
        except DQUnknownDatasetException:
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,True



    # register location
    def registerDatasetLocation(self,datasetName,location,lifetime=None,owner=None,backEnd=None):
        methodName = 'registerDatasetLocation'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            # cleanup DN
            owner = parse_dn(owner)
            # get DQ2 API
            if backEnd == None:
                dq2 = DQ2()
            else:
                dq2 = DQ2(force_backend=backEnd)
            # set
            dq2.registerDatasetLocation(datasetName,location,lifetime=lifetime)
            dq2.setReplicaMetaDataAttribute(datasetName,location,'owner',owner)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,True



    # delete dataset
    def deleteDataset(self,datasetName,emptyOnly,ignoreUnknown=False):
        methodName = 'deleteDataset'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        isOK = True
        retStr = ''
        nFiles = -1
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get the number of files
            if emptyOnly:
                nFiles = dq2.getNumberOfFiles(datasetName)
            # erase
            if not emptyOnly or nFiles == 0:
                dq2.eraseDataset(datasetName)
                retStr = 'deleted {0}'.format(datasetName)
            else:
                retStr = 'keep {0} where {1} files are available'.format(datasetName,nFiles)
        except DQUnknownDatasetException:
            if ignoreUnknown:
                pass
            else:
                isOK = False
        except:
            isOK = False
        if isOK:
            tmpLog.info('done')
            return self.SC_SUCCEEDED,retStr
        else:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)



    # register subscription
    def registerDatasetSubscription(self,datasetName,location,activity=None,ignoreUnknown=False):
        methodName = 'registerDatasetSubscription'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        isOK = True
        try:
            # get DQ2 API            
            dq2 = DQ2()
            # call
            dq2.registerDatasetSubscription(datasetName,location,activity=activity)
        except DQSubscriptionExistsException:
            pass
        except DQUnknownDatasetException:
            if ignoreUnknown:
                pass
            else:
                isOK = False
        except:
            isOK = False
        if not isOK:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,True



    # find lost files
    def findLostFiles(self,datasetName,fileMap):
        methodName = 'findLostFiles'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.info('start')
        try:
            # get replicas
            tmpStat,tmpOut = self.listDatasetReplicas(datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get dataset replicas with {0}'.format(tmpOut))
                raise tmpStat,tmpOut
            # check if complete replica is available
            hasCompReplica = False
            datasetReplicaMap = tmpOut
            for tmpEndPoint in datasetReplicaMap.keys():
                if datasetReplicaMap[tmpEndPoint][-1]['found'] != None and \
                        datasetReplicaMap[tmpEndPoint][-1]['total'] == datasetReplicaMap[tmpEndPoint][-1]['found']:
                    hasCompReplica = True
                    break
            # no lost files
            if hasCompReplica:
                tmpLog.info('done with no lost files')
                return self.SC_SUCCEEDED,{}
            # get LFNs and scopes
            lfnMap = {}
            scopeMap = {}
            for tmpGUID in fileMap.keys():
                tmpLFN = fileMap[tmpGUID]['lfn']
                lfnMap[tmpGUID] = tmpLFN
                scopeMap[tmpLFN] = fileMap[tmpGUID]['scope']
            # get LFC and SE
            lfcSeMap = {}
            for tmpEndPoint in datasetReplicaMap.keys():
                # get LFC
                lfc = TiersOfATLAS.getLocalCatalog(tmpEndPoint)
                # add map
                if not lfcSeMap.has_key(lfc):
                    lfcSeMap[lfc] = []
                # get SE
                seStr = TiersOfATLAS.getSiteProperty(tmpEndPoint, 'srm')
                tmpMatch = re.search('://([^:/]+):*\d*/',seStr)
                if tmpMatch != None:
                    se = tmpMatch.group(1)
                    if not se in lfcSeMap[lfc]:
                        lfcSeMap[lfc].append(se)
            # get SURLs
            for lfcHost,seList in lfcSeMap.iteritems():
                tmpStat,tmpRetMap = self.getSURLsFromLFC(lfnMap,lfcHost,seList,scopes=scopeMap)
                if tmpStat != self.SC_SUCCEEDED:
                    tmpLog.error('faild to get SURLs with {0}'.format(tmpRetMap))
                    raise tmpStat,tmpRetMap
                # look for missing files
                newLfnMap = {}
                for tmpGUID,tmpLFN in lfnMap.iteritems():
                    if not tmpLFN in tmpRetMap:
                        newLfnMap[tmpGUID] = tmpLFN
                lfnMap = newLfnMap
            tmpLog.info('done with lost '+','.join(str(tmpLFN) for tmpLFN in lfnMap.values()))
            return self.SC_SUCCEEDED,lfnMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
    
