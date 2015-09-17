import re
import os
import sys
import lfc
import socket
import random
import datetime
import json
import urllib2
import traceback

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
from dq2.container.exceptions import DQContainerExistsException,\
    DQContainerAlreadyHasDataset
import dq2.filecatalog
from dq2.common import parse_dn
from dq2.info.client.infoClient import infoClient

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,DataIdentifierAlreadyExists,\
    DuplicateRule

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
        # list of blacklisted endpoints
        self.blackListEndPoints = []
        # time of last update for blacklist
        self.lastUpdateBL = None
        # how frequently update DN/token map
        self.timeIntervalBL = datetime.timedelta(seconds=60*10)
        # dict of endpoints
        self.endPointDict = {}
        # time of last update for endpoint dict
        self.lastUpdateEP = None
        # how frequently update endpoint dict
        self.timeIntervalEP = datetime.timedelta(seconds=60*10)




    # get files in dataset
    def getFilesInDataset(self,datasetName,getNumEvents=False,skipDuplicate=True,ignoreUnknown=False,longFormat=False):
        methodName = 'getFilesInDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listFilesInDataset(datasetName,long=longFormat)
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
            tmpLog.debug('done')
            return self.SC_SUCCEEDED,fileMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errStr = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            if ignoreUnknown and errtype == DQUnknownDatasetException:
                return self.SC_SUCCEEDED,{}
            return errCode,'{0} : {1}'.format(methodName,errStr)



    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        methodName = 'listDatasetReplicas'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get DQ2 API            
            dq2=DQ2()
            if not datasetName.endswith('/'):
                # get file list
                tmpRet = self.convertOutListDatasetReplicas(datasetName)
                tmpLog.debug('got new '+str(tmpRet))
                return self.SC_SUCCEEDED,tmpRet
            else:
                # list of attributes summed up
                attrList = ['total','found']
                retMap = {}
                # get constituent datasets
                dsList = dq2.listDatasetsInContainer(datasetName)
                for tmpName in dsList:
                    tmpLog.debug(tmpName)
                    tmpRet = self.convertOutListDatasetReplicas(tmpName)
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
                tmpLog.debug('got '+str(retMap))
                return self.SC_SUCCEEDED,retMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errStr = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            return errCode,'{0} : {1}'.format(methodName,errStr)



    # get site property
    def getSiteProperty(self,seName,attribute):
        methodName = 'getSiteProperty'
        try:
            return self.SC_SUCCEEDED,TiersOfATLAS.getSiteProperty(seName,attribute)
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)



    # get site alternateName
    def getSiteAlternateName(self,seName):
        self.updateEndPointDict()
        if seName in self.endPointDict:
            return [self.endPointDict[seName]['site']]
        return None



    # get associated endpoints
    def getAssociatedEndpoints(self,altName):
        self.updateEndPointDict()
        epList = []
        for seName,seVal in self.endPointDict.iteritems():
            if seVal['site'] == altName:
                epList.append(seName)
        return epList



    # convert token to endpoint
    def convertTokenToEndpoint(self,baseSeName,token):
        self.updateEndPointDict()
        altName = self.getSiteAlternateName(baseSeName)
        if altName != None:
            for seName,seVal in self.endPointDict.iteritems():
                if seVal['site'] == altName:
                    # space token
                    if seVal['token'] == token:
                        return seName
                    # pattern matching
                    if re.search(token,seName) != None:
                        return seName
        return None



    # get cloud for an endpoint
    def getCloudForEndPoint(self,endPoint):
        self.updateEndPointDict()
        if endPoint in self.endPointDict:
            return self.endPointDict[endPoint]['cloud']
        return None



    # check if endpoint is NG
    def checkNGEndPoint(self,endPoint,ngList):
        for ngPatt in ngList:
            if re.search(ngPatt,endPoint) != None:
                return True
        return False    
        

                        
    # get available files
    def getAvailableFiles(self,datasetSpec,siteEndPointMap,siteMapper,ngGroup=[],checkLFC=False,
                          checkCompleteness=True,storageToken=None,useCompleteOnly=False):
        # make logger
        methodName = 'getAvailableFiles'
        methodName += ' <datasetID={0}>'.format(datasetSpec.datasetID)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start datasetName={0} checkCompleteness={1}'.format(datasetSpec.datasetName,
                                                                          checkCompleteness))
        try:
            # list of NG endpoints
            ngEndPoints = []
            if 1 in ngGroup:
                ngEndPoints += ['_SCRATCHDISK$','_LOCALGROUPDISK$','_LOCALGROUPTAPE$','_USERDISK$',
                               '_DAQ$','_TMPDISK$','_TZERO$','_GRIDFTP$','MOCKTEST$']
            if 2 in ngGroup:
                ngEndPoints += ['_LOCALGROUPTAPE$',
                               '_DAQ$','_TMPDISK$','_GRIDFTP$','MOCKTEST$']
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
                    altName = self.getSiteAlternateName(endPoint)
                    if altName != None and altName != ['']:
                        for assEndPoint in self.getAssociatedEndpoints(altName[0]):
                            if not assEndPoint in siteAllEndPointsMap[siteName] and \
                                   not self.checkNGEndPoint(assEndPoint,ngEndPoints):
                                siteAllEndPointsMap[siteName].append(assEndPoint)
            # get files
            tmpStat,tmpOut = self.getFilesInDataset(datasetSpec.datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get file list with {0}'.format(tmpOut))
                return tmpStat,tmpOut
            totalNumFiles = len(tmpOut)
            # get replica map
            tmpStat,tmpOut = self.listDatasetReplicas(datasetSpec.datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get dataset replicas with {0}'.format(tmpOut))
                return tmpStat,tmpOut
            datasetReplicaMap = tmpOut
            # collect SE, LFC hosts, storage path, storage type
            lfcSeMap = {}
            storagePathMap = {}
            completeReplicaMap = {}
            cloudLocCheckDst = {}
            cloudLocCheckSrc = {}
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                tmpLfcSeMap = {}
                tmpStoragePathMap = {}
                tmpSiteSpec = siteMapper.getSite(siteName)
                siteHasCompleteReplica = False
                # cloud locality check 
                if tmpSiteSpec.hasValueInCatchall('cloudLocCheck'):
                    tmpCheckCloud = tmpSiteSpec.cloud
                    if not tmpCheckCloud in cloudLocCheckDst:
                        cloudLocCheckDst[tmpCheckCloud] = set()
                    cloudLocCheckDst[tmpCheckCloud].add(siteName) 
                # loop over all endpoints
                for tmpEndPoint in allEndPointList:
                    # cloud locality check
                    tmpCheckCloud = self.getCloudForEndPoint(tmpEndPoint)
                    if not siteName in cloudLocCheckSrc:
                        cloudLocCheckSrc[siteName] = set()
                    cloudLocCheckSrc[siteName].add(tmpCheckCloud)
                    # storage type
                    if TiersOfATLAS.isTapeSite(tmpEndPoint):
                        storageType = 'localtape'
                    else:
                        storageType = 'localdisk'
                    # no scan when site has complete replicas
                    if (datasetReplicaMap.has_key(tmpEndPoint) and datasetReplicaMap[tmpEndPoint][-1]['found'] != None \
                            and datasetReplicaMap[tmpEndPoint][-1]['total'] == datasetReplicaMap[tmpEndPoint][-1]['found'] \
                            and datasetReplicaMap[tmpEndPoint][-1]['total'] >= totalNumFiles) \
                            or (not checkCompleteness and datasetReplicaMap.has_key(tmpEndPoint)) \
                            or DataServiceUtils.isCachedFile(datasetSpec.datasetName,tmpSiteSpec):
                        completeReplicaMap[tmpEndPoint] = storageType
                        siteHasCompleteReplica = True
                    # no LFC scan for many-time datasets or disabled completeness check
                    if datasetSpec.isManyTime() or (not checkCompleteness and not datasetReplicaMap.has_key(tmpEndPoint)) \
                            or useCompleteOnly:
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
                    # check token
                    if not storageToken in [None,'','NULL']:
                        try:
                            if seStr.split(':')[1] != storageToken:
                                continue
                        except:
                            pass
                    # add full path to storage map
                    tmpSePath = tmpMatch.group(1)
                    if not tmpSePath in tmpStoragePathMap:
                        tmpStoragePathMap[tmpSePath] = []
                    tmpStoragePathMap[tmpSePath].append({'siteName':siteName,'storageType':storageType,'endPoint':tmpEndPoint})
                    # add compact path
                    tmpSePathBack = tmpSePath
                    tmpSePath = re.sub('(:\d+)*/srm/[^\?]+\?SFN=','',tmpSePath)
                    if tmpSePathBack != tmpSePath:
                        if not tmpSePath in tmpStoragePathMap:
                            tmpStoragePathMap[tmpSePath] = []
                        tmpStoragePathMap[tmpSePath].append({'siteName':siteName,'storageType':storageType,'endPoint':tmpEndPoint})
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
            checkedDst = set()
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
                            checkedDst.add(siteName)
                            # add for cloud locality check
                            if siteName in cloudLocCheckSrc:
                                # loop over possible endpoint clouds associated to the site
                                for tmpCheckCloud in cloudLocCheckSrc[siteName]:
                                    # use only cloud matching to the endpoint
                                    if tmpCheckCloud == self.getCloudForEndPoint(tmpEndPoint):
                                        dstSiteNameList = set()
                                        # sites using cloud locality check
                                        if tmpCheckCloud in cloudLocCheckDst:
                                            dstSiteNameList = dstSiteNameList.union(cloudLocCheckDst[tmpCheckCloud])
                                        # skip if no sites
                                        if len(dstSiteNameList) == 0:
                                            continue
                                        for dstSiteName in dstSiteNameList:
                                            if not dstSiteName in checkedDst:
                                                returnMap[dstSiteName][storageType] += datasetSpec.Files
                                                checkedDst.add(dstSiteName)
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
                                tmpEndPoint = tmpSiteDict['endPoint']
                                if not tmpFileSpec in returnMap[siteName][storageType]:
                                    returnMap[siteName][storageType] += tmpFileSpecList
                                checkedDst.add(siteName)
                                # add for cloud locality check
                                if siteName in cloudLocCheckSrc:
                                    # loop over possible endpoint clouds associated to the site
                                    for tmpCheckCloud in cloudLocCheckSrc[siteName]:
                                        # use only cloud matching to the endpoint
                                        if tmpCheckCloud == self.getCloudForEndPoint(tmpEndPoint):
                                            dstSiteNameList = set()
                                            # sites using cloud locality check
                                            if tmpCheckCloud in cloudLocCheckDst:
                                                dstSiteNameList = dstSiteNameList.union(cloudLocCheckDst[tmpCheckCloud])
                                            # skip if no sites
                                            if len(dstSiteNameList) == 0:
                                                continue
                                            for dstSiteName in dstSiteNameList:
                                                if not dstSiteName in checkedDst:
                                                    if not tmpFileSpec in returnMap[dstSiteName][storageType]:
                                                        returnMap[dstSiteName][storageType] += tmpFileSpecList
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
            tmpLog.debug('done')            
            return self.SC_SUCCEEDED,returnMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed with {0} {1} '.format(errtype.__name__,errvalue)
            errMsg += traceback.format_exc()
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
            if guid in pfnMap:
                retLFNs[lfn] = pfnMap[guid]
        # return
        return self.SC_SUCCEEDED,retLFNs



    # get dataset metadata
    def getDatasetMetaData(self,datasetName):
        # make logger
        methodName = 'getDatasetMetaData'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # get
            tmpRet = client.get_metadata(scope,dsn)
            # set state
            if tmpRet['is_open'] == True and tmpRet['did_type'] != 'CONTAINER':
                tmpRet['state'] = 'open'
            else:
                tmpRet['state'] = 'closed'
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
        tmpLog.debug('start')
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
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
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
            tmpLog.debug('got '+str(dsList))
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            if 'DQContainerUnknownException' in str(errvalue):
                dsList = []
                tmpLog.debug('got '+str(dsList))
                return self.SC_SUCCEEDED,dsList
            errCode = self.checkError(errtype)
            errStr = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            return errCode,'{0} : {1}'.format(methodName,errStr)



    # register new dataset/container
    def registerNewDataset(self,datasetName,backEnd='rucio',location=None,lifetime=None,metaData=None):
        methodName = 'registerNewDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start location={0} lifetime={1}'.format(location,lifetime))
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # lifetime
            if lifetime != None:
                lifetime=lifetime*86400
            # register
            if not datasetName.endswith('/'):
                # register dataset
                client.add_dataset(scope,dsn,meta=metaData,lifetime=lifetime,rse=location)
            else:
                # register container
                client.add_container(scope=scope,name=dsn[:-1])
        except (DQContainerExistsException,
                DQDatasetExistsException,
                DataIdentifierAlreadyExists): 
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            if not 'DataIdentifierAlreadyExists' in str(errvalue):
                errCode = self.checkError(errtype)
                errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
                tmpLog.error(errMsg)
                return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True
            


    # list datasets in container
    def listDatasetsInContainer(self,containerName):
        methodName = 'listDatasetsInContainer'
        methodName += ' <containerName={0}>'.format(containerName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get list
            dsList = dq2.listDatasetsInContainer(containerName)
            tmpLog.debug('got '+str(dsList))
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)

        

    # expand Container
    def expandContainer(self,containerName):
        methodName = 'expandContainer'
        methodName += ' <contName={0}>'.format(containerName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
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
            tmpLog.debug('got {0}'.format(str(dsList)))
            return self.SC_SUCCEEDED,dsList
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error('failed with {0}'.format(errMsg))
            return errCode,'{0} : {1}'.format(methodName,errMsg)

        

    # add dataset to container
    def addDatasetsToContainer(self,containerName,datasetNames,backEnd='rucio'):
        methodName = 'addDatasetsToContainer'
        methodName += ' <contName={0}>'.format(containerName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get DQ2 API
            if backEnd == None:
                dq2 = DQ2()
            else:
                dq2 = DQ2(force_backend=backEnd)
            # add
            dq2.registerDatasetsInContainer(containerName,datasetNames)
        except DQContainerAlreadyHasDataset:
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error('failed with {0}'.format(errMsg))
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # get latest DBRelease
    def getLatestDBRelease(self):
        methodName = 'getLatestDBRelease'
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('trying to get the latest version number of DBR')
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
            if ddoReplicas == []:
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
        tmpLog.debug('use {0}'.format(latestDBR))
        return self.SC_SUCCEEDED,latestDBR



    # freeze dataset
    def freezeDataset(self,datasetName,ignoreUnknown=False):
        methodName = 'freezeDataset'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
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
            tmpLog.debug('done')
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
        tmpLog.debug('start')
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
        tmpLog.debug('done with '+str(tmpRet))
        return self.SC_SUCCEEDED,tmpRet



    # set dataset ownership
    def setDatasetOwner(self,datasetName,userName,backEnd='rucio'):
        methodName = 'setDatasetOwner'
        methodName = '{0} datasetName={1} userName={2}'.format(methodName,datasetName,userName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            if backEnd == 'dq2':
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
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # set dataset metadata
    def setDatasetMetadata(self,datasetName,metadataName,metadaValue):
        methodName = 'setDatasetMetadata'
        methodName = '{0} datasetName={1} metadataName={2} metadaValue={3}'.format(methodName,datasetName,
                                                                                   metadataName,metadaValue)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # set
            client.set_metadata(scope,dsn,metadataName,metadaValue)
        except (UnsupportedOperation,DataIdentifierNotFound):
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # register location
    def registerDatasetLocation(self,datasetName,location,lifetime=None,owner=None,backEnd='rucio',
                                activity=None):
        methodName = 'registerDatasetLocation'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get rucio API
            client = RucioClient()
            # cleanup DN
            owner = parse_dn(owner)
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # lifetime
            if lifetime != None:
                lifetime = lifetime * 86400
            elif 'SCRATCHDISK' in location:
                lifetime = 14 * 86400
            # get owner
            if owner != None:
                tmpStat,userInfo = self.finger(owner)
                if tmpStat != self.SC_SUCCEEDED:
                    raise RuntimeError,'failed to get nickname for {0}'.format(owner)
                owner = userInfo['nickname']
            else:
                owner = client.account
            # add rule
            dids = []
            did = {'scope': scope, 'name': dsn}
            dids.append(did)
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,lifetime=lifetime,
                                        grouping='DATASET',account=owner,locked=False,notify='N',
                                        ignore_availability=True,activity=activity)
        except DuplicateRule:
            pass
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # delete dataset
    def deleteDataset(self,datasetName,emptyOnly,ignoreUnknown=False):
        methodName = 'deleteDataset'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
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
            tmpLog.debug('done')
            return self.SC_SUCCEEDED,retStr
        else:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)



    # register subscription
    def registerDatasetSubscription(self,datasetName,location,activity=None,ignoreUnknown=False,
                                    backEnd='rucio'):
        methodName = 'registerDatasetSubscription'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isOK = True
        try:
            # get DQ2 API
            if backEnd == None:
                dq2 = DQ2()
            else:
                dq2 = DQ2(force_backend=backEnd)
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
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # find lost files
    def findLostFiles(self,datasetName,fileMap):
        methodName = 'findLostFiles'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get replicas
            tmpStat,tmpOut = self.listDatasetReplicas(datasetName)
            if tmpStat != self.SC_SUCCEEDED:
                tmpLog.error('faild to get dataset replicas with {0}'.format(tmpOut))
                return tmpStat,tmpOut
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
                tmpLog.debug('done with no lost files')
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
                if seStr != None:
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
                    return tmpStat,tmpRetMap
                # look for missing files
                newLfnMap = {}
                for tmpGUID,tmpLFN in lfnMap.iteritems():
                    if not tmpLFN in tmpRetMap:
                        newLfnMap[tmpGUID] = tmpLFN
                lfnMap = newLfnMap
            tmpLog.debug('done with lost '+','.join(str(tmpLFN) for tmpLFN in lfnMap.values()))
            return self.SC_SUCCEEDED,lfnMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)



    # get sites associated to a DDM endpoint
    def getSitesWithEndPoint(self,endPoint,siteMapper,siteType):
        retList = []
        # get alternate name                                                                                                          
        altNameList = self.getSiteAlternateName(endPoint)
        if altNameList != None and altNameList != [''] and len(altNameList) > 0:
            altName = altNameList[0]
            # loop over all sites
            for tmpSiteName,tmpSiteSpec in siteMapper.siteSpecList.iteritems():
                # check type
                if tmpSiteSpec.type != siteType:
                    continue
                # check status
                if tmpSiteSpec.status == 'offline':
                    continue
                # end point
                tmpAltNameList = self.getSiteAlternateName(tmpSiteSpec.ddm)
                if tmpAltNameList == None or tmpAltNameList == [''] or len(tmpAltNameList) == 0:
                    continue
                if altName != tmpAltNameList[0]:
                    continue
                # append
                if not tmpSiteName in retList:
                    retList.append(tmpSiteName)
        # return
        return retList



    # convert output of listDatasetReplicas
    def convertOutListDatasetReplicas(self,datasetName):
        retMap = {}
        # get rucio API
        client = RucioClient()
        # get scope and name
        scope,dsn = self.extract_scope(datasetName)
        itr = client.list_dataset_replicas(scope,dsn)
        for item in itr:
            rse = item["rse"]
            retMap[rse] = [{'total':item["length"],
                            'found':item["available_length"],
                            'immutable':1}]
        return retMap



    # delete files from dataset
    def deleteFilesFromDataset(self,datasetName,filesToDelete):
        methodName  = 'deleteFilesFromDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # open dataset
            try:
                client.set_status(scope,dsn,open=True)
            except UnsupportedOperation:
                pass
            # exec
            client.detach_dids(scope=scope, name=dsn, dids=filesToDelete)
        except:
            isOK = False
        if not isOK:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # extract scope
    def extract_scope(self,dsn):
        if ':' in dsn:
            return dsn.split(':')[:2]
        scope = dsn.split('.')[0]
        if dsn.startswith('user') or dsn.startswith('group'):
            scope = ".".join(dsn.split('.')[0:2])
        return scope,dsn



    # open dataset
    def openDataset(self,datasetName):
        methodName  = 'openDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # open dataset
            try:
                client.set_status(scope,dsn,open=True)
            except (UnsupportedOperation,DataIdentifierNotFound):
                pass
        except:
            isOK = False
        if not isOK:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,True



    # update backlist
    def updateBlackList(self):
        methodName  = 'updateBlackList'
        tmpLog = MsgWrapper(logger,methodName)
        # check freashness
        timeNow = datetime.datetime.utcnow()
        if self.lastUpdateBL != None and timeNow-self.lastUpdateBL < self.timeIntervalBL:
            return
        self.lastUpdateBL = timeNow
        # get json
        try:
            tmpLog.debug('start')
            res = urllib2.urlopen('http://atlas-agis-api.cern.ch/request/ddmendpointstatus/query/list/?json&fstate=OFF&activity=w')
            jsonStr = res.read()
            self.blackListEndPoints = json.loads(jsonStr).keys()
            tmpLog.debug('{0} endpoints blacklisted'.format(len(self.blackListEndPoints)))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'failed to update BL with {0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
        return



    # check if the endpoint is backlisted
    def isBlackListedEP(self,endPoint):
        methodName  = 'isBlackListedEP'
        methodName += ' <endPoint={0}>'.format(endPoint)
        tmpLog = MsgWrapper(logger,methodName)
        try:
            # update BL
            self.updateBlackList()
            if endPoint in self.blackListEndPoints:
                return self.SC_SUCCEEDED,True
        except:
            pass
        return self.SC_SUCCEEDED,False



    # get disk usage at RSE
    def getRseUsage(self,rse,src='srm'):
        methodName  = 'getRseUsage'
        methodName += ' <rse={0}>'.format(rse)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        retMap = {}
        try:
            # get rucio API
            client = RucioClient()
            # get info
            itr = client.get_rse_usage(rse)
            # look for srm
            for item in itr:
                if item['source'] == src:
                    try:
                        total = item['total']/1024/1024/1024
                    except:
                        total = None
                    try:
                        used = item['used']/1024/1024/1024
                    except:
                        used = None
                    try:
                        free = item['free']/1024/1024/1024
                    except:
                        free = None
                    retMap = {'total':total,
                              'used':used,
                              'free':free}
                    break
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,{}
        tmpLog.debug('done {0}'.format(str(retMap)))
        return self.SC_SUCCEEDED,retMap



    # update endpoint dict
    def updateEndPointDict(self):
        methodName  = 'updateEndPointDict'
        tmpLog = MsgWrapper(logger,methodName)
        # check freashness
        timeNow = datetime.datetime.utcnow()
        if self.lastUpdateEP != None and timeNow-self.lastUpdateEP < self.timeIntervalEP:
            return
        self.lastUpdateEP = timeNow
        # get json
        try:
            tmpLog.debug('start')
            jsonStr = ''
            res = urllib2.urlopen('http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&state=ACTIVE')
            jsonStr = res.read()
            tmpList= json.loads(jsonStr)
            endPointDict = {}
            for tmpItem in tmpList:
                endPointDict[tmpItem['name']] = tmpItem
            self.endPointDict = endPointDict
            tmpLog.debug('got {0} endpoints '.format(len(self.endPointDict)))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'failed to update EP with {0} {1} jsonStr={2}'.format(errtype.__name__,
                                                                           errvalue,
                                                                           jsonStr)
            tmpLog.error(errStr)
        return
