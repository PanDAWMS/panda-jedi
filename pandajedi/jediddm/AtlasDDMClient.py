import re
import os
import sys
import lfc
import socket
import random
import datetime
import json
import types
import urllib2
import traceback

from pandajedi.jedicore.MsgWrapper import MsgWrapper

from DDMClientBase import DDMClientBase

from rucio.client import Client as RucioClient
from rucio.common.exception import UnsupportedOperation,DataIdentifierNotFound,DataIdentifierAlreadyExists,\
    DuplicateRule,DuplicateContent

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
        self.fatalErrors = []
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
        # NG endpoint types
        self.ngEndPointTypes = ['TEST','SPECIAL']



    # get a parsed certificate DN
    def parse_dn(self,tmpDN):
        if tmpDN is not None:
            tmpDN = re.sub('/CN=limited proxy','',tmpDN)
            tmpDN = re.sub('(/CN=proxy)+$', '', tmpDN)
            #tmpDN = re.sub('(/CN=\d+)+$', '', tmpDN)
        return tmpDN
        


    # get files in dataset
    def getFilesInDataset(self,datasetName,getNumEvents=False,skipDuplicate=True,ignoreUnknown=False,longFormat=False):
        methodName = 'getFilesInDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get Rucio API            
            client = RucioClient()
            # extract scope from dataset
            scope,dsn = self.extract_scope(datasetName)
            # get files
            fileMap = {}
            baseLFNmap = {}
            for x in client.list_files(scope, dsn, long=longFormat):
                # convert to old dict format
                lfn = str(x['name'])
                attrs = {}
                attrs['lfn'] = lfn
                attrs['chksum'] = "ad:" + str(x['adler32'])
                attrs['md5sum'] = attrs['chksum']
                attrs['checksum'] = attrs['chksum']
                attrs['fsize'] = x['bytes']
                attrs['filesize'] = attrs['fsize']
                attrs['scope'] = str(x['scope'])
                attrs['events'] = str(x['events'])
                if longFormat:
                    attrs['lumiblocknr'] = str(x['lumiblocknr'])
                guid = str('%s-%s-%s-%s-%s' % (x['guid'][0:8], x['guid'][8:12], x['guid'][12:16], x['guid'][16:20], x['guid'][20:32]))
                attrs['guid'] = guid
                # skip duplicated files
                if skipDuplicate:
                    # extract base LFN and attempt number
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
                            del fileMap[oldMap['guid']]
                            addMap = True
                    else:
                        addMap = True
                    # append    
                    if not addMap:    
                        continue
                    baseLFNmap[baseLFN] = {'guid':guid,
                                           'attNr':attNr}
                fileMap[guid] = attrs
            tmpLog.debug('done')
            return self.SC_SUCCEEDED,fileMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errStr = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errStr)
            if ignoreUnknown and errtype == DataIdentifierNotFound:
                return self.SC_SUCCEEDED,{}
            return errCode,'{0} : {1}'.format(methodName,errStr)



    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        methodName = 'listDatasetReplicas'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
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
                tmpS,dsList = self.listDatasetsInContainer(datasetName)
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



    # list replicas per dataset
    def listReplicasPerDataset(self,datasetName,deepScan=False):
        methodName = 'listReplicasPerDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start with deepScan={0}'.format(deepScan))
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            datasets = []
            if not datasetName.endswith('/'):
                datasets = [dsn]
            else:
                # get constituent datasets
                itr = client.list_content(scope,dsn)
                datasets = [i['name'] for i in itr]
            retMap = {}
            for tmpName in datasets:
                retMap[tmpName] = self.convertOutListDatasetReplicas(tmpName,deepScan)
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
        self.updateEndPointDict()
        try:
            retVal = self.endPointDict[seName][attribute]
            if isinstance(retVal,types.UnicodeType):
                retVal = str(retVal)
            return self.SC_SUCCEEDED,retVal
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
            if seVal['site'] == altName and not seVal['type'] in self.ngEndPointTypes:
                epList.append(seName)
        return epList



    # convert token to endpoint
    def convertTokenToEndpoint(self,baseSeName,token):
        self.updateEndPointDict()
        try:
            altName = self.getSiteAlternateName(baseSeName)[0]
            if altName != None:
                for seName,seVal in self.endPointDict.iteritems():
                    if seVal['site'] == altName and not seVal['type'] in self.ngEndPointTypes:
                        # space token
                        if seVal['token'] == token:
                            return seName
                        # pattern matching
                        if re.search(token,seName) != None:
                            return seName
        except:
            pass
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
        try:
            tmpLog.debug('start datasetName={0} checkCompleteness={1} nFiles={2}'.format(datasetSpec.datasetName,
                                                                                         checkCompleteness,
                                                                                         len(datasetSpec.Files)))
            # update endpoints
            self.updateEndPointDict()
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
                        for endPointToA in self.endPointDict.keys():
                            if re.search('^'+endPointPatt+'$',endPointToA) != None:
                                if not endPointToA in allEndPointList:
                                    allEndPointList.append(endPointToA)
                    else:
                        # normal endpoint
                        if endPointPatt in self.endPointDict.keys() and \
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
                    tmpStat,isTape = self.getSiteProperty(tmpEndPoint,'is_tape')
                    if isTape == True:
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
                    lfc = 'rucio://atlas-rucio.cern.ch:/grid/atlas'
                    # add map
                    if not tmpLfcSeMap.has_key(lfc):
                        tmpLfcSeMap[lfc] = []
                    # get SE
                    tmpStat,seStr = self.getSiteProperty(tmpEndPoint, 'se')
                    if tmpStat != self.SC_SUCCEEDED:
                        tmpLog.error('faild to get SE from for {0} with {1}'.format(tmpEndPoint,seStr))
                        return tmpStat,seStr
                    tmpMatch = re.search('://([^:/]+):*\d*/',seStr)
                    if tmpMatch != None:
                        se = tmpEndPoint
                        if not se in tmpLfcSeMap[lfc]:
                            tmpLfcSeMap[lfc].append(se)
                    if tmpMatch == None:
                        tmpLog.error('faild to extract SE from %s for %s:%s' % \
                                     (seStr,siteName,tmpEndPoint))
                    # get SE + path
                    tmpStat,endpointStr = self.getSiteProperty(tmpEndPoint, 'endpoint')
                    if tmpStat != self.SC_SUCCEEDED:
                        tmpLog.error('faild to get endpoint from for {0} with {1}'.format(tmpEndPoint,endpointStr))
                        return tmpStat,endpointStr
                    # check token
                    if not storageToken in [None,'','NULL']:
                        try:
                            if seStr.split(':')[1] != storageToken:
                                continue
                        except:
                            pass
                    # add full path to storage map
                    tmpSePath = seStr+endpointStr
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
                                            if dstSiteName in returnMap and not dstSiteName in checkedDst:
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
            # all
            for siteName in returnMap.keys():
                siteAllFileList = set()
                storageTypeFile = returnMap[siteName]
                for storageType,fileList in storageTypeFile.iteritems():
                    for tmpFileSpec in fileList:
                        siteAllFileList.add(tmpFileSpec)
                storageTypeFile['all'] = siteAllFileList
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
            client = RucioClient()
            # get PFN
            iGUID = 0
            nGUID = 1000
            retVal = {}
            dids   = []
            for guid,lfn in files.iteritems():
                iGUID += 1
                scope = scopes[lfn]
                dids.append({'scope':scope,'name':lfn})
                if len(dids) % nGUID == 0 or iGUID == len(files):
                    for tmpDict in client.list_replicas(dids,['srm']):
                        tmpLFN = str(tmpDict['name'])
                        surls = []
                        for tmpRSE,tmpSURLs in tmpDict['rses'].iteritems():
                            # rse selection
                            if len(storages) > 0 and not tmpRSE in storages:
                                continue
                            surls += tmpSURLs
                        if len(surls) > 0:
                            retVal[tmpLFN] = surls
                    dids = []
        except:
            errType,errValue = sys.exc_info()[:2]
            return self.SC_FAILED,"file lookup failed with {0}:{1} {2}".format(errType,errValue,traceback.format_exc())
        # return
        return self.SC_SUCCEEDED,retVal



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
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            filters = {}
            if dsn.endswith('/'):
                dsn = dsn[:-1]
            filters['name'] = dsn
            dsList = set()
            for name in client.list_dids(scope, filters, 'dataset'):
                dsList.add('%s:%s' % (scope, name))
            for name in client.list_dids(scope, filters, 'container'):
                dsList.add('%s:%s/' % (scope, name))
            dsList = list(dsList)
            # ignore panda internal datasets
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
        except DataIdentifierAlreadyExists: 
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
            

    
    # wrapper for list_content
    def wp_list_content(self,client,scope,dsn):
        retList = []
        # get contents
        for data in client.list_content(scope,dsn):
            if data['type'] == 'CONTAINER':
                retList += self.wp_list_content(client,data['scope'],data['name'])
            elif data['type'] == 'DATASET':
                retList.append('{0}:{1}'.format(data['scope'],data['name']))
            else:
                pass
        return retList



    # list datasets in container
    def listDatasetsInContainer(self,containerName):
        methodName = 'listDatasetsInContainer'
        methodName += ' <containerName={0}>'.format(containerName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get rucio
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(containerName)
            # get contents
            dsList = self.wp_list_content(client,scope,dsn)
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
            # get Rucio API
            client = RucioClient()
            c_scope,c_name = self.extract_scope(containerName)
            if c_name.endswith('/'):
                c_name = c_name[:-1]
            dsns = []
            for ds in datasetNames:
                ds_scope, ds_name = self.extract_scope(ds)
                dsn = {'scope': ds_scope, 'name': ds_name}
                dsns.append(dsn)
            try:
                # add datasets
                client.add_datasets_to_container(scope=c_scope, name=c_name, dsns=dsns)
            except DuplicateContent:
                # add datasets one by one
                for ds in dsns:
                    try:
                        client.add_datasets_to_container(scope=c_scope, name=c_name, dsns=[ds])
                    except DuplicateContent:
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
            tmpLog.error('failed to get a list of DBRelease datasets from DDM')
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
            tmpLog.error('failed to get the latest version of DBRelease dataset from DDM')
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
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # check metadata to avoid a bug in rucio
            tmpRet = client.get_metadata(scope,dsn)
            # close
            client.set_status(scope,dsn,open=False)
        except UnsupportedOperation:
            pass
        except DataIdentifierNotFound:
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
            userName = self.parse_dn(userName)
            # get rucio API
            client = RucioClient()
            userInfo = None
            for i in client.list_accounts(account_type='USER',identity=userName):
                userInfo = {'nickname':i['account'],
                            'email':i['email']}
                break
            if userInfo == None:
                # remove /CN=\d
                userName = re.sub('(/CN=\d+)+$','',userName)
                for i in client.list_accounts(account_type='USER',identity=userName):
                    userInfo = {'nickname':i['account'],
                                'email':i['email']}
                    break
            if userInfo == None:
                tmpLog.error('failed to get account info')
                return self.SC_FAILED,None
            tmpRet = userInfo
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0}:{1}'.format(methodName,errMsg)
        tmpLog.debug('done with '+str(tmpRet))
        return self.SC_SUCCEEDED,tmpRet



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
                                activity=None,grouping=None,weight=None,copies=1):
        methodName = 'registerDatasetLocation'
        methodName = '{0} datasetName={1} location={2}'.format(methodName,datasetName,location)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # get rucio API
            client = RucioClient()
            # cleanup DN
            owner = self.parse_dn(owner)
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
            if grouping == None:
                grouping = 'DATASET'
            # add rule
            dids = []
            did = {'scope': scope, 'name': dsn}
            dids.append(did)
            locList = location.split(',')
            for tmpLoc in locList:
                client.add_replication_rule(dids=dids,copies=copies,rse_expression=tmpLoc,lifetime=lifetime,
                                            grouping=grouping,account=owner,locked=False,notify='N',
                                            ignore_availability=True,activity=activity,weight=weight)
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
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # get the number of files
            if emptyOnly:
                nFiles = 0
                for x in client.list_files(scope, dsn, long=long):
                    nFiles += 1
            # erase
            if not emptyOnly or nFiles == 0:
                client.set_metadata(scope=scope, name=dsn, key='lifetime', value=0.0001)
                retStr = 'deleted {0}'.format(datasetName)
            else:
                retStr = 'keep {0} where {1} files are available'.format(datasetName,nFiles)
        except DataIdentifierNotFound:
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
    def registerDatasetSubscription(self,datasetName,location,activity,lifetime=None,
                                    asynchronous=False):
        methodName = 'registerDatasetSubscription'
        methodName = '{0} datasetName={1} location={2} activity={3} asyn={4}'.format(methodName,datasetName,
                                                                                     location,activity,asynchronous)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isOK = True
        try:
            if lifetime != None:
                lifetime = lifetime*24*60*60
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            dids = [{'scope': scope, 'name': dsn}]
            # check if a replication rule already exists
            for rule in client.list_did_rules(scope=scope, name=dsn):
                if (rule['rse_expression'] == location) and (rule['account'] == client.account):
                    return True
            client.add_replication_rule(dids=dids,copies=1,rse_expression=location,weight=None,
                                        lifetime=lifetime, grouping='DATASET', account=client.account,
                                        locked=False, notify='N',ignore_availability=True,
                                        activity=activity,asynchronous=asynchronous)
        except DuplicateRule:
            pass
        except DataIdentifierNotFound:
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
                tmpStat,lfc = self.getSiteProperty(tmpEndPoint,'servedlfc')
                if tmpStat != self.SC_SUCCEEDED:
                    tmpLog.error('faild to get catalog for {0} with {0}'.format(tmpEndPoint,lfc))
                    return tmpStat,lfc
                # add map
                if not lfcSeMap.has_key(lfc):
                    lfcSeMap[lfc] = []
                # get SE
                tmpStat,seStr = self.getSiteProperty(tmpEndPoint,'se')
                if tmpStat != self.SC_SUCCEEDED:
                    tmpLog.error('faild to get SE for {0} with {0}'.format(tmpEndPoint,seStr))
                    return tmpStat,seStr
                tmpMatch = re.search('://([^:/]+):*\d*/',seStr)
                if tmpMatch != None:
                    se = tmpEndPoint
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
    def convertOutListDatasetReplicas(self,datasetName,usefileLookup=False):
        retMap = {}
        # get rucio API
        client = RucioClient()
        # get scope and name
        scope,dsn = self.extract_scope(datasetName)
        # get replicas
        itr = client.list_dataset_replicas(scope,dsn,deep=usefileLookup)
        items = []
        for item in itr:
            items.append(item)
        # deep lookup if shallow gave nothing
        if items == [] and not usefileLookup:
            itr = client.list_dataset_replicas(scope,dsn,deep=True)
            for item in itr:
                items.append(item)
        for item in items:
            rse = item["rse"]
            retMap[rse] = [{'total':item["length"],
                            'found':item["available_length"],
                            'tsize':item["bytes"],
                            'asize':item["available_bytes"],
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
            res = urllib2.urlopen('http://atlas-agis-api.cern.ch/request/ddmendpoint/query/list/?json&state=ACTIVE&preset=dict')
            jsonStr = res.read()
            self.endPointDict = json.loads(jsonStr)
            tmpLog.debug('got {0} endpoints '.format(len(self.endPointDict)))
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errStr = 'failed to update EP with {0} {1} jsonStr={2}'.format(errtype.__name__,
                                                                           errvalue,
                                                                           jsonStr)
            tmpLog.error(errStr)
        return



    # check if the dataset is distributed
    def isDistributedDataset(self,datasetName):
        methodName  = 'isDistributedDataset'
        methodName += ' <datasetName={0}>'.format(datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isDDS = None
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # get rules
            for rule in client.list_did_rules(scope,dsn):
                if rule['grouping'] != 'NONE':
                    isDDS = False
                    break
                elif isDDS == None:
                    isDDS = True
            # use False when there is no rule
            if isDDS == None:
                isDDS = False
        except:
            isOK = False
        if not isOK:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            errMsg = '{0} {1}'.format(errtype.__name__,errvalue)
            tmpLog.error(errMsg)
            return errCode,'{0} : {1}'.format(methodName,errMsg)
        tmpLog.debug('done with {0}'.format(isDDS))
        return self.SC_SUCCEEDED,isDDS


    # update replication rules
    def updateReplicationRules(self,datasetName,dataMap):
        methodName = 'updateReplicationRules'
        methodName = '{0} datasetName={1}'.format(methodName,datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope,dsn = self.extract_scope(datasetName)
            # get rules
            for rule in client.list_did_rules(scope=scope, name=dsn):
                for dataKey,data in dataMap.iteritems():
                    if rule['rse_expression'] == dataKey or re.search(dataKey,rule['rse_expression']) is not None:
                        tmpLog.debug('set data={0} on {1}'.format(str(data),rule['rse_expression']))
                        client.update_replication_rule(rule['id'],data)
        except DataIdentifierNotFound:
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
