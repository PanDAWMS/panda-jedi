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
        from dq2.clientapi.DQ2 import DQUnknownDatasetException
        self.fatalErrors = [DQUnknownDatasetException]



    # get files in dataset
    def getFilesInDataset(self,datasetName):
        methodName = 'getFilesInDataset'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listFilesInDataset(datasetName)
            return self.SC_SUCCEEDED,tmpRet[0]
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errCode = self.checkError(errtype)
            return errCode,'%s : %s %s' % (methodName,errtype.__name__,errvalue)



    # list dataset replicas
    def listDatasetReplicas(self,datasetName):
        methodName = 'listDatasetReplicas'
        try:
            # get DQ2 API            
            dq2=DQ2()
            # get file list
            tmpRet = dq2.listDatasetReplicas(datasetName,old=False)
            return self.SC_SUCCEEDED,tmpRet
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
    def getAvailableFiles(self,datasetSpec,siteEndPointMap,siteMapper,ngGroup=[]):
        # make logger
        methodName = 'getAvailableFiles'
        methodName = '{0} datasetID={1}({2})'.format(methodName,datasetSpec.datasetID,datasetSpec.datasetName)
        tmpLog = MsgWrapper(logger,methodName)
        tmpLog.debug('start')
        try:
            # list of NG endpoints
            ngEndPoints = []
            if 1 in ngGroup:
                ngEndPoints += ['_SCRATCHDISK$','_LOCALGROUPDISK$','_USERDISK$',
                               '_DAQ$','_TMPDISK$','_TZERO$','_GRIDFTP$']
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
            # collect SE and LFC hosts
            lfcSeMap = {}
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                for tmpEndPoint in allEndPointList:
                    # get LFC
                    lfcStr = TiersOfATLAS.getLocalCatalog(tmpEndPoint)
                    try:
                        lfc = lfcStr.split('/')[2].split(':')[0]
                    except:
                        logger.error('faild to extract LFC from %s for %s:%s' % \
                                     (lfcStr,siteName,tmpEndPoint))
                        continue
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
                    else:
                        logger.error('faild to extract SE from %s for %s:%s' % \
                                     (seStr,siteName,tmpEndPoint))
            # collect GUIDs and LFNs
            fileMap = {}
            lfnMap = {}
            for tmpFile in datasetSpec.Files:
                fileMap[tmpFile.GUID] = tmpFile.lfn
                lfnMap[tmpFile.lfn] = tmpFile
            # get SURLs
            surlMap = {}
            for lfcHost,seList in lfcSeMap.iteritems():
                tmpStat,tmpRetMap = self.getSURLsFromLFC(fileMap,lfcHost,seList)
                if tmpStat != self.SC_SUCCEEDED:
                    raise tmpStat,tmpRetMap
                for lfn,surls in tmpRetMap.iteritems():
                    if not surlMap.has_key(lfn):
                        surlMap[lfn] = surls
                    else:
                        surlMap[lfn] += surls
            # make return
            returnMap = {}
            for siteName,allEndPointList in siteAllEndPointsMap.iteritems():
                # add site
                if not returnMap.has_key(siteName):
                    returnMap[siteName] = {'localdisk':[],'localtape':[],'cache':[],'remote':[]}
                # loop over all files    
                for tmpFileSpec in datasetSpec.Files:
                    # check if the file is cached
                    tmpSiteSpec = siteMapper.getSite(siteName)
                    if DataServiceUtils.isCachedFile(datasetSpec.datasetName,tmpSiteSpec):
                        # add to cached file list
                        returnMap[siteName]['cache'].append(tmpFileSpec)
                        continue
                    # skip if SURL is not found
                    if not surlMap.has_key(tmpFileSpec.lfn):
                        continue
                    # loop over all endpoints
                    for tmpEndPoint in allEndPointList:
                        # get SE + path
                        seStr = TiersOfATLAS.getSiteProperty(tmpEndPoint, 'srm')
                        tmpMatch = re.search('(srm://.+)$',seStr)
                        if tmpMatch == None:
                            logger.error('faild to extract SE+PATH from %s for %s:%s' % \
                                         (seStr,siteName,tmpEndPoint))
                            continue
                        fullSePath = tmpMatch.group(1)
                        compactSePath = re.sub('(:\d+)*/srm/[^\?]+\?SFN=','',fullSePath)
                        # loop over all SURLs
                        for tmpSURL in surlMap[tmpFileSpec.lfn]:
                            if tmpSURL.startswith(fullSePath) or tmpSURL.startswith(compactSePath):
                                # add to local file list
                                if TiersOfATLAS.isTapeSite(tmpEndPoint):
                                    # TAPE
                                    if not tmpFileSpec in returnMap[siteName]['localtape']:
                                        returnMap[siteName]['localtape'].append(tmpFileSpec)
                                else:
                                    # DISK
                                    if not tmpFileSpec in returnMap[siteName]['localdisk']:
                                        returnMap[siteName]['localdisk'].append(tmpFileSpec)
            # return
            tmpLog.debug('done')            
            return self.SC_SUCCEEDED,returnMap
        except:
            errtype,errvalue = sys.exc_info()[:2]
            errMsg = 'failed with {0} {1}'.format(errtype.__name__,errvalue)
            logger.error(errMsg)
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
        nGUID = 1000
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



    # check error
    def checkError(self,errType):
        if errType in self.fatalErrors:
            # fatal error
            return self.SC_FATAL
        else:
            # temprary error
            return self.SC_FAILED
            
    
