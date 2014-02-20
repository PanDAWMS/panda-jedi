import re
import sys
import random

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from JobBrokerBase import JobBrokerBase
import AtlasBrokerUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for ATLAS analysis
class AtlasAnalJobBroker (JobBrokerBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        JobBrokerBase.__init__(self,ddmIF,taskBufferIF)
        self.dataSiteMap = {}


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID))
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # get primary site candidates 
        sitePreAssigned = False
        excludeList = []
        includeList = None
        if not taskSpec.site in ['',None]:
            # site is pre-assigned
            scanSiteList = [taskSpec.site]
            tmpLog.debug('site={0} is pre-assigned'.format(taskSpec.site))
            sitePreAssigned = True
        else:
            scanSiteList = []
            # get list of site access
            siteAccessList = self.taskBufferIF.listSiteAccess(None,taskSpec.userName)
            siteAccessMap = {}
            for tmpSiteName,tmpAccess in siteAccessList:
                siteAccessMap[tmpSiteName] = tmpAccess
            # site limitation
            if taskSpec.useLimitedSites():
                if 'excludedSite' in taskParamMap:
                    excludeList = taskParamMap['excludedSite']
                if 'includedSite' in taskParamMap:
                    includeList = taskParamMap['includedSite']
            # loop over all sites        
            for siteName,tmpSiteSpec in self.siteMapper.siteSpecList.iteritems():
                if tmpSiteSpec.type == 'analysis' and taskSpec.cloud in [None,'','any',tmpSiteSpec.cloud]:
                    # check if excluded
                    if AtlasBrokerUtils.isMatched(siteName,excludeList):
                        tmpLog.debug('  skip {0} excluded'.format(siteName))
                        continue
                    # check if included
                    if includeList != None and not AtlasBrokerUtils.isMatched(siteName,includeList):
                        tmpLog.debug('  skip {0} not included'.format(siteName))
                        continue
                    # limited access
                    if tmpSiteSpec.accesscontrol == 'grouplist':
                        if not siteAccessMap.has_key(tmpSiteSpec.sitename) or \
                                siteAccessMap[tmpSiteSpec.sitename] != 'approved':
                            tmpLog.debug('  skip {0} limited access'.format(siteName))
                            continue
                    scanSiteList.append(siteName)
            tmpLog.debug('cloud=%s has %s candidates' % (taskSpec.cloud,len(scanSiteList)))
        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,taskSpec.prodSourceLabel)
        if not tmpSt:
            tmpLog.error('failed to get job statistics')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # allowed remote access protocol
        allowedRemoteProtocol = 'xrd'
        ######################################
        # selection for data availability
        dataWeight = {}
        remoteSourceList = {}
        if inputChunk.getDatasets() != []:    
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                if not self.dataSiteMap.has_key(datasetName):
                    # get the list of sites where data is available
                    tmpLog.debug('getting the list of sites where {0} is avalable'.format(datasetName))
                    tmpSt,tmpRet = AtlasBrokerUtils.getAnalSitesWithData(scanSiteList,
                                                                         self.siteMapper,
                                                                         self.ddmIF,datasetName)
                    if tmpSt == self.SC_FAILED:
                        tmpLog.error('failed to get the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retTmpError
                    if tmpSt == self.SC_FATAL:
                        tmpLog.error('fatal error when getting the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retFatal
                    # append
                    self.dataSiteMap[datasetName] = tmpRet
                    if datasetName.startswith('ddo'):
                        tmpLog.debug(' {0} sites'.format(len(tmpRet)))
                    else:
                        tmpLog.debug(' {0} sites : {1}'.format(len(tmpRet),str(tmpRet)))
                # check if the data is available at somewhere
                if self.dataSiteMap[datasetName] == {}:
                    tmpLog.error('{0} is unavaiable at any site'.format(datasetName))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retFatal
                # check if the data is available on disk
                if AtlasBrokerUtils.getAnalSitesWithDataDisk(self.dataSiteMap[datasetName]) == []:
                    tmpLog.error('{0} is avaiable only on tape'.format(datasetName))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retFatal
            # get the list of sites where data is available    
            scanSiteList = None     
            normFactor = 0
            for datasetName,tmpDataSite in self.dataSiteMap.iteritems():
                normFactor += 1
                # get sites where disk replica is available
                tmpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite)
                # get sites which can remotely access source sites
                if not sitePreAssigned:
                    tmpSatelliteSites = AtlasBrokerUtils.getSatelliteSites(tmpSiteList,self.taskBufferIF,
                                                                           protocol=allowedRemoteProtocol)
                else:
                    tmpSatelliteSites = {}
                for tmpSiteName in tmpSiteList:
                    if not dataWeight.has_key(tmpSiteName):
                        dataWeight[tmpSiteName] = 1
                    else:
                        dataWeight[tmpSiteName] += 1
                for tmpSiteName,tmpWeightSrcMap in tmpSatelliteSites.iteritems():
                    # check exclusion
                    if AtlasBrokerUtils.isMatched(tmpSiteName,excludeList):
                        continue
                    # check inclusion
                    if includeList != None and not AtlasBrokerUtils.isMatched(tmpSiteName,includeList):
                        continue
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # limited access
                    if tmpSiteSpec.accesscontrol == 'grouplist':
                        if not siteAccessMap.has_key(tmpSiteSpec.sitename) or \
                                siteAccessMap[tmpSiteSpec.sitename] != 'approved':
                            continue
                    # check cloud
                    if not taskSpec.cloud in [None,'','any',tmpSiteSpec.cloud]: 
                        continue
                    # sum weight
                    if not dataWeight.has_key(tmpSiteName):
                        dataWeight[tmpSiteName] = tmpWeightSrcMap['weight']
                    else:
                        dataWeight[tmpSiteName] += tmpWeightSrcMap['weight']
                    # make remote source list
                    if not remoteSourceList.has_key(tmpSiteName):
                        remoteSourceList[tmpSiteName] = {}
                    remoteSourceList[tmpSiteName][datasetName] = tmpWeightSrcMap['source']
                # first list
                if scanSiteList == None:
                    scanSiteList = tmpSiteList + tmpSatelliteSites.keys()
                    continue
                # pickup sites which have all data
                newScanList = []
                for tmpSiteName in scanSiteList + tmpSatelliteSites.keys():
                    if tmpSiteName in tmpSiteList:
                        newScanList.append(tmpSiteName)
                scanSiteList = newScanList
                tmpLog.debug('{0} is available at {1} sites'.format(datasetName,len(scanSiteList)))
            tmpLog.debug('{0} candidates have input data'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retFatal
        ######################################
        # selection for status
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check site status
            skipFlag = False
            if tmpSiteSpec.status in ['offline','brokeroff','test']:
                skipFlag = True
            if not skipFlag:    
                newScanSiteList.append(tmpSiteName)
            else:
                tmpLog.debug('  skip %s due to status=%s' % (tmpSiteName,tmpSiteSpec.status))
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed site status check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for release
        if not taskSpec.transHome in [None,'AnalysisTransforms']:
            if taskSpec.transHome.startswith('ROOT'):
                # hack until x86_64-slc6-gcc47-opt is published in installedsw
                if taskSpec.architecture == 'x86_64-slc6-gcc47-opt':
                    tmpCmtConfig = 'x86_64-slc6-gcc46-opt'
                else:
                    tmpCmtConfig = taskSpec.architecture
                siteListWithSW = taskBuffer.checkSitesWithRelease(scanSiteList,
                                                                  cmtConfig=tmpCmtConfig,
                                                                  onlyCmtConfig=True)
            else:    
                # remove AnalysisTransforms-
                transHome = re.sub('^[^-]+-*','',taskSpec.transHome)
                transHome = re.sub('_','-',transHome)
                if re.search('rel_\d+(\n|$)',taskSpec.transHome) == None:
                    # cache is checked 
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             caches=transHome,
                                                                             cmtConfig=taskSpec.architecture)
                elif transHome == '':
                    # release is checked 
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             releases=taskSpec.transUses,
                                                                             cmtConfig=taskSpec.architecture)
                else:
                    # nightlies
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             releases='nightlies',
                                                                             cmtConfig=taskSpec.architecture)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # release check is disabled or release is available
                if tmpSiteSpec.releases == ['ANY'] or \
                   tmpSiteSpec.cloud in ['ND']:
                    newScanSiteList.append(tmpSiteName)
                elif tmpSiteName in siteListWithSW:
                    newScanSiteList.append(tmpSiteName)
                else:
                    # release is unavailable
                    tmpLog.debug('  skip %s due to missing rel/cache %s:%s' % \
                                 (tmpSiteName,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for SW {1}:{2}'.format(len(scanSiteList),
                                                                       taskSpec.transHome,
                                                                       taskSpec.architecture))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for memory
        minRamCount  = taskSpec.ramCount
        if not minRamCount in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxmemory != 0 and minRamCount != 0 and minRamCount > tmpSiteSpec.maxmemory:
                    tmpLog.debug('  skip {0} due to site RAM shortage={1}(site upper limit) < {2}'.format(tmpSiteName,
                                                                                                          tmpSiteSpec.maxmemory,
                                                                                                          minRamCount))
                    continue
                if tmpSiteSpec.minmemory != 0 and minRamCount != 0 and minRamCount < tmpSiteSpec.minmemory:
                    tmpLog.debug('  skip {0} due to job RAM shortage={1}(site lower limit) > {2}'.format(tmpSiteName,
                                                                                                         tmpSiteSpec.minmemory,
                                                                                                         minRamCount))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed memory check ={1}{2}'.format(len(scanSiteList),
                                                                             minRamCount,taskSpec.ramCountUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for scratch disk
        minDiskCountS = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCountS = minDiskCountS / 1024 / 1024
        # size for direct IO sites
        if taskSpec.useLocalIO():
            minDiskCountR = minDiskCountS
        else:
            minDiskCountR = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()
            minDiskCountR = minDiskCountR / 1024 / 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir != 0:
                if tmpSiteSpec.isDirectIO():
                    minDiskCount = minDiskCountR
                else:
                    minDiskCount = minDiskCountS
                if minDiskCount > tmpSiteSpec.maxwdir:
                    tmpLog.debug('  skip {0} due to small scratch disk={1} < {2}'.format(tmpSiteName,
                                                                                         tmpSiteSpec.maxwdir,
                                                                                         minDiskCount))
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed scratch disk check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for available space in SE
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            # check at the site
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # free space must be >= 200GB
            diskThreshold = 200
            tmpSpaceSize = tmpSiteSpec.space
            if tmpSiteSpec.space != 0 and tmpSpaceSize < diskThreshold:
                tmpLog.debug('  skip {0} due to disk shortage in SE = {1} < {2}GB'.format(tmpSiteName,tmpSiteSpec.space,
                                                                                          diskThreshold))
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed SE space check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for walltime
        minWalltime = taskSpec.walltime
        if not minWalltime in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxtime != 0 and minWalltime > tmpSiteSpec.maxtime:
                    tmpLog.debug('  skip {0} due to short site walltime={1}(site upper limit) < {2}'.format(tmpSiteName,
                                                                                                            tmpSiteSpec.maxtime,
                                                                                                            minWalltime))
                    continue
                if tmpSiteSpec.mintime != 0 and minWalltime < tmpSiteSpec.mintime:
                    tmpLog.debug('  skip {0} due to short job walltime={1}(site lower limit) > {2}'.format(tmpSiteName,
                                                                                                           tmpSiteSpec.mintime,
                                                                                                           minWalltime))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed walltime check ={1}{2}'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for nPilot
        nWNmap = self.taskBufferIF.getCurrentSiteData()
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            # check at the site
            nPilot = 0
            if nWNmap.has_key(tmpSiteName):
                nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
            if nPilot == 0 and not taskSpec.prodSourceLabel in ['test']:
                tmpLog.debug('  skip %s due to no pilot' % tmpSiteName)
                #continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # sites already used by task
        tmpSt,sitesUsedByTask = self.taskBufferIF.getSitesUsedByTask_JEDI(taskSpec.jediTaskID)
        if not tmpSt:
            tmpLog.error('failed to get sites which already used by task')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # calculate weight
        tmpSt,jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                    taskSpec.prodSourceLabel,
                                                                                    taskSpec.currentPriority)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        tmpLog.debug('final {0} candidates'.format(len(scanSiteList)))
        weightMap = {}
        candidateSpecList = []
        for tmpSiteName in scanSiteList:
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'running',  None,taskSpec.workQueue_ID)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'defined',  None,taskSpec.workQueue_ID)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',None,taskSpec.workQueue_ID)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + 1) / float(nAssigned + 1)
            # noramize weights by taking data availability into account
            if dataWeight.has_key(tmpSiteName):
                weight = weight * dataWeight[tmpSiteName]
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # set weight
            siteCandidateSpec.weight = weight
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if not weightMap.has_key(weight):
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)    
        # limit the number of sites
        maxNumSites = 5
        weightList = weightMap.keys()
        weightList.sort()
        weightList.reverse()
        for weightVal in weightList:
            if len(candidateSpecList) >= maxNumSites:
                break
            sitesWithWeight = weightMap[weightVal]
            random.shuffle(sitesWithWeight)
            candidateSpecList += sitesWithWeight[:(maxNumSites-len(candidateSpecList))]
        # collect site names
        scanSiteList = []    
        for siteCandidateSpec in candidateSpecList:
            scanSiteList.append(siteCandidateSpec.siteName)
        # get list of available files
        availableFileMap = {}     
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteStorageEndpointMap(scanSiteList,self.siteMapper)
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            ngGroup=[2])
                if tmpAvFileMap == None:
                    raise Interaction.JEDITemporaryError,'ddmIF.getAvailableFiles failed'
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        # append candidates
        newScanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            tmpSiteName = siteCandidateSpec.siteName
            # set available files
            if inputChunk.getDatasets() == []: 
                isAvailable = True
            else:
                isAvailable = False
            for tmpDatasetName,availableFiles in availableFileMap.iteritems():
                tmpDatasetSpec = inputChunk.getDatasetWithName(tmpDatasetName)
                # check remote files
                if remoteSourceList.has_key(tmpSiteName) and remoteSourceList[tmpSiteName].has_key(tmpDatasetName):
                    for tmpRemoteSite in remoteSourceList[tmpSiteName][tmpDatasetName]:
                        if availableFiles.has_key(tmpRemoteSite) and \
                                len(tmpDatasetSpec.Files) <= availableFiles[tmpRemoteSite]['localdisk']:
                            # use only remote disk files
                            siteCandidateSpec.remoteFiles += availableFiles[tmpRemoteSite]['localdisk']
                            # set remote site and access protocol
                            siteCandidateSpec.remoteProtocol = allowedRemoteProtocol
                            siteCandidateSpec.remoteSource   = tmpSiteName
                            isAvailable = True
                            break
                # local files
                if availableFiles.has_key(tmpSiteName):
                    if len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['localdisk']) or \
                            len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['cache']):
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['localdisk']
                        # add cached files to local list since cached files go to pending when reassigned
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.localTapeFiles  += availableFiles[tmpSiteName]['localtape']
                        siteCandidateSpec.cacheFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.remoteFiles += availableFiles[tmpSiteName]['remote']
                        isAvailable = True
                if not isAvailable:
                    break
            # append
            if not isAvailable:
                tmpLog.debug('  skip {0} file unavailable'.format(siteCandidateSpec.siteName))
                continue
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpLog.debug('  use {0} with weight={1} nLocalDisk={2} nLocalTaps={3} nCache={4} nRemote={5}'.format(siteCandidateSpec.siteName,
                                                                                                                 siteCandidateSpec.weight,
                                                                                                                 len(siteCandidateSpec.localDiskFiles),
                                                                                                                 len(siteCandidateSpec.localTapeFiles),
                                                                                                                 len(siteCandidateSpec.cacheFiles),
                                                                                                                 len(siteCandidateSpec.remoteFiles),
                                                                                                                 ))
        scanSiteList = newScanSiteList
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
    
