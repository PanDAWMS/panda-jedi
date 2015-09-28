import re
import sys
import random
import datetime

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from JobBrokerBase import JobBrokerBase
from pandaserver.taskbuffer import PrioUtil
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


        
    # wrapper for return
    def sendLogMessage(self,tmpLog):
        # send info to logger
        tmpLog.bulkSendMsg('analy_brokerage')
        tmpLog.debug('sent')



    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID),
                            monToken='<jediTaskID={0} {1}>'.format(taskSpec.jediTaskID,
                                                                   datetime.datetime.utcnow().isoformat('/')))
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # get primary site candidates 
        sitePreAssigned = False
        excludeList = []
        includeList = None
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
            if tmpSiteSpec.type == 'analysis':
                scanSiteList.append(siteName)
        # preassigned
        if not taskSpec.site in ['',None]:
            # site is pre-assigned
            tmpLog.debug('site={0} is pre-assigned'.format(taskSpec.site))
            sitePreAssigned = True
            if not taskSpec.site in scanSiteList:
                scanSiteList.append(taskSpec.site)
        tmpLog.debug('initial {0} candidates'.format(len(scanSiteList)))
        # allowed remote access protocol
        allowedRemoteProtocol = 'fax'
        # MP    
        if taskSpec.coreCount != None and taskSpec.coreCount > 1:
            # use MCORE only
            useMP = 'only'
        elif taskSpec.coreCount == 0:
            # use MCORE and normal 
            useMP = 'any'
        else:
            # not use MCORE
            useMP = 'unuse'
        ######################################
        # selection for data availability
        dataWeight = {}
        remoteSourceList = {}
        tapeOnlyDatasets = []
        if inputChunk.getDatasets() != []:    
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                if not self.dataSiteMap.has_key(datasetName):
                    # get the list of sites where data is available
                    tmpLog.debug('getting the list of sites where {0} is avalable'.format(datasetName))
                    tmpSt,tmpRet = AtlasBrokerUtils.getAnalSitesWithData(scanSiteList,
                                                                         self.siteMapper,
                                                                         self.ddmIF,datasetName)
                    if tmpSt in [Interaction.JEDITemporaryError,Interaction.JEDITimeoutError]: 
                        tmpLog.error('temporary failed to get the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        # send info to logger
                        self.sendLogMessage(tmpLog)
                        return retTmpError
                    if tmpSt == Interaction.JEDIFatalError:
                        tmpLog.error('fatal error when getting the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        # send info to logger
                        self.sendLogMessage(tmpLog)
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
                    # send info to logger
                    self.sendLogMessage(tmpLog)
                    return retFatal
                # check if the data is available on disk
                if AtlasBrokerUtils.getAnalSitesWithDataDisk(self.dataSiteMap[datasetName]) == []:
                    tmpLog.debug('{0} is avaiable only on tape'.format(datasetName))
                    tapeOnlyDatasets.append(datasetName)
                    #taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    # send info to logger
                    #self.sendLogMessage(tmpLog)
                    #return retFatal
            # get the list of sites where data is available    
            scanSiteList = None     
            normFactor = 0
            for datasetName,tmpDataSite in self.dataSiteMap.iteritems():
                normFactor += 1
                # get sites where replica is available
                tmpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite,includeTape=True)
                tmpDiskSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite,includeTape=False)
                # get sites which can remotely access source sites
                if inputChunk.isMerging:
                    # disable remote access for merging
                    tmpSatelliteSites = {}
                elif (not sitePreAssigned) or (sitePreAssigned and not taskSpec.site in tmpSiteList):
                    tmpSatelliteSites = AtlasBrokerUtils.getSatelliteSites(tmpDiskSiteList,self.taskBufferIF,
                                                                           self.siteMapper,nSites=50,
                                                                           protocol=allowedRemoteProtocol)
                else:
                    tmpSatelliteSites = {}
                # make weight map for local
                for tmpSiteName in tmpSiteList:
                    if not dataWeight.has_key(tmpSiteName):
                        dataWeight[tmpSiteName] = 1
                    else:
                        dataWeight[tmpSiteName] += 1
                    # give more weight to disk
                    if tmpSiteName in tmpDiskSiteList:
                        dataWeight[tmpSiteName] += 1 
                # make weight map for remote
                for tmpSiteName,tmpWeightSrcMap in tmpSatelliteSites.iteritems():
                    # skip since local data is available
                    if tmpSiteName in tmpSiteList:
                        continue
                    # negative weight for remote access
                    wRemote = 50.0
                    # sum weight
                    if not dataWeight.has_key(tmpSiteName):
                        dataWeight[tmpSiteName] = float(tmpWeightSrcMap['weight'])/wRemote
                    else:
                        dataWeight[tmpSiteName] += float(tmpWeightSrcMap['weight'])/wRemote
                    # make remote source list
                    if not remoteSourceList.has_key(tmpSiteName):
                        remoteSourceList[tmpSiteName] = {}
                    remoteSourceList[tmpSiteName][datasetName] = tmpWeightSrcMap['source']
                # first list
                if scanSiteList == None:
                    scanSiteList = []
                    for tmpSiteName in tmpSiteList + tmpSatelliteSites.keys():
                        if not tmpSiteName in scanSiteList:
                            scanSiteList.append(tmpSiteName)
                    continue
                # pickup sites which have all data
                newScanList = []
                for tmpSiteName in tmpSiteList + tmpSatelliteSites.keys():
                    if tmpSiteName in scanSiteList and not tmpSiteName in newScanList:
                        newScanList.append(tmpSiteName)
                scanSiteList = newScanList
                tmpLog.debug('{0} is available at {1} sites'.format(datasetName,len(scanSiteList)))
            tmpLog.debug('{0} candidates have input data'.format(len(scanSiteList)))
            # check for preassigned
            if sitePreAssigned and not taskSpec.site in scanSiteList:
                scanSiteList = []
                tmpLog.debug('data is unavailable locally or remotely at preassigned site {0}'.format(taskSpec.site))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retFatal
        ######################################
        # selection for status
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check site status
            skipFlag = False
            if tmpSiteSpec.status in ['offline']:
                skipFlag = True
            elif tmpSiteSpec.status in ['brokeroff','test']:
                if not sitePreAssigned:
                    skipFlag = True
                elif tmpSiteName != taskSpec.site:
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
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for MP
        if not sitePreAssigned:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if useMP == 'any' or (useMP == 'only' and tmpSiteSpec.coreCount > 1) or \
                        (useMP =='unuse' and tmpSiteSpec.coreCount in [0,1,None]):
                        newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.debug('  skip %s due to core mismatch site:%s != task:%s' % \
                                 (tmpSiteName,tmpSiteSpec.coreCount,taskSpec.coreCount))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for useMP={1}'.format(len(scanSiteList),useMP))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for release
        if taskSpec.transHome != None:
            if taskSpec.transHome.startswith('ROOT'):
                # hack until x86_64-slc6-gcc47-opt is published in installedsw
                if taskSpec.architecture == 'x86_64-slc6-gcc47-opt':
                    tmpCmtConfig = 'x86_64-slc6-gcc46-opt'
                else:
                    tmpCmtConfig = taskSpec.architecture
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                         cmtConfig=tmpCmtConfig,
                                                                         onlyCmtConfig=True)
            elif 'AthAnalysisBase' in taskSpec.transHome:
                # AthAnalysis
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                         cmtConfig=taskSpec.architecture,
                                                                         onlyCmtConfig=True)
            else:    
                # remove AnalysisTransforms-
                transHome = re.sub('^[^-]+-*','',taskSpec.transHome)
                transHome = re.sub('_','-',transHome)
                if re.search('rel_\d+(\n|$)',taskSpec.transHome) == None and taskSpec.transHome != 'AnalysisTransforms':
                    # cache is checked 
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             caches=transHome,
                                                                             cmtConfig=taskSpec.architecture)
                elif transHome == '' and taskSpec.transUses != None:
                    # remove Atlas-
                    transUses = taskSpec.transUses.split('-')[-1]
                    # release is checked 
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             releases=transUses,
                                                                             cmtConfig=taskSpec.architecture)
                else:
                    # nightlies
                    siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                             releases='CVMFS')
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # release check is disabled or release is available
                if tmpSiteSpec.releases == ['ANY']:
                    newScanSiteList.append(tmpSiteName)
                elif tmpSiteName in siteListWithSW:
                    newScanSiteList.append(tmpSiteName)
                else:
                    # release is unavailable
                    tmpLog.debug('  skip %s due to missing rel/cache %s:%s:%s' % \
                                 (tmpSiteName,taskSpec.transUses,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for SW {1}:{2}:{3}'.format(len(scanSiteList),
                                                                           taskSpec.transUses,
                                                                           taskSpec.transHome,
                                                                           taskSpec.architecture))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for memory
        minRamCount  = max(taskSpec.ramCount, inputChunk.ramCount)
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
                                                                             minRamCount,taskSpec.ramUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for scratch disk
        tmpMaxAtomSize  = inputChunk.getMaxAtomSize()
        tmpEffAtomSize  = inputChunk.getMaxAtomSize(effectiveSize=True)
        tmpOutDiskSize  = taskSpec.getOutDiskSize()
        tmpWorkDiskSize = taskSpec.getWorkDiskSize()
        minDiskCountS = tmpOutDiskSize*tmpEffAtomSize + tmpWorkDiskSize + tmpMaxAtomSize
        minDiskCountS = minDiskCountS / 1024 / 1024
        # size for direct IO sites
        if taskSpec.useLocalIO():
            minDiskCountR = minDiskCountS
        else:
            minDiskCountR = tmpOutDiskSize*tmpEffAtomSize + tmpWorkDiskSize
            minDiskCountR = minDiskCountR / 1024 / 1024
        tmpLog.debug('maxAtomSize={0} effectiveAtomSize={1} outDiskCount={2} workDiskSize={3}'.format(tmpMaxAtomSize,
                                                                                                      tmpEffAtomSize,
                                                                                                      tmpOutDiskSize,
                                                                                                      tmpWorkDiskSize))
        tmpLog.debug('minDiskCountScratch={0} minDiskCountRemote={1}'.format(minDiskCountS,
                                                                             minDiskCountR))
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
            # send info to logger
            self.sendLogMessage(tmpLog)
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
            # check if blacklisted
            if self.ddmIF.isBlackListedEP(tmpSiteSpec.ddm):
                tmpLog.debug('  skip {0} since {1} is blacklisted in DDM'.format(tmpSiteName,tmpSiteSpec.ddm))
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed SE space check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for walltime
        minWalltime = taskSpec.walltime
        if not minWalltime in [0,None] and minWalltime > 0:
            minWalltime *= tmpEffAtomSize
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
                # send info to logger
                self.sendLogMessage(tmpLog)
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
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # check inclusion and exclusion
        newScanSiteList = []
        sitesForANY = []
        for tmpSiteName in scanSiteList:
            autoSite = False
            # check exclusion
            if AtlasBrokerUtils.isMatched(tmpSiteName,excludeList):
                tmpLog.debug('  skip {0} excluded'.format(tmpSiteName))
                continue
            # check inclusion
            if includeList != None and not AtlasBrokerUtils.isMatched(tmpSiteName,includeList):
                if 'AUTO' in includeList:
                    autoSite = True
                else:
                    tmpLog.debug('  skip {0} not included'.format(tmpSiteName))
                    continue
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # limited access
            if tmpSiteSpec.accesscontrol == 'grouplist':
                if not siteAccessMap.has_key(tmpSiteSpec.sitename) or \
                        siteAccessMap[tmpSiteSpec.sitename] != 'approved':
                    tmpLog.debug('  skip {0} limited access'.format(tmpSiteName))
                    continue
            # check cloud
            if not taskSpec.cloud in [None,'','any',tmpSiteSpec.cloud]: 
                tmpLog.debug('  skip {0} cloud missmatch'.format(tmpSiteName))
                continue
            if autoSite:
                sitesForANY.append(tmpSiteName)
            else:
                newScanSiteList.append(tmpSiteName)
        # use AUTO sites if no sites are included
        if newScanSiteList == []:
            newScanSiteList = sitesForANY
        else:
            for tmpSiteName in sitesForANY:
                tmpLog.debug('  skip {0} not included'.format(tmpSiteName))
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed inclusion/exclusion/cloud'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # sites already used by task
        tmpSt,sitesUsedByTask = self.taskBufferIF.getSitesUsedByTask_JEDI(taskSpec.jediTaskID)
        if not tmpSt:
            tmpLog.error('failed to get sites which already used by task')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # calculate weight
        fqans = taskSpec.makeFQANs()
        """
        tmpDm1,tmpDm2,tmpPriorityOffset,tmpSerNum,tmpWeight = self.taskBufferIF.getPrioParameters([],taskSpec.userName,fqans,
                                                                                                  taskSpec.workingGroup,True)
        currentPriority = PrioUtil.calculatePriority(tmpPriorityOffset,tmpSerNum,tmpWeight)
        currentPriority -= 500
        tmpLog.debug('currentPriority={0}'.format(currentPriority))
        """
        tmpSt,jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                    taskSpec.prodSourceLabel)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        # check for preassigned
        if sitePreAssigned and not taskSpec.site in scanSiteList:
            tmpLog.debug("preassigned site {0} didn't pass all tests".format(taskSpec.site))
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retFatal
        ######################################
        # final procedure
        tmpLog.debug('final {0} candidates'.format(len(scanSiteList)))
        weightMap = {}
        candidateSpecList = []
        preSiteCandidateSpec = None
        for tmpSiteName in scanSiteList:
            # get number of jobs in each job status. Using workQueueID=None to include non-JEDI jobs
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'running',  None,None)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'defined',  None,None)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',None,None) + \
                         AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'throttled',None,None)
            nStarting  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'starting', None,None)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + 1)
            nThrottled = 0
            if remoteSourceList.has_key(tmpSiteName):
                nThrottled = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'throttled',None,None)
                weight /= float(nThrottled + 1)
            # noramize weights by taking data availability into account
            tmpDataWeight = 1
            if dataWeight.has_key(tmpSiteName):
                weight = weight * dataWeight[tmpSiteName]
                tmpDataWeight = dataWeight[tmpSiteName]
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # preassigned
            if sitePreAssigned and tmpSiteName == taskSpec.site:
                preSiteCandidateSpec = siteCandidateSpec
            # set weight
            siteCandidateSpec.weight = weight
            tmpLog.debug('  site={0} nRun={1} nDef={2} nAct={3} nStart={4} nTr={5} dataW={6} W={7}'.format(tmpSiteName,
                                                                                                           nRunning,
                                                                                                           nAssigned,
                                                                                                           nActivated,
                                                                                                           nStarting,
                                                                                                           nThrottled,
                                                                                                           tmpDataWeight,
                                                                                                           weight))
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
        # append preassigned
        if sitePreAssigned and preSiteCandidateSpec != None and not preSiteCandidateSpec in candidateSpecList: 
            candidateSpecList.append(preSiteCandidateSpec)
        # collect site names
        scanSiteList = []    
        for siteCandidateSpec in candidateSpecList:
            scanSiteList.append(siteCandidateSpec.siteName)
        # get list of available files
        availableFileMap = {}     
        for datasetSpec in inputChunk.getDatasets():
            try:
                # get list of site to be scanned
                fileScanSiteList = []
                for tmpSiteName in scanSiteList:
                    fileScanSiteList.append(tmpSiteName)
                    if remoteSourceList.has_key(tmpSiteName) and remoteSourceList[tmpSiteName].has_key(datasetSpec.datasetName):
                        for tmpRemoteSite in remoteSourceList[tmpSiteName][datasetSpec.datasetName]:
                            if not tmpRemoteSite in fileScanSiteList:
                                fileScanSiteList.append(tmpRemoteSite)
                # mapping between sites and storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteStorageEndpointMap(fileScanSiteList,self.siteMapper)
                # disable file lookup for merge jobs
                if inputChunk.isMerging:
                    checkCompleteness = False
                else:
                    checkCompleteness = True
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            ngGroup=[2],
                                                            checkCompleteness=checkCompleteness)
                if tmpAvFileMap == None:
                    raise Interaction.JEDITemporaryError,'ddmIF.getAvailableFiles failed'
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
        # append candidates
        newScanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            tmpSiteName = siteCandidateSpec.siteName
            # preassigned
            if sitePreAssigned and tmpSiteName != taskSpec.site:
                tmpLog.debug('  skip {0} non pre-assigned site'.format(tmpSiteName))
                continue
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
                                len(tmpDatasetSpec.Files) <= len(availableFiles[tmpRemoteSite]['localdisk']):
                            # use only remote disk files
                            siteCandidateSpec.remoteFiles += availableFiles[tmpRemoteSite]['localdisk']
                            # set remote site and access protocol
                            siteCandidateSpec.remoteProtocol = allowedRemoteProtocol
                            siteCandidateSpec.remoteSource   = tmpRemoteSite
                            isAvailable = True
                            break
                # local files
                if availableFiles.has_key(tmpSiteName):
                    if len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['localdisk']) or \
                            len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['cache']) or \
                            len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['localtape']):
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['localdisk']
                        # add cached files to local list since cached files go to pending when reassigned
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.localTapeFiles  += availableFiles[tmpSiteName]['localtape']
                        siteCandidateSpec.cacheFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.remoteFiles += availableFiles[tmpSiteName]['remote']
                        isAvailable = True
                    else:
                        tmpMsg = '{0} is incompete at {1} : nFiles={2} nLocal={3} nCached={4} nTape={5}'
                        tmpLog.debug(tmpMsg.format(tmpDatasetName,
                                                   tmpSiteName,
                                                   len(tmpDatasetSpec.Files),
                                                   len(availableFiles[tmpSiteName]['localdisk']),
                                                   len(availableFiles[tmpSiteName]['cache']),
                                                   len(availableFiles[tmpSiteName]['localtape']),
                                                   ))
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
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retTmpError
        # send info to logger
        self.sendLogMessage(tmpLog)
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
    
