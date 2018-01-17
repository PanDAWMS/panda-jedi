import re
import sys
import datetime

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from JobBrokerBase import JobBrokerBase
import AtlasBrokerUtils
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import EventServiceUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandalogger import logger_config
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# definitions for network
AGIS_CLOSENESS = 'AGIS_closeness'
BLOCKED_LINK = -1
MIN_CLOSENESS = 0 # closeness goes from 0(best) to 11(worst)
MAX_CLOSENESS = 11
# NWS tags need to be prepended with activity
TRANSFERRED_1H = '_done_1h'
TRANSFERRED_6H = '_done_6h'
QUEUED = '_queued'
ZERO_TRANSFERS = 0.00001
URG_ACTIVITY = 'Express'
PRD_ACTIVITY = 'Production Output'
# NWS tags for FTS throughput
FTS_1H = 'fts_mbps_1h'
FTS_1D = 'fts_mbps_1d'
FTS_1W = 'fts_mbps_1w'
APP = 'jedi'
COMPONENT = 'jobbroker'
VO = 'atlas'

# brokerage for ATLAS production
class AtlasProdJobBroker (JobBrokerBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        JobBrokerBase.__init__(self,ddmIF, taskBufferIF)
        self.hospitalQueueMap = AtlasBrokerUtils.getHospitalQueues(self.siteMapper)
        self.dataSiteMap = {}
        self.suppressLogSending = False

        self.nwActive = taskBufferIF.getConfigValue(COMPONENT, 'NW_ACTIVE', APP, VO)
        if self.nwActive is None:
            self.nwActive = False

        self.nwQueueImportance = taskBufferIF.getConfigValue(COMPONENT, 'NW_QUEUE_IMPORTANCE', APP, VO)
        if self.nwQueueImportance is None:
            self.nwQueueImportance = 0.5
        self.nwThroughputImportance = 1 - self.nwQueueImportance

        self.nw_threshold = taskBufferIF.getConfigValue(COMPONENT, 'NW_THRESHOLD', APP, VO)
        if self.nw_threshold is None:
            self.nw_threshold = 1.7

        self.queue_threshold = taskBufferIF.getConfigValue(COMPONENT, 'NQUEUED_SAT_CAP', APP, VO)
        if self.queue_threshold is None:
            self.queue_threshold = 150

        self.nw_weight_multiplier = taskBufferIF.getConfigValue(COMPONENT, 'NW_WEIGHT_MULTIPLIER', APP, VO)
        if self.nw_weight_multiplier is None:
            self.nw_weight_multiplier = 1

    # wrapper for return
    def sendLogMessage(self,tmpLog):
        # log suppression
        if self.suppressLogSending:
            return
        # send info to logger
        tmpLog.bulkSendMsg('prod_brokerage')
        tmpLog.debug('sent')

       
    def convertMBpsToWeight(self, mbps):
        """
        Takes MBps value and converts to a weight between 1 and 2
        """
        mbps_thresholds = [200, 100, 75, 50, 20, 10, 2, 1]
        weights = [2, 1.9, 1.8, 1.6, 1.5, 1.3, 1.2, 1.1]

        for threshold, weight in zip(mbps_thresholds, weights):
            if mbps > threshold:
                return weight
        return 1


    # get all T1 sites
    def getAllT1Sites(self):
        cloudList = self.siteMapper.getCloudList()
        t1Sites = set()
        for cloudName in cloudList:
            # T1
            t1SiteName = self.siteMapper.getCloud(cloudName)['source']
            t1Sites.add(t1SiteName)
            # hospital
            if self.hospitalQueueMap.has_key(cloudName):
                for tmpSiteName in self.hospitalQueueMap[cloudName]:
                    t1Sites.add(tmpSiteName)
        return list(t1Sites)


    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap, hintForTB=False, siteListForTB=None, glLog=None):
        # suppress sending log
        if hintForTB:
            self.suppressLogSending = True
        # make logger
        if glLog == None:
            tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID),
                                monToken='<jediTaskID={0} {1}>'.format(taskSpec.jediTaskID,
                                                                       datetime.datetime.utcnow().isoformat('/')))
        else:
            tmpLog = glLog

        if not hintForTB:
            tmpLog.debug('start')

        if self.nwActive:
            tmpLog.debug('Network weights are ACTIVE!')
        else:
            tmpLog.debug('Network weights are PASSIVE!')

        timeNow = datetime.datetime.utcnow()
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # get sites in the cloud
        sitePreAssigned = False
        siteListPreAssigned = False
        if siteListForTB != None:
            scanSiteList = siteListForTB
        elif not taskSpec.site in ['',None] and inputChunk.getPreassignedSite() == None:
            if ',' in taskSpec.site:
                # site list
                siteListPreAssigned = True
                scanSiteList = taskSpec.site.split(',')
            else:
                # site
                sitePreAssigned = True
                scanSiteList = [taskSpec.site]
            tmpLog.info('site={0} is pre-assigned criteria=+preassign'.format(taskSpec.site))
        elif inputChunk.getPreassignedSite() != None:
            if inputChunk.masterDataset.creationTime != None and inputChunk.masterDataset.modificationTime != None and \
                    inputChunk.masterDataset.modificationTime != inputChunk.masterDataset.creationTime and \
                    timeNow-inputChunk.masterDataset.modificationTime > datetime.timedelta(hours=24) and \
                    taskSpec.frozenTime != None and timeNow-taskSpec.frozenTime > datetime.timedelta(hours=6):
                # ignore pre-assigned site since pmerge is timed out
                tmpLog.info('ignore pre-assigned for pmerge due to timeout')
                scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
                tmpLog.info('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
            else:
                # pmerge
                siteListPreAssigned = True
                scanSiteList = DataServiceUtils.getSitesShareDDM(self.siteMapper,inputChunk.getPreassignedSite())
                scanSiteList.append(inputChunk.getPreassignedSite())
                tmpMsg = 'use site={0} since they share DDM endpoints with orinal_site={1} which is pre-assigned in masterDS '.format(str(scanSiteList),
                                                                                                                                      inputChunk.getPreassignedSite())
                tmpMsg += 'criteria=+premerge'
                tmpLog.info(tmpMsg)
        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
            tmpLog.info('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))

        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        if not tmpSt:
            tmpLog.error('failed to get job statistics')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        # T1 
        if not taskSpec.useWorldCloud():
            t1Sites = [self.siteMapper.getCloud(cloudName)['source']]
            # hospital sites
            if self.hospitalQueueMap.has_key(cloudName):
                t1Sites += self.hospitalQueueMap[cloudName]
        else:
            # get destination for WORLD cloud
            if not hintForTB:
                # get nucleus
                nucleusSpec = self.siteMapper.getNucleus(taskSpec.nucleus)
                if nucleusSpec == None:
                    tmpLog.error('unknown nucleus {0}'.format(taskSpec.nucleus))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    self.sendLogMessage(tmpLog)
                    return retTmpError
                t1Sites = []
                t1Sites = nucleusSpec.allPandaSites
            else:
                # use all sites in nuclei for WORLD task brokerage
                t1Sites = []
                for tmpNucleus in self.siteMapper.nuclei.values():
                    t1Sites += tmpNucleus.allPandaSites

        # sites sharing SE with T1
        if len(t1Sites) > 0:
            sitesShareSeT1 = DataServiceUtils.getSitesShareDDM(self.siteMapper,t1Sites[0])
        else:
            sitesShareSeT1 = []
        # all T1
        allT1Sites = self.getAllT1Sites()
        # core count
        if inputChunk.isMerging and taskSpec.mergeCoreCount != None:
            taskCoreCount = taskSpec.mergeCoreCount
        else:
            taskCoreCount = taskSpec.coreCount
        # MP
        if taskCoreCount != None and taskCoreCount > 1:
            # use MCORE only
            useMP = 'only'
        elif taskCoreCount == 0:
            # use MCORE and normal 
            useMP = 'any'
        else:
            # not use MCORE
            useMP = 'unuse'
        # get workQueue
        workQueue = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
        if workQueue.is_global_share:
            wq_tag = workQueue.queue_name
        else:
            wq_tag = workQueue.queue_id

        ######################################
        # selection for status
        if not sitePreAssigned:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # skip unified queues
                if tmpSiteSpec.is_unified:
                    tmpLog.info('  skip site=%s due to is_unified=%s criteria=-unified' % (tmpSiteName,tmpSiteSpec.is_unified))
                    continue
                # check site status
                skipFlag = False
                if tmpSiteSpec.status not in ['online', 'standby']:
                    skipFlag = True
                if not skipFlag:    
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info('  skip site=%s due to status=%s criteria=-status' % (tmpSiteName,tmpSiteSpec.status))
            scanSiteList = newScanSiteList        
            tmpLog.info('{0} candidates passed site status check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError

        #################################################
        # WORLD CLOUD: get the nucleus and the network map
        nucleus = taskSpec.nucleus
        storageMapping = self.taskBufferIF.getPandaSiteToOutputStorageSiteMapping()

        if taskSpec.useWorldCloud() and nucleus:
            # get connectivity stats to the nucleus in case of WORLD cloud
            if inputChunk.isExpress():
                transferred_tag = '{0}{1}'.format(URG_ACTIVITY, TRANSFERRED_6H)
                queued_tag = '{0}{1}'.format(URG_ACTIVITY, QUEUED)
            else:
                transferred_tag = '{0}{1}'.format(PRD_ACTIVITY, TRANSFERRED_6H)
                queued_tag = '{0}{1}'.format(PRD_ACTIVITY, QUEUED)

            networkMap = self.taskBufferIF.getNetworkMetrics(nucleus,
                                                             [AGIS_CLOSENESS, transferred_tag, queued_tag,
                                                              FTS_1H, FTS_1D, FTS_1W])

        #####################################################
        # filtering out blacklisted or links with long queues
        if taskSpec.useWorldCloud() and nucleus and not sitePreAssigned and not siteListPreAssigned:
            newScanSiteList = []
            for tmpPandaSiteName in self.get_unified_sites(scanSiteList):
                try:
                    tmpAtlasSiteName = storageMapping[tmpPandaSiteName]
                    if nucleus == tmpAtlasSiteName or \
                                    (networkMap[tmpAtlasSiteName][AGIS_CLOSENESS] != BLOCKED_LINK and \
                                    networkMap[tmpAtlasSiteName][queued_tag] < self.queue_threshold):
                        newScanSiteList.append(tmpPandaSiteName)
                    else:
                        if networkMap[tmpAtlasSiteName][AGIS_CLOSENESS] == BLOCKED_LINK:
                            reason = 'agis_closeness={0}'.format(networkMap[tmpAtlasSiteName][AGIS_CLOSENESS])
                        elif networkMap[tmpAtlasSiteName][queued_tag] >= self.queue_threshold:
                            reason = 'too many output files queued {0}(>{1} link limit)'\
                                .format(networkMap[tmpAtlasSiteName][queued_tag], self.queue_threshold)
                        else:
                            reason = 'reason unknown'
                        tmpLog.info('  skip site={0} due to {1}, from satellite={2} to nucleus={3}: criteria=-link_unusable'
                        .format(tmpPandaSiteName, reason, tmpAtlasSiteName, nucleus))
                except KeyError:
                    # Don't skip missing links for the moment. In later stages missing links
                    # default to the worst connectivity and will be penalized.
                    newScanSiteList.append(tmpPandaSiteName)

            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed site status check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for high priorities
        t1WeightForHighPrio = 1
        if (taskSpec.currentPriority >= 800 or inputChunk.useScout()) \
                and not sitePreAssigned and not siteListPreAssigned:
            if not taskSpec.useEventService():
                if taskSpec.currentPriority >= 900 or inputChunk.useScout():
                    t1WeightForHighPrio = 100
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if not tmpSiteSpec.is_opportunistic():
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpMsg = '  skip site={0} to avoid opportunistic for high priority jobs '.format(tmpSiteName)
                    tmpMsg += 'criteria=-opportunistic'
                    tmpLog.info(tmpMsg)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed for opportunistic check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection to avoid slow or inactive sites
        if (taskSpec.currentPriority >= 800 or inputChunk.useScout() or \
                inputChunk.isMerging or taskSpec.mergeOutput()) \
                and not sitePreAssigned:
            # get inactive sites
            inactiveTimeLimit = 2
            inactiveSites = self.taskBufferIF.getInactiveSites_JEDI('production',inactiveTimeLimit)
            newScanSiteList = []
            tmpMsgList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                nToGetAll = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteSpec.get_unified_name(),'activated') + \
                    AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteSpec.get_unified_name(),'starting')
                if tmpSiteName in ['BNL_CLOUD','BNL_CLOUD_MCORE','ATLAS_OPP_OSG']:
                    tmpMsg = '  skip site={0} since high prio/scouts/merge needs to avoid slow sites '.format(tmpSiteName)
                    tmpMsg += 'criteria=-slow'
                    tmpMsgList.append(tmpMsg)
                elif tmpSiteName in inactiveSites and nToGetAll > 0:
                    tmpMsg = '  skip site={0} since high prio/scouts/merge needs to avoid inactive sites (laststart is older than {1}h) '.format(tmpSiteName,
                                                                                                                                                 inactiveTimeLimit)
                    tmpMsg += 'criteria=-inactive'
                    tmpMsgList.append(tmpMsg)
                else:
                    newScanSiteList.append(tmpSiteName)
            if newScanSiteList != []:
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                for tmpMsg in tmpMsgList:
                    tmpLog.info(tmpMsg)
            tmpLog.info('{0} candidates passed for slowness/inactive check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for data availability
        """
        if not sitePreAssigned and not siteListPreAssigned:
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                # ignore DBR
                if DataServiceUtils.isDBR(datasetName):
                    continue
                if not self.dataSiteMap.has_key(datasetName):
                    # get the list of sites where data is available
                    tmpLog.info('getting the list of sites where {0} is available'.format(datasetName))
                    tmpSt,tmpRet = AtlasBrokerUtils.getSitesWithData(self.siteMapper,
                                                                     self.ddmIF,datasetName,
                                                                     datasetSpec.storageToken)
                    if tmpSt == self.SC_FAILED:
                        tmpLog.error('failed to get the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        self.sendLogMessage(tmpLog)
                        return retTmpError
                    if tmpSt == self.SC_FATAL:
                        tmpLog.error('fatal error when getting the list of sites where data is available, since %s' % tmpRet)
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        self.sendLogMessage(tmpLog)
                        return retFatal
                    # append
                    self.dataSiteMap[datasetName] = tmpRet
                    tmpLog.debug('map of data availability : {0}'.format(str(tmpRet)))
                # check if T1 has the data
                if self.dataSiteMap[datasetName].has_key(cloudName):
                    cloudHasData = True
                else:
                    cloudHasData = False
                t1hasData = False
                if cloudHasData:
                    for tmpSE,tmpSeVal in self.dataSiteMap[datasetName][cloudName]['t1'].iteritems():
                        if tmpSeVal['state'] == 'complete':
                            t1hasData = True
                            break
                    # T1 has incomplete data while no data at T2
                    if not t1hasData and self.dataSiteMap[datasetName][cloudName]['t2'] == []:
                        # use incomplete data at T1 anyway
                        t1hasData = True
                # data is missing at T1         
                if not t1hasData:
                    tmpLog.info('{0} is unavailable at T1. scanning T2 sites in homeCloud={1}'.format(datasetName,cloudName))
                    # make subscription to T1
                    # FIXME
                    pass
                    # use T2 until data is complete at T1
                    newScanSiteList = []
                    for tmpSiteName in scanSiteList:                    
                        if cloudHasData and tmpSiteName in self.dataSiteMap[datasetName][cloudName]['t2']:
                            newScanSiteList.append(tmpSiteName)
                        else:
                            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                            if tmpSiteSpec.cloud != cloudName:
                                tmpLog.info('  skip %s due to foreign T2' % tmpSiteName)
                            else:
                                tmpLog.info('  skip %s due to missing data at T2' % tmpSiteName)
                    scanSiteList = newScanSiteList
                    tmpLog.info('{0} candidates passed T2 scan in the home cloud with input:{1}'.format(len(scanSiteList),datasetName))
                    if scanSiteList == []:
                        tmpLog.error('no candidates')
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retTmpError
        """        
        ######################################
        # selection for fairshare
        if not sitePreAssigned and taskSpec.prodSourceLabel in ['managed'] or not workQueue.queue_name in ['test','validation']:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if AtlasBrokerUtils.hasZeroShare(tmpSiteSpec,taskSpec,inputChunk.isMerging,tmpLog):
                    tmpLog.info('  skip site={0} due to zero share criteria=-zeroshare'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)                
            scanSiteList = newScanSiteList        
            tmpLog.info('{0} candidates passed zero share check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for jumbo jobs
        if not sitePreAssigned and taskSpec.getNumJumboJobs() is None:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.useJumboJobs():
                    tmpLog.info('  skip site={0} since it is only for jumbo jobs criteria=-jumbo'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)                
            scanSiteList = newScanSiteList        
            tmpLog.info('{0} candidates passed jumbo job check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for I/O intensive tasks
        # FIXME
        pass
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
                    tmpLog.info('  skip site=%s due to core mismatch site:%s <> task:%s criteria=-cpucore' % \
                                 (tmpSiteName,tmpSiteSpec.coreCount,taskCoreCount))
            scanSiteList = newScanSiteList        
            tmpLog.info('{0} candidates passed for useMP={1}'.format(len(scanSiteList),useMP))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for release
        if taskSpec.transHome != None:
            unified_site_list = self.get_unified_sites(scanSiteList)
            if re.search('-\d+\.\d+\.\d+$',taskSpec.transHome) is not None:
                # 3 digits base release
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                                                                         releases=taskSpec.transHome.split('-')[-1],
                                                                         cmtConfig=taskSpec.architecture)
                siteListWithSW += self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                                                                          caches=taskSpec.transHome,
                                                                          cmtConfig=taskSpec.architecture)
            elif re.search('rel_\d+(\n|$)',taskSpec.transHome) is None and \
                    re.search('\d{4}-\d{2}-\d{2}T\d{4}$',taskSpec.transHome) is None:
                # only cache is checked for normal tasks
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                                                                         caches=taskSpec.transHome,
                                                                         cmtConfig=taskSpec.architecture)
            else:
                # nightlies
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                                                                         releases='CVMFS')
                #                                                         releases='nightlies',
                #                                                         cmtConfig=taskSpec.architecture)
            newScanSiteList = []
            for tmpSiteName in unified_site_list:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # release check is disabled or release is available
                if tmpSiteSpec.releases == ['ANY'] or \
                   tmpSiteName in ['CERN-RELEASE']:
                    newScanSiteList.append(tmpSiteName)
                elif tmpSiteName in siteListWithSW:
                    newScanSiteList.append(tmpSiteName)
                else:
                    # release is unavailable
                    tmpLog.info('  skip site=%s due to missing cache=%s:%s criteria=-cache' % \
                                 (tmpSiteName,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed for ATLAS release {1}:{2}'.format(len(scanSiteList),
                                                                                  taskSpec.transHome,
                                                                                  taskSpec.architecture))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for memory
        origMinRamCount = inputChunk.getMaxRamCount()
        if not origMinRamCount in [0,None]:
            if inputChunk.isMerging:
                strMinRamCount = '{0}(MB)'.format(origMinRamCount)
            else:
                strMinRamCount = '{0}({1})'.format(origMinRamCount,taskSpec.ramUnit)
            if not inputChunk.isMerging and not taskSpec.baseRamCount in [0,None]:
                strMinRamCount += '+{0}'.format(taskSpec.baseRamCount)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement 
                minRamCount = origMinRamCount
                if taskSpec.ramPerCore() and not inputChunk.isMerging:
                    if not tmpSiteSpec.coreCount in [None,0]:
                        minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    minRamCount += taskSpec.baseRamCount
                # compensate
                minRamCount = JediCoreUtils.compensateRamCount(minRamCount)
                # site max memory requirement
                if not tmpSiteSpec.maxrss in [0,None]:
                    site_maxmemory = tmpSiteSpec.maxrss
                else:
                    site_maxmemory = tmpSiteSpec.maxmemory
                # check at the site
                if not site_maxmemory in [0,None] and minRamCount != 0 and minRamCount > site_maxmemory:
                    tmpMsg = '  skip site={0} due to site RAM shortage {1}(site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                    site_maxmemory,
                                                                                                                    minRamCount)
                    tmpMsg += 'criteria=-lowmemory'
                    tmpLog.info(tmpMsg)
                    continue
                # site min memory requirement
                if not tmpSiteSpec.minrss in [0,None]:
                    site_minmemory = tmpSiteSpec.minrss
                else:
                    site_minmemory = tmpSiteSpec.minmemory
                if not site_minmemory in [0,None] and minRamCount != 0 and minRamCount < site_minmemory:
                    tmpMsg = '  skip site={0} due to job RAM shortage {1}(site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                      site_minmemory,
                                                                                                                      minRamCount)
                    tmpMsg += 'criteria=-highmemory'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.info('{0} candidates passed memory check {1}'.format(len(scanSiteList),strMinRamCount))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for scratch disk
        if taskSpec.outputScaleWithEvents():
            minDiskCount = taskSpec.getOutDiskSize()*inputChunk.getMaxAtomSize(getNumEvents=True)
        else:
            minDiskCount = taskSpec.getOutDiskSize()*inputChunk.getMaxAtomSize(effectiveSize=True)
        minDiskCount  += taskSpec.getWorkDiskSize()
        minDiskCountL  = minDiskCount
        minDiskCountD  = minDiskCount 
        minDiskCountL += inputChunk.getMaxAtomSize()
        minDiskCountL  = minDiskCountL / 1024 / 1024
        minDiskCountD  = minDiskCountD / 1024 / 1024
        newScanSiteList = []
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check remote access
            if taskSpec.allowInputLAN() == 'only' and not tmpSiteSpec.direct_access_lan:
                tmpMsg = '  skip site={0} since remote IO is disabled '.format(tmpSiteName)
                tmpMsg += 'criteria=-remoteio'
                tmpLog.info(tmpMsg)
                continue
            # check scratch size
            if tmpSiteSpec.maxwdir != 0:
                if taskSpec.allowInputLAN() != None and tmpSiteSpec.direct_access_lan:
                    # size for remote access
                    minDiskCount = minDiskCountD
                else:
                    # size for copy-to-scratch
                    minDiskCount = minDiskCountL
                if minDiskCount > tmpSiteSpec.maxwdir:
                    tmpMsg = '  skip site={0} due to small scratch disk {1} less than {2} '.format(tmpSiteName,
                                                                                                   tmpSiteSpec.maxwdir,
                                                                                                   minDiskCount)
                    tmpMsg += 'criteria=-disk'
                    tmpLog.info(tmpMsg)
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed scratch disk check minDiskCount>{1}MB'.format(len(scanSiteList),
                                                                                          minDiskCount))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for available space in SE
        newScanSiteList = []
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # don't check for T1
            if tmpSiteName in t1Sites:
                pass
            else:
                # check endpoint
                tmpEndPoint = tmpSiteSpec.ddm_endpoints_output.getEndPoint(tmpSiteSpec.ddm_output)
                if tmpEndPoint != None:
                    # check free size
                    tmpSpaceSize = 0
                    if tmpEndPoint['space_free'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_free']
                    if tmpEndPoint['space_expired'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_expired']
                    diskThreshold = 200
                    if tmpSpaceSize < diskThreshold:
                        tmpLog.info('  skip site={0} due to disk shortage at {1} {2}GB < {3}GB criteria=-disk'.format(tmpSiteName, tmpSiteSpec.ddm_output,
                                                                                                                    tmpSpaceSize, diskThreshold))
                        continue
                    # check if blacklisted
                    if tmpEndPoint['blacklisted'] == 'Y':
                        tmpLog.info('  skip site={0} since endpoint={1} is blacklisted in DDM criteria=-blacklist'.format(tmpSiteName, tmpSiteSpec.ddm_output))
                        continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed SE space check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for walltime
        if taskSpec.useEventService() and not taskSpec.useJobCloning():
            nEsConsumers = taskSpec.getNumEventServiceConsumer()
            if nEsConsumers is None:
                nEsConsumers = 1
            maxAttemptEsJob = taskSpec.getMaxAttemptEsJob()
            if maxAttemptEsJob is None:
                maxAttemptEsJob = EventServiceUtils.defMaxAttemptEsJob
        else:
            nEsConsumers = 1
            maxAttemptEsJob = 1
        if not taskSpec.useHS06():
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
            if taskSpec.walltime != None:
                minWalltime = taskSpec.walltime * tmpMaxAtomSize
            else:
                minWalltime = None
            # take # of consumers into account
            if not taskSpec.useEventService() or taskSpec.useJobCloning():
                strMinWalltime = 'walltime*inputSize={0}*{1}'.format(taskSpec.walltime,tmpMaxAtomSize)
            else:
                strMinWalltime = 'walltime*inputSize/nEsConsumers/maxAttemptEsJob={0}*{1}/{2}/{3}'.format(taskSpec.walltime,
                                                                                                          tmpMaxAtomSize,
                                                                                                          nEsConsumers,
                                                                                                          maxAttemptEsJob)
        else:
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(getNumEvents=True)
            if taskSpec.cpuTime != None:
                minWalltime = taskSpec.cpuTime * tmpMaxAtomSize
            else:
                minWalltime = None
            # take # of consumers into account
            if not taskSpec.useEventService() or taskSpec.useJobCloning():
                strMinWalltime = 'cpuTime*nEventsPerJob={0}*{1}'.format(taskSpec.cpuTime,tmpMaxAtomSize)
            else:
                strMinWalltime = 'cpuTime*nEventsPerJob/nEsConsumers/maxAttemptEsJob={0}*{1}/{2}/{3}'.format(taskSpec.cpuTime,
                                                                                                             tmpMaxAtomSize,
                                                                                                             nEsConsumers,
                                                                                                             maxAttemptEsJob)
        if minWalltime != None:
            minWalltime /= (nEsConsumers * maxAttemptEsJob)
        if minWalltime != None or inputChunk.useScout():
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                siteMaxTime = tmpSiteSpec.maxtime
                origSiteMaxTime = siteMaxTime
                # sending scouts or merge or wallime-undefined jobs to only sites where walltime is more than 1 day
                if (not sitePreAssigned and inputChunk.useScout()) or inputChunk.isMerging or \
                        (taskSpec.walltime in [0,None] and taskSpec.walltimeUnit in ['',None] and taskSpec.cpuTimeUnit in ['',None]):
                    minTimeForZeroWalltime = 24*60*60
                    if siteMaxTime != 0 and siteMaxTime < minTimeForZeroWalltime:
                        tmpMsg = '  skip site={0} due to site walltime {1} (site upper limit) insufficient '.format(tmpSiteName,
                                                                                                                    siteMaxTime)
                        if inputChunk.useScout():
                            tmpMsg += 'for scouts ({0} at least) '.format(minTimeForZeroWalltime)
                            tmpMsg += 'criteria=-scoutwalltime'
                        else:
                            tmpMsg += 'for zero walltime ({0} at least) '.format(minTimeForZeroWalltime)
                            tmpMsg += 'criteria=-zerowalltime'
                        tmpLog.info(tmpMsg)
                        continue
                # check max walltime at the site
                tmpSiteStr = '{0}'.format(siteMaxTime)
                if taskSpec.useHS06():
                    oldSiteMaxTime = siteMaxTime
                    siteMaxTime -= taskSpec.baseWalltime
                    tmpSiteStr = '({0}-{1})'.format(oldSiteMaxTime,taskSpec.baseWalltime)
                if not siteMaxTime in [None,0] and not tmpSiteSpec.coreCount in [None,0]:
                    siteMaxTime *= tmpSiteSpec.coreCount
                    tmpSiteStr += '*{0}'.format(tmpSiteSpec.coreCount)
                if taskSpec.useHS06():
                    if not siteMaxTime in [None,0] and not tmpSiteSpec.corepower in [None,0]:
                        siteMaxTime *= tmpSiteSpec.corepower
                        tmpSiteStr += '*{0}'.format(tmpSiteSpec.corepower)
                    siteMaxTime *= float(taskSpec.cpuEfficiency) / 100.0
                    siteMaxTime = long(siteMaxTime)
                    tmpSiteStr += '*{0}%'.format(taskSpec.cpuEfficiency)
                if origSiteMaxTime != 0 and minWalltime > siteMaxTime:
                    tmpMsg = '  skip site={0} due to short site walltime {1} (site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                       tmpSiteStr,
                                                                                                                       strMinWalltime)
                    tmpMsg += 'criteria=-shortwalltime'
                    tmpLog.info(tmpMsg)
                    continue
                # check min walltime at the site
                siteMinTime = tmpSiteSpec.mintime
                origSiteMinTime = siteMinTime
                tmpSiteStr = '{0}'.format(siteMinTime)
                if taskSpec.useHS06():
                    oldSiteMinTime = siteMinTime
                    siteMinTime -= taskSpec.baseWalltime
                    tmpSiteStr = '({0}-{1})'.format(oldSiteMinTime,taskSpec.baseWalltime)
                if not siteMinTime in [None,0] and not tmpSiteSpec.coreCount in [None,0]:
                    siteMinTime *= tmpSiteSpec.coreCount
                    tmpSiteStr += '*{0}'.format(tmpSiteSpec.coreCount)
                if taskSpec.useHS06():
                    if not siteMinTime in [None,0] and not tmpSiteSpec.corepower in [None,0]:
                        siteMinTime *= tmpSiteSpec.corepower
                        tmpSiteStr += '*{0}'.format(tmpSiteSpec.corepower)
                    siteMinTime *= float(taskSpec.cpuEfficiency) / 100.0
                    siteMinTime = long(siteMinTime)
                    tmpSiteStr += '*{0}%'.format(taskSpec.cpuEfficiency)
                if origSiteMinTime != 0 and minWalltime < siteMinTime:
                    tmpMsg = '  skip site {0} due to short job walltime {1} (site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                         tmpSiteStr,
                                                                                                                         strMinWalltime)
                    tmpMsg += 'criteria=-longwalltime'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            if not taskSpec.useHS06():
                tmpLog.info('{0} candidates passed walltime check {1}({2})'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
            else:
                tmpLog.info('{0} candidates passed walltime check {1}({2}*nEventsPerJob)'.format(len(scanSiteList),strMinWalltime,taskSpec.cpuTimeUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for network connectivity
        if not sitePreAssigned:
            ipConnectivity = taskSpec.getIpConnectivity()
            if ipConnectivity != None:
                newScanSiteList = []
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # check at the site
                    if tmpSiteSpec.wnconnectivity == 'full':
                        pass
                    elif tmpSiteSpec.wnconnectivity == 'http' and ipConnectivity == 'http':
                        pass
                    else:
                        tmpMsg = '  skip site={0} due to insufficient connectivity (site={1}) for task={2} '.format(tmpSiteName,
                                                                                                                    tmpSiteSpec.wnconnectivity,
                                                                                                                    ipConnectivity)
                        tmpMsg += 'criteria=-network'
                        tmpLog.info(tmpMsg)
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed network check ({1})'.format(len(scanSiteList),
                                                                                ipConnectivity))
                if scanSiteList == []:
                    tmpLog.error('no candidates')
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    self.sendLogMessage(tmpLog)
                    return retTmpError
        ######################################
        # selection for event service
        if not sitePreAssigned:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # event service
                if taskSpec.useEventService() and not taskSpec.useJobCloning():
                    if tmpSiteSpec.getJobSeed() == 'std':
                        tmpMsg = '  skip site={0} since EventService is not allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-es'
                        tmpLog.info(tmpMsg)
                        continue
                else:
                    if tmpSiteSpec.getJobSeed() == 'es':
                        tmpMsg = '  skip site={0} since only EventService is allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-nones'
                        tmpLog.info(tmpMsg)
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed EventService check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for dynamic number of events
        if not sitePreAssigned and taskSpec.dynamicNumEvents():
            newScanSiteList = []
            minGranularity = taskSpec.get_min_granularity()
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                nSimEvents = tmpSiteSpec.get_n_sim_events()
                # check nSimEvents
                if nSimEvents is None:
                    tmpMsg = '  skip site={0} since not allowed to run jobs with dynamic number of events '.format(tmpSiteName)
                    tmpMsg += 'criteria=-dyn'
                    tmpLog.info(tmpMsg)
                    continue
                elif nSimEvents < minGranularity:
                    tmpMsg = '  skip site={0} since num of simulated events {1} is less than min granuality {2} '.format(tmpSiteName,
                                                                                                                         nSimEvents,
                                                                                                                         minGranularity)
                    tmpMsg += 'criteria=-dyn'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed DynNumEvents check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for transferring
        newScanSiteList = []
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            if not tmpSiteName in t1Sites+sitesShareSeT1:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # limit
                def_maxTransferring = 2000 
                if tmpSiteSpec.transferringlimit == 0:
                    # use default value
                    maxTransferring   = def_maxTransferring
                else:
                    maxTransferring = tmpSiteSpec.transferringlimit
                # check at the site
                nTraJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'transferring')
                nRunJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'running')
                if max(maxTransferring,2*nRunJobs) < nTraJobs and not tmpSiteSpec.cloud in ['ND']:
                    tmpLog.info('  skip site=%s due to too many transferring=%s greater than max(%s,2x%s) criteria=-transferring' % \
                                     (tmpSiteName,nTraJobs,def_maxTransferring,nRunJobs))
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed transferring check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for T1 weight
        t1Weight = taskSpec.getT1Weight()
        if t1Weight == 0:
            tmpLog.info('IO intensity {0}'.format(taskSpec.ioIntensity))
            # use T1 weight in cloudconfig if IO intensive
            if taskSpec.ioIntensity != None and taskSpec.ioIntensity > 500:
                t1Weight = self.siteMapper.getCloud(cloudName)['weight']
            else:
                t1Weight = 1
        if t1Weight < 0 and not inputChunk.isMerging:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if not tmpSiteName in t1Sites:
                    tmpLog.info('  skip site={0} due to negative T1 weight criteria=-t1weight'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            t1Weight = 1
        t1Weight = max(t1Weight,t1WeightForHighPrio)
        tmpLog.info('T1 weight {0}'.format(t1Weight))
        tmpLog.info('{0} candidates passed T1 weight check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for nPilot
        nPilotMap = {}
        if not sitePreAssigned:
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            newScanSiteList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                nPilot = 0
                if nWNmap.has_key(tmpSiteName):
                    nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
                # skip no pilot sites unless the task and the site use jumbo jobs or the site is standby
                if nPilot == 0 and not 'test' in taskSpec.prodSourceLabel and \
                        (taskSpec.getNumJumboJobs() == None or not tmpSiteSpec.useJumboJobs()) and \
                        tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type) is None:
                    tmpLog.info('  skip site=%s due to no pilot criteria=-nopilot' % tmpSiteName)
                    continue
                newScanSiteList.append(tmpSiteName)
                nPilotMap[tmpSiteName] = nPilot
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        # return if to give a hint for task brokerage
        if hintForTB:
            return self.SC_SUCCEEDED,scanSiteList
        ######################################
        # get available files
        siteSizeMap = {}        
        availableFileMap = {}
        siteFilesMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(self.get_unified_sites(scanSiteList),
                                                                                self.siteMapper, ignore_cc=True)
                # disable file lookup for merge jobs or secondary datasets
                checkCompleteness = True
                useCompleteOnly = False
                if inputChunk.isMerging:
                    checkCompleteness = False
                if not datasetSpec.isMaster():
                    useCompleteOnly = True
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            check_completeness=checkCompleteness,
                                                            storage_token=datasetSpec.storageToken,
                                                            complete_only=useCompleteOnly)
                if tmpAvFileMap == None:
                    raise Interaction.JEDITemporaryError,'ddmIF.getAvailableFiles failed'
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
            # loop over all sites to get the size of available files
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                if not siteSizeMap.has_key(tmpSiteName):
                    siteSizeMap[tmpSiteName] = 0
                if not tmpSiteName in siteFilesMap:
                    siteFilesMap[tmpSiteName] = set()
                # get the total size of available files
                if availableFileMap[datasetSpec.datasetName].has_key(tmpSiteName):
                    availableFiles = availableFileMap[datasetSpec.datasetName][tmpSiteName]
                    for tmpFileSpec in \
                            availableFiles['localdisk']+availableFiles['cache']:
                        if not tmpFileSpec.lfn in siteFilesMap[tmpSiteName]:
                            siteSizeMap[tmpSiteName] += tmpFileSpec.fsize
                        siteFilesMap[tmpSiteName].add(tmpFileSpec.lfn)
        # get max total size
        tmpTotalSizes = siteSizeMap.values()
        tmpTotalSizes.sort()
        if tmpTotalSizes != []:
            totalSize = tmpTotalSizes.pop()
        else:
            totalSize = 0
        # get max num of available files
        maxNumFiles = 0
        for tmpSiteName,tmpSet in siteFilesMap.iteritems():
            if maxNumFiles < len(tmpSet):
                maxNumFiles = len(tmpSet)
        ######################################
        # selection for fileSizeToMove
        ioIntensityCutoff = 200
        moveSizeCutoffGB = 10
        if not sitePreAssigned and totalSize > 0 and not inputChunk.isMerging and taskSpec.ioIntensity is not None and taskSpec.ioIntensity > ioIntensityCutoff \
                and not (taskSpec.useEventService() and not taskSpec.useJobCloning()):
            newScanSiteList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # file size to move in MB
                mbToMove = long((totalSize-siteSizeMap[tmpSiteName])/(1024*1024))
                if mbToMove > moveSizeCutoffGB * 1024:
                    tmpMsg = '  skip site={0} since size of missing input is too large ({1} GB > {2} GB) '.format(tmpSiteName,
                                                                                                                  long(mbToMove/1024),
                                                                                                                  moveSizeCutoffGB)
                    tmpMsg += 'for IO intensive task ({0} > {1} kBPerS) '.format(taskSpec.ioIntensity,ioIntensityCutoff)
                    tmpMsg += 'criteria=-io'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed IO check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # calculate weight
        if workQueue.is_global_share:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        else:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                         taskSpec.prodSourceLabel)

        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        tmpLog.info('calculate weight and check cap for {0} candidates'.format(len(scanSiteList)))
        weightMapPrimary = {}
        weightMapSecondary = {}
        weightMapJumbo = {}
        newScanSiteList = []
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running', None, wq_tag)
            nDefined   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'defined', None, wq_tag) + self.getLiveCount(tmpSiteName)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'assigned', None, wq_tag)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'activated', None, wq_tag) + \
                         AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'throttled', None, wq_tag)
            nStarting  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'starting', None, wq_tag)
            if tmpSiteName in nPilotMap:
                nPilot = nPilotMap[tmpSiteName]
            else:
                nPilot = 0
            # take into account the number of standby jobs
            numStandby = tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type)
            if numStandby is None:
                pass
            elif numStandby == 0:
                # use the number of starting jobs as the number of standby jobs
                nRunning = nStarting
                nStarting = 0
            else:
                # the number of standby jobs is defined
                nRunning = numStandby
            manyAssigned = float(nAssigned + 1) / float(nActivated + 1)
            manyAssigned = min(2.0,manyAssigned)
            manyAssigned = max(1.0,manyAssigned)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + nDefined + 1) / manyAssigned
            weightStr = 'nRun={0} nAct={1} nAss={2} nStart={3} nDef={4} manyAss={6} nPilot={7} totalSizeMB={5} '.format(nRunning,nActivated,nAssigned,
                                                                                                                        nStarting,nDefined,
                                                                                                                        long(totalSize/1024/1024),
                                                                                                                        manyAssigned,nPilot)
            # reduce weights by taking data availability into account
            if totalSize > 0:
                # file size to move in MB
                mbToMove = long((totalSize-siteSizeMap[tmpSiteName])/(1024*1024))
                # number of files to move
                nFilesToMove = maxNumFiles-len(siteFilesMap[tmpSiteName])
                # consider size and # of files 
                weight = weight * (totalSize+siteSizeMap[tmpSiteName]) / totalSize / (nFilesToMove/100+1)
                weightStr += 'fileSizeToMoveMB={0} nFilesToMove={1} '.format(mbToMove,nFilesToMove)
            # T1 weight
            if tmpSiteName in t1Sites+sitesShareSeT1:
                weight *= t1Weight
                weightStr += 't1W={0} '.format(t1Weight)

            # apply network metrics to weight
            if taskSpec.useWorldCloud() and nucleus:

                tmpAtlasSiteName = None
                try:
                    tmpAtlasSiteName = storageMapping[tmpSiteName]
                except KeyError:
                    tmpLog.debug('Panda site {0} was not in site mapping. Default network values will be given'.
                                 format(tmpSiteName))

                try:
                    closeness = networkMap[tmpAtlasSiteName][AGIS_CLOSENESS]
                except KeyError:
                    tmpLog.debug('No {0} information found in network matrix from {1}({2}) to {3}'.
                                 format(AGIS_CLOSENESS, tmpAtlasSiteName, tmpSiteName, nucleus))
                    closeness = MAX_CLOSENESS * 0.7

                try:
                    nFilesInQueue = networkMap[tmpAtlasSiteName][queued_tag]
                except KeyError:
                    tmpLog.debug('No {0} information found in network matrix from {1} ({2}) to {3}'.
                                 format(queued_tag, tmpAtlasSiteName, tmpSiteName, nucleus))
                    nFilesInQueue = 0

                mbps = None
                try:
                    mbps = networkMap[tmpAtlasSiteName][FTS_1W]
                    mbps = networkMap[tmpAtlasSiteName][FTS_1D]
                    mbps = networkMap[tmpAtlasSiteName][FTS_1H]
                except KeyError:
                    if mbps is None:
                        tmpLog.debug('No dynamic FTS mbps information found in network matrix from {0}({1}) to {2}'.
                                     format(tmpAtlasSiteName, tmpSiteName, nucleus))

                # network weight: value between 1 and 2, except when nucleus == satellite
                if nucleus == tmpAtlasSiteName: # 25 per cent weight boost for processing in nucleus itself
                    weightNwQueue = 2.5
                    weightNwThroughput = 2.5
                else:
                    # queue weight: the more in queue, the lower the weight
                    weightNwQueue = 2 - (nFilesInQueue * 1.0 / self.queue_threshold)

                    # throughput weight: the higher the throughput, the higher the weight
                    if mbps is not None:
                        weightNwThroughput = self.convertMBpsToWeight(mbps)
                    else:
                        weightNwThroughput = 1 + ((MAX_CLOSENESS - closeness) * 1.0 / (MAX_CLOSENESS-MIN_CLOSENESS))

                # combine queue and throughput weights
                weightNw = self.nwQueueImportance * weightNwQueue + self.nwThroughputImportance * weightNwThroughput

                weightStr += 'weightNw={0} ( closeness={1} nFilesQueued={2} throughputMBps={3} Network weight:{4} )'.\
                    format(weightNw, closeness, nFilesInQueue, mbps, self.nwActive)

                #If network measurements in active mode, apply the weight
                if self.nwActive:
                    weight *= weightNw

                tmpLog.info('subject=network_data src={1} dst={2} weight={3} weightNw={4} '
                            'weightNwThroughput={5} weightNwQueued={6} mbps={7} closeness={8} nqueued={9}'
                            .format(taskSpec.jediTaskID, tmpAtlasSiteName, nucleus, weight, weightNw,
                                    weightNwThroughput, weightNwQueue, mbps, closeness, nFilesInQueue))

            # make candidate
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName)
            # set weight and params
            siteCandidateSpec.weight = weight
            siteCandidateSpec.nRunningJobs = nRunning
            siteCandidateSpec.nQueuedJobs = nActivated + nAssigned + nStarting
            siteCandidateSpec.nAssignedJobs = nAssigned
            # set available files
            for tmpDatasetName,availableFiles in availableFileMap.iteritems():
                if availableFiles.has_key(tmpSiteName):
                    siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['localdisk']
                    siteCandidateSpec.localTapeFiles  += availableFiles[tmpSiteName]['localtape']
                    siteCandidateSpec.cacheFiles  += availableFiles[tmpSiteName]['cache']
                    siteCandidateSpec.remoteFiles += availableFiles[tmpSiteName]['remote']
            # add files as remote since WAN access is allowed
            if taskSpec.allowInputWAN() and tmpSiteSpec.allowWanInputAccess():
                siteCandidateSpec.remoteProtocol = 'direct'
                for datasetSpec in inputChunk.getDatasets(): 
                    siteCandidateSpec.remoteFiles += datasetSpec.Files
            # check if site is locked for WORLD
            lockedByBrokerage = False
            if taskSpec.useWorldCloud():
                lockedByBrokerage = self.checkSiteLock(taskSpec.vo, taskSpec.prodSourceLabel,
                                                       tmpPseudoSiteName, taskSpec.workQueue_ID, taskSpec.resource_type)
            # check cap with nRunning
            cutOffValue = 20
            cutOffFactor = 2 
            nRunningCap = max(cutOffValue,cutOffFactor*nRunning)
            nRunningCap = max(nRunningCap,nPilot)
            if taskSpec.getNumJumboJobs() == None or not tmpSiteSpec.useJumboJobs():
                forJumbo = False
            else:
                forJumbo = True
            # OK message. Use jumbo as primary by default
            if not forJumbo:
                okMsg = '  use site={0} with weight={1} {2} criteria=+use'.format(tmpPseudoSiteName,weight,weightStr)
                okAsPrimay = False
            else:
                okMsg = '  use site={0} for jumbo jobs with weight={1} {2} criteria=+usejumbo'.format(tmpPseudoSiteName,weight,weightStr)
                okAsPrimay = True
            # checks
            if lockedByBrokerage:
                ngMsg = '  skip site={0} due to locked by another brokerage '.format(tmpPseudoSiteName)
                ngMsg += 'criteria=-lock'
            elif (not tmpSiteName in siteSizeMap or siteSizeMap[tmpSiteName] >= totalSize) and \
                    (nActivated+nStarting) > nRunningCap:
                ngMsg = '  skip site={0} weight={1} due to nActivated+nStarting={2} '.format(tmpPseudoSiteName,
                                                                                             weight,
                                                                                             nActivated+nStarting)
                ngMsg += 'greater than max({0},{1}*nRun,nPilot) '.format(cutOffValue, cutOffFactor)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-cap'
            elif tmpSiteName in siteSizeMap and siteSizeMap[tmpSiteName] < totalSize and \
                    (nDefined+nActivated+nAssigned+nStarting) > nRunningCap:
                ngMsg = '  skip site={0} weight={1} due to nDefined+nActivated+nAssigned+nStarting={2} '.format(tmpPseudoSiteName,
                                                                                                                weight,
                                                                                                                nDefined+nActivated+nAssigned+nStarting)
                ngMsg += 'greater than max({0},{1}*nRun,nPilot) '.format(cutOffValue, cutOffFactor)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-cap'
            elif taskSpec.useWorldCloud() and self.nwActive and inputChunk.isExpress() \
                    and weightNw < self.nw_threshold * self.nw_weight_multiplier:
                ngMsg = '  skip site={0} due to low network weight for express task weightNw={1} threshold={2} '\
                    .format(tmpPseudoSiteName, weightNw, self.nw_threshold)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-lowNetworkWeight'
            else:
                ngMsg = '  skip site={0} due to low weight '.format(tmpPseudoSiteName)
                ngMsg += 'weight={0} {1} '.format(weight, weightStr)
                ngMsg += 'criteria=-loweigh'
                okAsPrimay = True
            # add to jumbo or primary or secondary
            if forJumbo:
                # only OK sites for jumbo
                if not okAsPrimay:
                    continue
                weightMap = weightMapJumbo
            elif okAsPrimay:
                weightMap = weightMapPrimary
            else:
                weightMap = weightMapSecondary
            # add weight
            if not weight in weightMap:
                weightMap[weight] = []
            weightMap[weight].append((siteCandidateSpec,okMsg,ngMsg))
        # use second candidates if no primary candidates passed cap/lock check
        if weightMapPrimary == {}:
            tmpLog.info('use second candidates since no sites pass cap/lock check')
            weightMap = weightMapSecondary
            # use hightest 3 weights
            weightRank = 3
        else:
            weightMap = weightMapPrimary
            # use all weights
            weightRank = None
            # dump NG message
            for tmpWeight in weightMapSecondary.keys():
                for siteCandidateSpec,tmpOkMsg,tmpNgMsg in weightMapSecondary[tmpWeight]:
                    tmpLog.info(tmpNgMsg)
        # add jumbo sites
        for weight,tmpList in weightMapJumbo.iteritems():
            if not weight in weightMap:
                weightMap[weight] = []
            for tmpItem in tmpList:
                weightMap[weight].append(tmpItem)
        # max candidates for WORLD
        if taskSpec.useWorldCloud():
            maxSiteCandidates = 10
        else:
            maxSiteCandidates = None
        newScanSiteList = []
        weightList = weightMap.keys()
        weightList.sort()
        weightList.reverse()
        for weightIdx,tmpWeight in enumerate(weightList):
            for siteCandidateSpec,tmpOkMsg,tmpNgMsg in weightMap[tmpWeight]:
                # candidates for jumbo jobs
                if taskSpec.getNumJumboJobs() != None:
                    tmpSiteSpec = self.siteMapper.getSite(siteCandidateSpec.siteName)
                    if tmpSiteSpec.useJumboJobs():
                        # use site for jumbo jobs
                        tmpLog.info(tmpOkMsg)
                        inputChunk.addSiteCandidateForJumbo(siteCandidateSpec)
                        continue
                # candidates for normal jobs
                if (weightRank == None or weightIdx < weightRank) and \
                        (maxSiteCandidates == None or len(newScanSiteList) < maxSiteCandidates):
                    # use site
                    tmpLog.info(tmpOkMsg)
                    newScanSiteList.append(siteCandidateSpec.siteName)
                    inputChunk.addSiteCandidate(siteCandidateSpec)
                else:
                    # dump NG message
                    tmpLog.info(tmpNgMsg)
        scanSiteList = newScanSiteList
        # final check
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        # lock sites for WORLD
        if taskSpec.useWorldCloud():
            for tmpSiteName in scanSiteList:
                #self.lockSite(taskSpec.vo,taskSpec.prodSourceLabel,tmpSiteName,taskSpec.workQueue_ID)
                pass
        tmpLog.info('final {0} candidates'.format(len(scanSiteList)))
        # return
        self.sendLogMessage(tmpLog)
        tmpLog.info('done')
        return self.SC_SUCCEEDED,inputChunk
    
