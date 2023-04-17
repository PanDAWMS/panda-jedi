import re
import copy
import datetime

from six import iteritems

try:
    long()
except Exception:
    long = int

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from .JobBrokerBase import JobBrokerBase
from . import AtlasBrokerUtils
from pandaserver.dataservice import DataServiceUtils
from dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer import EventServiceUtils
from pandaserver.taskbuffer import JobUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
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
class AtlasProdJobBroker(JobBrokerBase):

    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        JobBrokerBase.__init__(self, ddmIF, taskBufferIF)
        self.hospitalQueueMap = AtlasBrokerUtils.getHospitalQueues(self.siteMapper, JobUtils.PROD_PS, JobUtils.PROD_PS)
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

        self.total_queue_threshold = taskBufferIF.getConfigValue(COMPONENT, 'NQUEUED_NUC_CAP_FOR_JOBS', APP, VO)
        if self.total_queue_threshold is None:
            self.total_queue_threshold = 10000

        self.nw_weight_multiplier = taskBufferIF.getConfigValue(COMPONENT, 'NW_WEIGHT_MULTIPLIER', APP, VO)
        if self.nw_weight_multiplier is None:
            self.nw_weight_multiplier = 1

        self.io_intensity_cutoff = taskBufferIF.getConfigValue(COMPONENT, 'IO_INTENSITY_CUTOFF', APP, VO)
        if self.io_intensity_cutoff is None:
            self.io_intensity_cutoff = 200

        self.max_prio_for_bootstrap = taskBufferIF.getConfigValue(COMPONENT, 'MAX_PRIO_TO_BOOTSTRAP', APP, VO)
        if self.max_prio_for_bootstrap is None:
            self.max_prio_for_bootstrap = 150

        # load the SW availability map
        try:
            self.sw_map = taskBufferIF.load_sw_map()
        except:
            logger.error('Failed to load the SW tags map!!!')
            self.sw_map = {}

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

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap, hintForTB=False, siteListForTB=None, glLog=None):
        # suppress sending log
        if hintForTB:
            self.suppressLogSending = True
        # make logger
        if glLog is None:
            tmpLog = MsgWrapper(logger, '<jediTaskID={0}>'.format(taskSpec.jediTaskID),
                                monToken='<jediTaskID={0} {1}>'.format(taskSpec.jediTaskID,
                                                                       datetime.datetime.utcnow().isoformat('/')))
        else:
            tmpLog = glLog

        if not hintForTB:
            tmpLog.debug('start currentPriority={}'.format(taskSpec.currentPriority))

        if self.nwActive:
            tmpLog.debug('Network weights are ACTIVE!')
        else:
            tmpLog.debug('Network weights are PASSIVE!')

        timeNow = datetime.datetime.utcnow()

        # return for failure
        retFatal    = self.SC_FATAL, inputChunk
        retTmpError = self.SC_FAILED, inputChunk

        # new maxwdir
        newMaxwdir = {}

        # get sites in the cloud
        siteSkippedTmp = dict()
        sitePreAssigned = False
        siteListPreAssigned = False
        if siteListForTB is not None:
            scanSiteList = siteListForTB

        elif taskSpec.site not in ['',None] and inputChunk.getPreassignedSite() is None:
            # convert to pseudo queues if needed
            scanSiteList = []
            for tmpSiteName in taskSpec.site.split(','):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec is None or not tmpSiteSpec.is_unified:
                    scanSiteList.append(tmpSiteName)
                else:
                    scanSiteList += self.get_pseudo_sites([tmpSiteName],
                                                          self.siteMapper.getCloud(cloudName)['sites'])
            for tmpSiteName in scanSiteList:
                tmpLog.info('site={0} is pre-assigned criteria=+preassign'.format(tmpSiteName))
            if len(scanSiteList) > 1:
                siteListPreAssigned = True
            else:
                sitePreAssigned = True

        elif inputChunk.getPreassignedSite() is not None:
            if inputChunk.masterDataset.creationTime is not None and inputChunk.masterDataset.modificationTime is not None and \
                    inputChunk.masterDataset.modificationTime != inputChunk.masterDataset.creationTime and \
                    timeNow-inputChunk.masterDataset.modificationTime > datetime.timedelta(hours=24) and \
                    taskSpec.frozenTime is not None and timeNow-taskSpec.frozenTime > datetime.timedelta(hours=6):
                # ignore pre-assigned site since pmerge is timed out
                tmpLog.info('ignore pre-assigned for pmerge due to timeout')
                scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
                tmpLog.info('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
            else:
                # pmerge
                siteListPreAssigned = True
                scanSiteList = DataServiceUtils.getSitesShareDDM(self.siteMapper, inputChunk.getPreassignedSite(),
                                                                 JobUtils.PROD_PS, JobUtils.PROD_PS)
                scanSiteList.append(inputChunk.getPreassignedSite())
                tmpMsg = 'use site={0} since they share DDM endpoints with orinal_site={1} which is pre-assigned in masterDS '.format(str(scanSiteList),
                                                                                                                                      inputChunk.getPreassignedSite())
                tmpMsg += 'criteria=+premerge'
                tmpLog.info(tmpMsg)

        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
            tmpLog.info('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))

        # high prio ES
        if taskSpec.useEventService() and not taskSpec.useJobCloning() and (taskSpec.currentPriority >= 900 or inputChunk.useScout()):
            esHigh = True
        else:
            esHigh = False

        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        if not tmpSt:
            tmpLog.error('failed to get job statistics')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        # T1
        nucleusSpec = None
        if not taskSpec.useWorldCloud():
            t1Sites = [self.siteMapper.getCloud(cloudName)['source']]
            # hospital sites
            if cloudName in self.hospitalQueueMap:
                t1Sites += self.hospitalQueueMap[cloudName]
        else:
            # get destination for WORLD cloud
            if not hintForTB:
                # get nucleus
                nucleusSpec = self.siteMapper.getNucleus(taskSpec.nucleus)
                if nucleusSpec is None:
                    tmpLog.error('unknown nucleus {0}'.format(taskSpec.nucleus))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
            sitesShareSeT1 = DataServiceUtils.getSitesShareDDM(self.siteMapper, t1Sites[0],
                                                               JobUtils.PROD_PS, JobUtils.PROD_PS,
                                                               True)
        else:
            sitesShareSeT1 = []

        # core count
        if inputChunk.isMerging and taskSpec.mergeCoreCount is not None:
            taskCoreCount = taskSpec.mergeCoreCount
        else:
            taskCoreCount = taskSpec.coreCount

        # MP
        if taskCoreCount is not None and taskCoreCount > 1:
            # use MCORE only
            useMP = 'only'
        elif taskCoreCount == 0 and (taskSpec.currentPriority >= 900 or inputChunk.useScout()):
            # use MCORE only for ES scouts and close to completion
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
            wq_tag_global_share = wq_tag
        else:
            wq_tag = workQueue.queue_id
            workQueueGS = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(None, taskSpec.gshare)
            wq_tag_global_share = workQueueGS.queue_name

        # init summary list
        self.init_summary_list('Job brokerage summary',
                               None,
                               scanSiteList)

        ######################################
        # selection for status
        if not sitePreAssigned and not siteListPreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)

                # skip unified queues
                if tmpSiteSpec.is_unified:
                    continue
                # check site status
                msgFlag = False
                skipFlag = False

                if tmpSiteSpec.status in ['online', 'standby']:
                    newScanSiteList.append(tmpSiteName)
                else:
                    msgFlag = True
                    skipFlag = True

                if msgFlag:
                    tmpStr = '  skip site=%s due to status=%s criteria=-status' % (tmpSiteName,tmpSiteSpec.status)
                    if skipFlag:
                        tmpLog.info(tmpStr)
                    else:
                        siteSkippedTmp[tmpSiteName] = tmpStr

            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed site status check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'status check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
            if queued_tag in networkMap['total']:
                totalQueued = networkMap['total'][queued_tag]
            else:
                totalQueued = 0

            tmpLog.debug('Total number of files being transferred to the nucleus : {0}'.format(totalQueued))
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()
            for tmpPandaSiteName in self.get_unified_sites(scanSiteList):
                try:
                    tmpAtlasSiteName = storageMapping[tmpPandaSiteName]['default']
                    skipFlag = False
                    tempFlag = False
                    criteria = '-link_unusable'
                    from_str = 'from satellite={0}'.format(tmpAtlasSiteName)
                    reason = ''
                    if nucleus == tmpAtlasSiteName:
                        # nucleus
                        pass
                    elif totalQueued >= self.total_queue_threshold:
                        # total exceed
                        reason = 'too many output files being transferred to the nucleus {0}(>{1} total limit)' \
                            .format(totalQueued, self.total_queue_threshold)
                        criteria = '-links_full'
                        from_str = ''
                        tempFlag = True
                    elif tmpAtlasSiteName not in networkMap:
                        # Don't skip missing links for the moment. In later stages missing links
                        # default to the worst connectivity and will be penalized.
                        pass
                    elif AGIS_CLOSENESS in networkMap[tmpAtlasSiteName] and \
                            networkMap[tmpAtlasSiteName][AGIS_CLOSENESS] == BLOCKED_LINK:
                        # blocked link
                        reason = 'agis_closeness={0}'.format(networkMap[tmpAtlasSiteName][AGIS_CLOSENESS])
                        skipFlag = True
                    elif queued_tag in networkMap[tmpAtlasSiteName] and \
                            networkMap[tmpAtlasSiteName][queued_tag] >= self.queue_threshold:
                        # too many queued
                        reason = 'too many output files queued in the channel {0}(>{1} link limit)'\
                                .format(networkMap[tmpAtlasSiteName][queued_tag], self.queue_threshold)
                        tempFlag = True

                    # add
                    tmpStr = '  skip site={0} due to {1}, {2} to nucleus={3}: criteria={4}'\
                        .format(tmpPandaSiteName, reason, from_str, nucleus, criteria)
                    if skipFlag:
                        tmpLog.info(tmpStr)
                    else:
                        newScanSiteList.append(tmpPandaSiteName)
                        if tempFlag:
                            newSkippedTmp[tmpPandaSiteName] = tmpStr
                except KeyError:
                    # Don't skip missing links for the moment. In later stages missing links
                    # default to the worst connectivity and will be penalized.
                    newScanSiteList.append(tmpPandaSiteName)
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed link check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'link check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
            oldScanSiteList = copy.copy(scanSiteList)

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
            self.add_summary_message(oldScanSiteList, scanSiteList, 'opportunistic check')

            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection to avoid slow or inactive sites
        if (taskSpec.currentPriority >= 800 or inputChunk.useScout() or \
                inputChunk.isMerging or taskSpec.mergeOutput()) \
                and not sitePreAssigned and not siteListPreAssigned:
            # get inactive sites
            inactiveTimeLimit = 2
            inactiveSites = self.taskBufferIF.getInactiveSites_JEDI('production',inactiveTimeLimit)
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()

            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                nToGetAll = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteSpec.get_unified_name(), 'activated') + \
                    AtlasBrokerUtils.getNumJobs(jobStatMap, tmpSiteSpec.get_unified_name(), 'starting')

                if tmpSiteName in inactiveSites and nToGetAll > 0:
                    tmpMsg = '  skip site={0} since high prio/scouts/merge needs to avoid inactive sites (laststart is older than {1}h) '.format(tmpSiteName,
                                                                                                                                                 inactiveTimeLimit)
                    tmpMsg += 'criteria=-inactive'
                    # temporary problem
                    newSkippedTmp[tmpSiteName] = tmpMsg
                newScanSiteList.append(tmpSiteName)
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed for slowness/inactive check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'slowness/inactive check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
                if datasetName not in self.dataSiteMap:
                    # get the list of sites where data is available
                    tmpLog.info('getting the list of sites where {0} is available'.format(datasetName))
                    tmpSt,tmpRet = AtlasBrokerUtils.getSitesWithData(self.siteMapper,
                                                                     self.ddmIF,datasetName, 
                                                                     JobUtils.PROD_PS, JobUtils.PROD_PS,
                                                                     datasetSpec.storageToken)
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
                    tmpLog.debug('map of data availability : {0}'.format(str(tmpRet)))
                # check if T1 has the data
                if cloudName in self.dataSiteMap[datasetName]:
                    cloudHasData = True
                else:
                    cloudHasData = False
                t1hasData = False
                if cloudHasData:
                    for tmpSE,tmpSeVal in iteritems(self.dataSiteMap[datasetName][cloudName]['t1']):
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
        if sitePreAssigned and taskSpec.prodSourceLabel not in [JobUtils.PROD_PS] or workQueue.queue_name not in ['test', 'validation']:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if AtlasBrokerUtils.hasZeroShare(tmpSiteSpec,taskSpec,inputChunk.isMerging,tmpLog):
                    tmpLog.info('  skip site={0} due to zero share criteria=-zeroshare'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed zero share check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'zero share check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for jumbo jobs
        if not sitePreAssigned and taskSpec.getNumJumboJobs() is None:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.useJumboJobs():
                    tmpLog.info('  skip site={0} since it is only for jumbo jobs criteria=-jumbo'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed jumbo job check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'jumbo job check')
            if scanSiteList == []:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        elif not sitePreAssigned and taskSpec.getNumJumboJobs() is not None:
            nReadyEvents = self.taskBufferIF.getNumReadyEvents(taskSpec.jediTaskID)
            if nReadyEvents is not None:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if tmpSiteSpec.useJumboJobs():
                        minEvents = tmpSiteSpec.getMinEventsForJumbo()
                        if minEvents is not None and nReadyEvents < minEvents:
                            tmpLog.info('  skip site={0} since not enough events ({1}<{2}) for jumbo criteria=-fewevents'.format(tmpSiteName,
                                                                                                                                 minEvents,
                                                                                                                                 nReadyEvents))
                            continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed jumbo events check nReadyEvents={1}'.format(len(scanSiteList), nReadyEvents))
                self.add_summary_message(oldScanSiteList, scanSiteList, 'jumbo events check')
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error('no candidates')
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # selection for iointensity limits

        # get default disk IO limit from GDP config
        max_diskio_per_core_default =  self.taskBufferIF.getConfigValue(COMPONENT, 'MAX_DISKIO_DEFAULT', APP, VO)
        if not max_diskio_per_core_default:
            max_diskio_per_core_default = 10**10

        # get the current disk IO usage per site
        diskio_percore_usage = self.taskBufferIF.getAvgDiskIO_JEDI()
        unified_site_list = self.get_unified_sites(scanSiteList)
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in unified_site_list:

            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)

            # measured diskIO at queue
            diskio_usage_tmp = diskio_percore_usage.get(tmpSiteName, 0)

            # figure out queue or default limit
            if tmpSiteSpec.maxDiskio and tmpSiteSpec.maxDiskio > 0:
                # there is a limit specified in AGIS
                diskio_limit_tmp = tmpSiteSpec.maxDiskio
            else:
                # we need to use the default value from GDP Config
                diskio_limit_tmp = max_diskio_per_core_default

            # normalize task diskIO by site corecount
            diskio_task_tmp = taskSpec.diskIO
            if taskSpec.diskIO is not None and taskSpec.coreCount not in [None, 0, 1] and tmpSiteSpec.coreCount not in [None, 0]:
                diskio_task_tmp = taskSpec.diskIO // tmpSiteSpec.coreCount

            try: # generate a log message parseable by logstash for monitoring
                log_msg = 'diskIO measurements: site={0} jediTaskID={1} '.format(tmpSiteName, taskSpec.jediTaskID)
                if diskio_task_tmp is not None:
                    log_msg += 'diskIO_task={:.2f} '.format(diskio_task_tmp)
                if diskio_usage_tmp is not None:
                    log_msg += 'diskIO_site_usage={:.2f} '.format(diskio_usage_tmp)
                if diskio_limit_tmp is not None:
                    log_msg += 'diskIO_site_limit={:.2f} '.format(diskio_limit_tmp)
                tmpLog.info(log_msg)
            except Exception:
                tmpLog.debug('diskIO measurements: Error generating diskIO message')

            # if the task has a diskIO defined, the queue is over the IO limit and the task IO is over the limit
            if diskio_task_tmp and diskio_usage_tmp and diskio_limit_tmp \
                and diskio_usage_tmp > diskio_limit_tmp and diskio_task_tmp > diskio_limit_tmp:
                tmpMsg = '  skip site={0} due to diskIO overload criteria=-diskIO'.format(tmpSiteName)
                newSkippedTmp[tmpSiteName] = tmpMsg

            newScanSiteList.append(tmpSiteName)

        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)

        tmpLog.info('{0} candidates passed diskIO check'.format(len(scanSiteList)))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'diskIO check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for MP
        if not sitePreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if useMP == 'any' or (useMP == 'only' and tmpSiteSpec.coreCount > 1) or \
                        (useMP =='unuse' and tmpSiteSpec.coreCount in [0,1,None]):
                    max_core_count = taskSpec.get_max_core_count()
                    if max_core_count and tmpSiteSpec.coreCount and tmpSiteSpec.coreCount > max_core_count:
                        tmpLog.info(
                            '  skip site=%s due to larger core count site:%s than task_max=%s criteria=-cpucore' % \
                            (tmpSiteName, tmpSiteSpec.coreCount, max_core_count))
                    else:
                        newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info('  skip site=%s due to core mismatch site:%s <> task:%s criteria=-cpucore' % \
                                 (tmpSiteName,tmpSiteSpec.coreCount,taskCoreCount))
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed for core count check with policy={1}'.format(len(scanSiteList),useMP))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'core count check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for release
        if taskSpec.transHome is not None:
            jsonCheck = AtlasBrokerUtils.JsonSoftwareCheck(self.siteMapper, self.sw_map)
            unified_site_list = self.get_unified_sites(scanSiteList)

            host_cpu_spec = taskSpec.get_host_cpu_spec()
            host_gpu_spec = taskSpec.get_host_gpu_spec()

            if taskSpec.get_base_platform() is None:
                use_container = False
            else:
                use_container = True

            cmt_config = taskSpec.get_sw_platform()
            need_cvmfs = False

            # 3 digits base release or normal tasks
            if (re.search('-\d+\.\d+\.\d+$', taskSpec.transHome) is not None) \
                    or (re.search('rel_\d+(\n|$)', taskSpec.transHome) is None
                        and re.search('\d{4}-\d{2}-\d{2}T\d{4}$', taskSpec.transHome) is None):
                cvmfs_repo = "atlas"
                sw_project = taskSpec.transHome.split('-')[0]
                sw_version = taskSpec.transHome.split('-')[1]
                cmt_config_only = False

            # nightlies
            else:
                cvmfs_repo = "nightlies"
                sw_project = None
                sw_version = None
                cmt_config_only = False

            siteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, cvmfs_repo,
                                                               sw_project, sw_version, cmt_config,
                                                               need_cvmfs, cmt_config_only,
                                                               need_container=use_container,
                                                               container_name=taskSpec.container_name,
                                                               only_tags_fc=taskSpec.use_only_tags_fc(),
                                                               host_cpu_spec=host_cpu_spec,
                                                               host_gpu_spec=host_gpu_spec,
                                                               log_stream=tmpLog)
            sitesAuto = copy.copy(siteListWithSW)
            sitesAny = []

            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)

            for tmpSiteName in unified_site_list:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteName in siteListWithSW:
                    # passed
                    newScanSiteList.append(tmpSiteName)
                elif not (taskSpec.container_name and taskSpec.use_only_tags_fc()) and \
                    host_cpu_spec is None and host_gpu_spec is None and \
                    tmpSiteSpec.releases == ['ANY']:
                    # release check is disabled or release is available
                    newScanSiteList.append(tmpSiteName)
                    sitesAny.append(tmpSiteName)
                else:
                    if tmpSiteSpec.releases == ['AUTO']:
                        autoStr = 'with AUTO'
                    else:
                        autoStr = 'without AUTO'
                    # release is unavailable
                    tmpLog.info('  skip site=%s %s due to cache=%s:%s sw_platform="%s" '
                                'cpu=%s gpu=%s criteria=-cache' %(tmpSiteName, autoStr, taskSpec.transHome,
                                                                  taskSpec.get_sw_platform(), taskSpec.container_name,
                                                                  str(host_cpu_spec), str(host_gpu_spec)))

            sitesAuto = self.get_pseudo_sites(sitesAuto, scanSiteList)
            sitesAny = self.get_pseudo_sites(sitesAny, scanSiteList)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{} candidates ({} with AUTO, {} with ANY) passed SW check '.format(len(scanSiteList),
                                                                                            len(sitesAuto),
                                                                                            len(sitesAny)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'SW check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for memory
        origMinRamCount = inputChunk.getMaxRamCount()
        if origMinRamCount not in [0,None]:
            if inputChunk.isMerging:
                strMinRamCount = '{0}(MB)'.format(origMinRamCount)
            else:
                strMinRamCount = '{0}({1})'.format(origMinRamCount,taskSpec.ramUnit)
            if not inputChunk.isMerging and taskSpec.baseRamCount not in [0,None]:
                strMinRamCount += '+{0}'.format(taskSpec.baseRamCount)
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement
                minRamCount = origMinRamCount
                if taskSpec.ramPerCore() and not inputChunk.isMerging:
                    if tmpSiteSpec.coreCount not in [None,0]:
                        minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    minRamCount += taskSpec.baseRamCount
                # compensate
                minRamCount = JobUtils.compensate_ram_count(minRamCount)
                # site max memory requirement
                site_maxmemory = 0
                if tmpSiteSpec.maxrss not in [0,None]:
                    site_maxmemory = tmpSiteSpec.maxrss
                # check at the site
                if site_maxmemory not in [0,None] and minRamCount != 0 and minRamCount > site_maxmemory:
                    tmpMsg = '  skip site={0} due to site RAM shortage {1}(site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                    site_maxmemory,
                                                                                                                    minRamCount)
                    tmpMsg += 'criteria=-lowmemory'
                    tmpLog.info(tmpMsg)
                    continue
                # site min memory requirement
                site_minmemory = 0
                if tmpSiteSpec.minrss not in [0,None]:
                    site_minmemory = tmpSiteSpec.minrss
                if site_minmemory not in [0,None] and minRamCount != 0 and minRamCount < site_minmemory:
                    tmpMsg = '  skip site={0} due to job RAM shortage {1}(site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                      site_minmemory,
                                                                                                                      minRamCount)
                    tmpMsg += 'criteria=-highmemory'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed memory check {1}'.format(len(scanSiteList),strMinRamCount))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'memory check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for scratch disk
        if taskSpec.outputScaleWithEvents():
            minDiskCount = max(taskSpec.getOutDiskSize()*inputChunk.getMaxAtomSize(getNumEvents=True),
                               inputChunk.defaultOutputSize)
        else:
            minDiskCount = max(taskSpec.getOutDiskSize()*inputChunk.getMaxAtomSize(effectiveSize=True),
                               inputChunk.defaultOutputSize)
        minDiskCount  += taskSpec.getWorkDiskSize()
        minDiskCountL  = minDiskCount
        minDiskCountD  = minDiskCount
        minDiskCountL += inputChunk.getMaxAtomSize()
        minDiskCountL  = minDiskCountL // 1024 // 1024
        minDiskCountD  = minDiskCountD // 1024 // 1024
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check direct access
            if taskSpec.allowInputLAN() == 'only' and not tmpSiteSpec.isDirectIO() and \
                    not tmpSiteSpec.always_use_direct_io() and not inputChunk.isMerging:
                tmpMsg = '  skip site={0} since direct IO is disabled '.format(tmpSiteName)
                tmpMsg += 'criteria=-remoteio'
                tmpLog.info(tmpMsg)
                continue
            # check scratch size
            if tmpSiteSpec.maxwdir != 0:
                if JediCoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
                    # size for remote access
                    minDiskCount = minDiskCountD
                else:
                    # size for copy-to-scratch
                    minDiskCount = minDiskCountL

                # get site and task corecount to scale maxwdir
                if tmpSiteSpec.coreCount in [None, 0, 1]:
                    site_cc = 1
                else:
                    site_cc = tmpSiteSpec.coreCount

                if taskSpec.coreCount in [None, 0, 1] and not taskSpec.useEventService():
                    task_cc = 1
                else:
                    task_cc = site_cc

                maxwdir_scaled = tmpSiteSpec.maxwdir * task_cc / site_cc

                if minDiskCount > maxwdir_scaled:
                    tmpMsg = '  skip site={0} due to small scratch disk {1} MB less than {2} MB'.format(tmpSiteName,
                                                                                                        maxwdir_scaled,
                                                                                                        minDiskCount)
                    tmpMsg += ' criteria=-disk'
                    tmpLog.info(tmpMsg)
                    continue
                newMaxwdir[tmpSiteName] = maxwdir_scaled
            newScanSiteList.append(tmpSiteName)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed scratch disk check minDiskCount>{1} MB'.format(len(scanSiteList),
                                                                                          minDiskCount))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'disk check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for available space in SE
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.PROD_PS, JobUtils.PROD_PS)
            # check endpoint
            tmpEndPoint = tmpSiteSpec.ddm_endpoints_output[scope_output].getEndPoint(tmpSiteSpec.ddm_output[scope_output])
            if tmpEndPoint is not None:
                # check free size
                tmpSpaceSize = 0
                if tmpEndPoint['space_free'] is not None:
                    tmpSpaceSize += tmpEndPoint['space_free']
                if tmpEndPoint['space_expired'] is not None:
                    tmpSpaceSize += tmpEndPoint['space_expired']
                diskThreshold = 200
                tmpMsg = None
                if tmpSpaceSize < diskThreshold and 'skip_RSE_check' not in tmpSiteSpec.catchall:  # skip_RSE_check: exceptional bypass of RSEs without storage reporting
                    tmpMsg = '  skip site={0} due to disk shortage at {1} {2}GB < {3}GB criteria=-disk'.format(tmpSiteName, tmpSiteSpec.ddm_output[scope_output],
                                                                                                               tmpSpaceSize, diskThreshold)
                # check if blacklisted
                elif tmpEndPoint['blacklisted'] == 'Y':
                    tmpMsg = '  skip site={0} since endpoint={1} is blacklisted in DDM criteria=-blacklist'.format(tmpSiteName, tmpSiteSpec.ddm_output[scope_output])
                if tmpMsg is not None:
                    newSkippedTmp[tmpSiteName] = tmpMsg
            newScanSiteList.append(tmpSiteName)
        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed SE space check'.format(len(scanSiteList)))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'SE space check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
                maxAttemptEsJob += 1
        else:
            nEsConsumers = 1
            maxAttemptEsJob = 1
        maxWalltime = None
        strMaxWalltime = None
        if not taskSpec.useHS06():
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
            if taskSpec.walltime is not None:
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
            if taskSpec.getCpuTime() is not None:
                minWalltime = taskSpec.getCpuTime() * tmpMaxAtomSize
                if taskSpec.dynamicNumEvents():
                    eventJump, totalEvents = inputChunk.check_event_jump_and_sum()
                    maxWalltime = taskSpec.getCpuTime() * totalEvents
                    strMaxWalltime = 'cpuTime*maxEventsPerJob={}*{}'.format(taskSpec.getCpuTime(), totalEvents)
            else:
                minWalltime = None
            # take # of consumers into account
            if not taskSpec.useEventService() or taskSpec.useJobCloning():
                strMinWalltime = 'cpuTime*nEventsPerJob={0}*{1}'.format(taskSpec.getCpuTime(), tmpMaxAtomSize)
            else:
                strMinWalltime = 'cpuTime*nEventsPerJob/nEsConsumers/maxAttemptEsJob={0}*{1}/{2}/{3}'.format(
                    taskSpec.getCpuTime(),
                    tmpMaxAtomSize,
                    nEsConsumers,
                    maxAttemptEsJob)
        if minWalltime is not None:
            minWalltime /= (nEsConsumers * maxAttemptEsJob)

        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            siteMaxTime = tmpSiteSpec.maxtime
            origSiteMaxTime = siteMaxTime
            # check max walltime at the site
            tmpSiteStr = '{0}'.format(siteMaxTime)
            if taskSpec.useHS06():
                oldSiteMaxTime = siteMaxTime
                siteMaxTime -= taskSpec.baseWalltime
                tmpSiteStr = '({0}-{1})'.format(oldSiteMaxTime,taskSpec.baseWalltime)
            if siteMaxTime not in [None,0] and tmpSiteSpec.coreCount not in [None,0]:
                siteMaxTime *= tmpSiteSpec.coreCount
                tmpSiteStr += '*{0}'.format(tmpSiteSpec.coreCount)
            if taskSpec.useHS06():
                if siteMaxTime not in [None,0]:
                    siteMaxTime *= tmpSiteSpec.corepower
                    tmpSiteStr += '*{0}'.format(tmpSiteSpec.corepower)
                siteMaxTime *= float(taskSpec.cpuEfficiency) / 100.0
                siteMaxTime = long(siteMaxTime)
                tmpSiteStr += '*{0}%'.format(taskSpec.cpuEfficiency)
            if origSiteMaxTime != 0 and minWalltime and minWalltime > siteMaxTime:
                tmpMsg = '  skip site={0} due to short site walltime {1} (site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                   tmpSiteStr,
                                                                                                                   strMinWalltime)
                tmpMsg += 'criteria=-shortwalltime'
                tmpLog.info(tmpMsg)
                continue
            # sending scouts or merge or walltime-undefined jobs to only sites where walltime is more than 1 day
            if (not sitePreAssigned and inputChunk.useScout()) or inputChunk.isMerging or \
                    (not taskSpec.walltime and not taskSpec.walltimeUnit and not taskSpec.cpuTimeUnit) or \
                    (not taskSpec.getCpuTime() and taskSpec.cpuTimeUnit):
                minTimeForZeroWalltime = 24
                str_minTimeForZeroWalltime = "{}hr*10HS06s".format(minTimeForZeroWalltime)
                minTimeForZeroWalltime *= 60 * 60 * 10

                if tmpSiteSpec.coreCount not in [None, 0]:
                    minTimeForZeroWalltime *= tmpSiteSpec.coreCount
                    str_minTimeForZeroWalltime += "*{}cores".format(tmpSiteSpec.coreCount)

                if siteMaxTime != 0 and siteMaxTime < minTimeForZeroWalltime:
                    tmpMsg = '  skip site={0} due to site walltime {1} (site upper limit) insufficient '.\
                        format(tmpSiteName, tmpSiteStr)
                    if inputChunk.useScout():
                        tmpMsg += 'for scouts ({0} at least) '.format(str_minTimeForZeroWalltime)
                        tmpMsg += 'criteria=-scoutwalltime'
                    else:
                        tmpMsg += 'for zero walltime ({0} at least) '.format(str_minTimeForZeroWalltime)
                        tmpMsg += 'criteria=-zerowalltime'
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
            if siteMinTime not in [None,0] and tmpSiteSpec.coreCount not in [None,0]:
                siteMinTime *= tmpSiteSpec.coreCount
                tmpSiteStr += '*{0}'.format(tmpSiteSpec.coreCount)
            if taskSpec.useHS06():
                if siteMinTime not in [None,0]:
                    siteMinTime *= tmpSiteSpec.corepower
                    tmpSiteStr += '*{0}'.format(tmpSiteSpec.corepower)
                siteMinTime *= float(taskSpec.cpuEfficiency) / 100.0
                siteMinTime = long(siteMinTime)
                tmpSiteStr += '*{0}%'.format(taskSpec.cpuEfficiency)
            if origSiteMinTime != 0 and ((minWalltime is None or minWalltime < siteMinTime) and
                                         (maxWalltime is None or maxWalltime < siteMinTime)):
                tmpMsg = '  skip site {0} due to short job walltime {1} (site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                     tmpSiteStr,
                                                                                                                     strMinWalltime)
                if maxWalltime:
                    tmpMsg += 'and {} '.format(strMaxWalltime)
                tmpMsg += 'criteria=-longwalltime'
                tmpLog.info(tmpMsg)
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        if not taskSpec.useHS06():
            tmpLog.info('{0} candidates passed walltime check {1}({2})'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
        else:
            tmpStr = '{0} candidates passed walltime check {1}({2})'.format(len(scanSiteList),
                                                                                          strMinWalltime,
                                                                                          taskSpec.cpuTimeUnit)
            if maxWalltime:
                tmpStr += ' and {}'.format(strMaxWalltime)
            tmpLog.info(tmpStr)
        self.add_summary_message(oldScanSiteList, scanSiteList, 'walltime check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for network connectivity
        if not sitePreAssigned:
            ipConnectivity = taskSpec.getIpConnectivity()
            ipStack =  taskSpec.getIpStack()
            if ipConnectivity or ipStack:
                newScanSiteList = []
                oldScanSiteList = copy.copy(scanSiteList)
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # check connectivity
                    if ipConnectivity:
                        wn_connectivity = tmpSiteSpec.get_wn_connectivity()
                        if wn_connectivity == 'full':
                            pass
                        elif wn_connectivity == 'http' and ipConnectivity == 'http':
                            pass
                        else:
                            tmpMsg = '  skip site={0} due to insufficient connectivity site={1} vs task={2} '.format(tmpSiteName,
                                                                                                                        wn_connectivity,
                                                                                                                        ipConnectivity)
                            tmpMsg += 'criteria=-network'
                            tmpLog.info(tmpMsg)
                            continue
                    # check IP stack
                    if ipStack:
                        wn_ipstack = tmpSiteSpec.get_ipstack()
                        if ipStack != wn_ipstack:
                            tmpMsg = '  skip site={0} due to IP stack mismatch site={1} vs task={2} '.format(tmpSiteName,
                                                                                                             wn_ipstack,
                                                                                                             ipStack)
                            tmpMsg += 'criteria=-network'
                            tmpLog.info(tmpMsg)
                            continue



                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed network check ({1})'.format(len(scanSiteList),
                                                                                ipConnectivity))
                self.add_summary_message(oldScanSiteList, scanSiteList, 'network check')
                if not scanSiteList:
                    self.dump_summary(tmpLog)
                    tmpLog.error('no candidates')
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # selection for event service
        if not sitePreAssigned:
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # event service
                if taskSpec.useEventService() and not taskSpec.useJobCloning():
                    if tmpSiteSpec.getJobSeed() == 'std':
                        tmpMsg = '  skip site={0} since EventService is not allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-es'
                        tmpLog.info(tmpMsg)
                        continue
                    if tmpSiteSpec.getJobSeed() == 'eshigh' and not esHigh:
                        tmpMsg = '  skip site={0} since low prio EventService is not allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-eshigh'
                        tmpLog.info(tmpMsg)
                        continue
                else:
                    if tmpSiteSpec.getJobSeed() == 'es':
                        tmpMsg = '  skip site={0} since only EventService is allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-nones'
                        tmpLog.info(tmpMsg)
                        continue
                # skip UCORE/SCORE
                if taskSpec.useEventService() and tmpSiteSpec.sitename != tmpSiteSpec.get_unified_name() \
                        and tmpSiteSpec.coreCount == 1:
                    tmpMsg = '  skip site={0} since EventService on UCORE/SCORE '.format(tmpSiteName)
                    tmpMsg += 'criteria=-es_ucore'
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed EventService check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'EventService check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        ######################################
        # selection for transferring
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        newSkippedTmp = dict()
        for tmpSiteName in self.get_unified_sites(scanSiteList):
            try:
                tmpAtlasSiteName = storageMapping[tmpSiteName]['default']
                if tmpSiteName not in t1Sites+sitesShareSeT1 and nucleus != tmpAtlasSiteName:
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
                    if max(maxTransferring,2*nRunJobs) < nTraJobs:
                        tmpStr = '  skip site=%s due to too many transferring=%s greater than max(%s,2x%s) criteria=-transferring' % \
                            (tmpSiteName, nTraJobs, maxTransferring, nRunJobs)
                        newSkippedTmp[tmpSiteName] = tmpStr
            except KeyError:
                pass
            newScanSiteList.append(tmpSiteName)
        siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
        scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
        tmpLog.info('{0} candidates passed transferring check'.format(len(scanSiteList)))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'transferring check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for T1 weight
        if taskSpec.getT1Weight() < 0 and not inputChunk.isMerging:
            useT1Weight = True
        else:
            useT1Weight = False
        t1Weight = taskSpec.getT1Weight()
        if t1Weight == 0:
            tmpLog.info('IO intensity {0}'.format(taskSpec.ioIntensity))
            # use T1 weight in cloudconfig if IO intensive
            if taskSpec.ioIntensity is not None and taskSpec.ioIntensity > 500:
                t1Weight = self.siteMapper.getCloud(cloudName)['weight']
            else:
                t1Weight = 1
        oldScanSiteList = copy.copy(scanSiteList)
        if t1Weight < 0 and not inputChunk.isMerging:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if tmpSiteName not in t1Sites+sitesShareSeT1:
                    tmpLog.info('  skip site={0} due to negative T1 weight criteria=-t1weight'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            t1Weight = 1
        t1Weight = max(t1Weight,t1WeightForHighPrio)
        tmpLog.info('T1 weight {0}'.format(t1Weight))
        tmpLog.info('{0} candidates passed T1 weight check'.format(len(scanSiteList)))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'T1 weight check')
        if not scanSiteList:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # selection for full chain
        if nucleusSpec:
            full_chain = taskSpec.check_full_chain_with_nucleus(nucleusSpec)
            oldScanSiteList = copy.copy(scanSiteList)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if full_chain:
                    # skip PQs not in the nucleus
                    if tmpSiteName not in t1Sites:
                        tmpLog.info('  skip site={0} not in nucleus for full chain criteria=-full_chain'.format(tmpSiteName))
                        continue
                else:
                    # skip PQs only for full-chain
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if tmpSiteSpec.bare_nucleus_mode() == 'only':
                        tmpLog.info('  skip site={0} only for full chain criteria=-not_full_chain'.format(tmpSiteName))
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{} candidates passed full chain ({}) check'.format(len(scanSiteList), full_chain))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'full chain check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for nPilot
        nPilotMap = {}
        if not sitePreAssigned and not siteListPreAssigned:
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            newSkippedTmp = dict()
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                nPilot = 0
                if tmpSiteName in nWNmap:
                    nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
                # skip no pilot sites unless the task and the site use jumbo jobs or the site is standby
                if nPilot == 0 and ('test' not in taskSpec.prodSourceLabel or inputChunk.isExpress()) and \
                        (taskSpec.getNumJumboJobs() is None or not tmpSiteSpec.useJumboJobs()) and \
                        tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type) is None:
                    tmpStr = '  skip site=%s due to no pilot criteria=-nopilot' % tmpSiteName
                    newSkippedTmp[tmpSiteName] = tmpStr
                newScanSiteList.append(tmpSiteName)
                nPilotMap[tmpSiteName] = nPilot
            siteSkippedTmp = self.add_pseudo_sites_to_skip(newSkippedTmp, scanSiteList, siteSkippedTmp)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'pilot check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError

        # return if to give a hint for task brokerage
        if hintForTB:
            ######################################
            # temporary problems
            newScanSiteList = []
            oldScanSiteList = copy.copy(scanSiteList)
            for tmpSiteName in scanSiteList:
                if tmpSiteName in siteSkippedTmp:
                    tmpLog.info(siteSkippedTmp[tmpSiteName])
                else:
                    newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed temporary problem check'.format(len(scanSiteList)))
            self.add_summary_message(oldScanSiteList, scanSiteList, 'temp problem check')
            if not scanSiteList:
                self.dump_summary(tmpLog)
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            return self.SC_SUCCEEDED,scanSiteList

        ######################################
        # get available files
        siteSizeMap = {}
        siteSizeMapWT = {}
        availableFileMap = {}
        siteFilesMap = {}
        siteFilesMapWT = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(self.get_unified_sites(scanSiteList),
                                                                                self.siteMapper, JobUtils.PROD_PS,
                                                                                JobUtils.PROD_PS, ignore_cc=True)
                # disable file lookup for merge jobs or secondary datasets
                checkCompleteness = True
                useCompleteOnly = False
                if inputChunk.isMerging:
                    checkCompleteness = False
                if not datasetSpec.isMaster() or \
                        (taskSpec.ioIntensity and taskSpec.ioIntensity > self.io_intensity_cutoff):
                    useCompleteOnly = True
                # get available files per site/endpoint
                tmpLog.debug('getting available files for {0}'.format(datasetSpec.datasetName))
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            check_completeness=checkCompleteness,
                                                            storage_token=datasetSpec.storageToken,
                                                            complete_only=useCompleteOnly)
                tmpLog.debug('got')
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError('ddmIF.getAvailableFiles failed')
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception as e:
                tmpLog.error('failed to get available files with {0}'.format(str(e).replace('\n', ' ')))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
            # loop over all sites to get the size of available files
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                siteSizeMap.setdefault(tmpSiteName, 0)
                siteSizeMapWT.setdefault(tmpSiteName, 0)
                siteFilesMap.setdefault(tmpSiteName, set())
                siteFilesMapWT.setdefault(tmpSiteName, set())
                # get the total size of available files
                if tmpSiteName in availableFileMap[datasetSpec.datasetName]:
                    availableFiles = availableFileMap[datasetSpec.datasetName][tmpSiteName]
                    for tmpFileSpec in \
                            availableFiles['localdisk']+availableFiles['cache']:
                        if tmpFileSpec.lfn not in siteFilesMap[tmpSiteName]:
                            siteSizeMap[tmpSiteName] += tmpFileSpec.fsize
                            siteSizeMapWT[tmpSiteName] += tmpFileSpec.fsize
                        siteFilesMap[tmpSiteName].add(tmpFileSpec.lfn)
                        siteFilesMapWT[tmpSiteName].add(tmpFileSpec.lfn)
                    for tmpFileSpec in availableFiles['localtape']:
                        if tmpFileSpec.lfn not in siteFilesMapWT[tmpSiteName]:
                            siteSizeMapWT[tmpSiteName] += tmpFileSpec.fsize
                            siteFilesMapWT[tmpSiteName].add(tmpFileSpec.lfn)
        # get max total size
        allLFNs = set()
        totalSize = 0
        for datasetSpec in inputChunk.getDatasets():
            for fileSpec in datasetSpec.Files:
                if fileSpec.lfn not in allLFNs:
                    allLFNs.add(fileSpec.lfn)
                    try:
                        totalSize += fileSpec.fsize
                    except Exception:
                        pass
        # get max num of available files
        maxNumFiles = len(allLFNs)

        ######################################
        # selection for fileSizeToMove
        moveSizeCutoffGB = self.taskBufferIF.getConfigValue(COMPONENT, 'SIZE_CUTOFF_TO_MOVE_INPUT', APP, VO)
        if moveSizeCutoffGB is None:
            moveSizeCutoffGB = 10
        moveNumFilesCutoff = self.taskBufferIF.getConfigValue(COMPONENT, 'NUM_CUTOFF_TO_MOVE_INPUT', APP, VO)
        if moveNumFilesCutoff is None:
            moveNumFilesCutoff = 100
        if not sitePreAssigned and totalSize > 0 and not inputChunk.isMerging and taskSpec.ioIntensity is not None \
                and taskSpec.ioIntensity > self.io_intensity_cutoff and not (taskSpec.useEventService() \
                and not taskSpec.useJobCloning()):
            newScanSiteList = []
            newScanSiteListWT = []
            msgList = []
            msgListDT = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # file size to move in MB
                mbToMove = long((totalSize-siteSizeMap[tmpSiteName])/(1024*1024))
                nFilesToMove = maxNumFiles-len(siteFilesMap[tmpSiteName])
                mbToMoveWT = long((totalSize-siteSizeMapWT[tmpSiteName])/(1024*1024))
                nFilesToMoveWT = maxNumFiles-len(siteFilesMapWT[tmpSiteName])
                if min(mbToMove, mbToMoveWT) > moveSizeCutoffGB * 1024 or min(nFilesToMove, nFilesToMoveWT) > moveNumFilesCutoff:
                    tmpMsg = '  skip site={0} '.format(tmpSiteName)
                    if mbToMove > moveSizeCutoffGB * 1024:
                        tmpMsg += 'since size of missing input is too large ({0} GB > {1} GB) '.format(long(mbToMove/1024),
                                                                                                       moveSizeCutoffGB)
                    else:
                        tmpMsg += 'since the number of missing input files is too large ({0} > {1}) '.format(nFilesToMove,
                                                                                                             moveNumFilesCutoff)
                    tmpMsg += 'for IO intensive task ({0} > {1} kBPerS) '.format(taskSpec.ioIntensity, self.io_intensity_cutoff)
                    tmpMsg += 'criteria=-io'
                    msgListDT.append(tmpMsg)
                    continue
                if mbToMove > moveSizeCutoffGB * 1024 or nFilesToMove > moveNumFilesCutoff:
                    tmpMsg = '  skip site={0} '.format(tmpSiteName)
                    if mbToMove > moveSizeCutoffGB * 1024:
                        tmpMsg += 'since size of missing disk input is too large ({0} GB > {1} GB) '.format(long(mbToMove/1024),
                                                                                                            moveSizeCutoffGB)
                    else:
                        tmpMsg += 'since the number of missing disk input files is too large ({0} > {1}) '.format(nFilesToMove,
                                                                                                                  moveNumFilesCutoff)
                    tmpMsg += 'for IO intensive task ({0} > {1} kBPerS) '.format(taskSpec.ioIntensity, self.io_intensity_cutoff)
                    tmpMsg += 'criteria=-io'
                    msgList.append(tmpMsg)
                    newScanSiteListWT.append(tmpSiteName)
                else:
                    newScanSiteList.append(tmpSiteName)
            if len(newScanSiteList + newScanSiteListWT) == 0:
                # disable if no candidate
                tmpLog.info('disabled IO check since no candidate passed')
            else:
                for tmpMsg in msgListDT:
                    tmpLog.info(tmpMsg)
                if len(newScanSiteList) == 0:
                    # use candidates with TAPE replicas
                    newScanSiteList = newScanSiteListWT
                else:
                    for tmpMsg in msgList:
                        tmpLog.info(tmpMsg)
                oldScanSiteList = copy.copy(scanSiteList)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info('{0} candidates passed IO check'.format(len(scanSiteList)))
                self.add_summary_message(oldScanSiteList, scanSiteList, 'IO check')
                if scanSiteList == []:
                    self.dump_summary(tmpLog)
                    tmpLog.error('no candidates')
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError

        ######################################
        # temporary problems
        newScanSiteList = []
        oldScanSiteList = copy.copy(scanSiteList)
        for tmpSiteName in scanSiteList:
            if tmpSiteName in siteSkippedTmp:
                tmpLog.info(siteSkippedTmp[tmpSiteName])
            else:
                newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.info('{0} candidates passed temporary problem check'.format(len(scanSiteList)))
        self.add_summary_message(oldScanSiteList, scanSiteList, 'temporary problem check')
        if scanSiteList == []:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError

        ######################################
        # calculate weight
        jobStatPrioMapGS = dict()
        jobStatPrioMapGSOnly = dict()
        if workQueue.is_global_share:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        else:
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                         taskSpec.prodSourceLabel)
            if tmpSt:
                tmpSt, jobStatPrioMapGS = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
                tmpSt, jobStatPrioMapGSOnly = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo, True)
        if tmpSt:
            # count jobs per resource type
            tmpSt, tmpStatMapRT = self.taskBufferIF.getJobStatisticsByResourceTypeSite(workQueue)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        workerStat = self.taskBufferIF.ups_load_worker_stats()
        upsQueues = set(self.taskBufferIF.ups_get_queues())
        tmpLog.info('calculate weight and check cap for {0} candidates'.format(len(scanSiteList)))
        cutoffName = 'NQUEUELIMITSITE_{}'.format(taskSpec.gshare)
        cutOffValue = self.taskBufferIF.getConfigValue(COMPONENT, cutoffName,
                                                       APP, VO)
        if not cutOffValue:
            cutOffValue = 20
        else:
            tmpLog.info('using {}={} as lower limit for nQueued'.format(cutoffName, cutOffValue))
        weightMapPrimary = {}
        weightMapSecondary = {}
        weightMapJumbo = {}
        largestNumRun = None
        newScanSiteList = []
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            if not workQueue.is_global_share and esHigh and tmpSiteSpec.getJobSeed() == 'eshigh':
                tmp_wq_tag = wq_tag_global_share
                tmp_jobStatPrioMap = jobStatPrioMapGS
            else:
                tmp_wq_tag = wq_tag
                tmp_jobStatPrioMap = jobStatPrioMap
            nRunning   = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'running', None, tmp_wq_tag)
            nRunningAll = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'running', None, None)
            corrNumPilotStr = ''
            if not workQueue.is_global_share:
                # correction factor for nPilot
                nRunningGS = AtlasBrokerUtils.getNumJobs(jobStatPrioMapGS, tmpSiteName, 'running', None, wq_tag_global_share)
                nRunningGSOnly = AtlasBrokerUtils.getNumJobs(jobStatPrioMapGSOnly, tmpSiteName, 'running', None, wq_tag_global_share)
                corrNumPilot = float(nRunningGS - nRunningGSOnly + 1) / float(nRunningGS + 1)
                corrNumPilotStr = '*(nRunResourceQueue({0})+1)/(nRunGlobalShare({1})+1)'.format(nRunningGS - nRunningGSOnly, nRunningGS)
            else:
                corrNumPilot = 1
            nDefined   = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'defined', None, tmp_wq_tag) + self.getLiveCount(tmpSiteName)
            nAssigned  = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'assigned', None, tmp_wq_tag)
            nActivated = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'activated', None, tmp_wq_tag)
            nStarting  = AtlasBrokerUtils.getNumJobs(tmp_jobStatPrioMap, tmpSiteName, 'starting', None, tmp_wq_tag)
            if tmpSiteName in nPilotMap:
                nPilot = nPilotMap[tmpSiteName]
            else:
                nPilot = 0
            # get num workers
            nWorkers = 0
            nWorkersCutoff = 20
            if tmpSiteName in workerStat:
                for tmpHarvesterID, tmpLabelStat in iteritems(workerStat[tmpSiteName]):
                    for tmpHarvesterID, tmpResStat in iteritems(tmpLabelStat):
                        for tmpResType, tmpCounts in iteritems(tmpResStat):
                            for tmpStatus, tmpNum in iteritems(tmpCounts):
                                if tmpStatus in ['running', 'submitted']:
                                    nWorkers += tmpNum
                # cap
                nWorkers = min(nWorkersCutoff, nWorkers)
            # use nWorkers to bootstrap
            if nPilot > 0 and nRunning < nWorkersCutoff and nWorkers > nRunning and tmpSiteName in upsQueues \
                    and taskSpec.currentPriority <= self.max_prio_for_bootstrap:
                tmpLog.debug(
                    'using nWorkers={0} as nRunning at {1} since original nRunning={2} is low'.format(nWorkers,
                                                                                                      tmpPseudoSiteName,
                                                                                                      nRunning))
                nRunning = nWorkers
            # take into account the number of standby jobs
            numStandby = tmpSiteSpec.getNumStandby(wq_tag, taskSpec.resource_type)
            if numStandby is None:
                pass
            elif taskSpec.currentPriority > self.max_prio_for_bootstrap:
                # don't use numSlots for high prio tasks
                tmpLog.debug('ignored numSlots at {} due to prio={} > {}'.format(tmpPseudoSiteName,
                                                                                 taskSpec.currentPriority,
                                                                                 self.max_prio_for_bootstrap))
            elif numStandby == 0:
                # use the number of starting jobs as the number of standby jobs
                nRunning = nStarting+nRunning
                tmpLog.debug('using nStarting+nRunning at {} to set nRunning={} due to numSlot={}'.format(
                    tmpPseudoSiteName, nRunning, numStandby))
            else:
                # the number of standby jobs is defined
                nRunning = max(int(numStandby/tmpSiteSpec.coreCount), nRunning)
                tmpLog.debug('using numSlots={1}/coreCount at {0} to set nRunning={2}'.format(tmpPseudoSiteName,
                                                                                              numStandby, nRunning))
            manyAssigned = float(nAssigned + 1) / float(nActivated + 1)
            manyAssigned = min(2.0,manyAssigned)
            manyAssigned = max(1.0,manyAssigned)
            # take into account nAssigned when jobs need input data transfer
            if tmpSiteName not in siteSizeMap or siteSizeMap[tmpSiteName] >= totalSize:
                useAssigned = False
            else:
                useAssigned = True
            # stat with resource type
            RT_Cap = 2
            if tmpSiteName in tmpStatMapRT and taskSpec.resource_type in tmpStatMapRT[tmpSiteName]:
                useCapRT = True
                tmpRTrunning = tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get('running', 0)
                tmpRTrunning = max(tmpRTrunning, nRunning)
                tmpRTqueue = tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get('defined', 0)
                if useAssigned:
                    tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get('assigned', 0)
                tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get('activated', 0)
                tmpRTqueue += tmpStatMapRT[tmpSiteName][taskSpec.resource_type].get('starting', 0)
            else:
                useCapRT = False
                tmpRTqueue = 0
                tmpRTrunning = 0
            if totalSize == 0 or totalSize-siteSizeMap[tmpSiteName] <= 0:
                weight = float(nRunning + 1) / float(nActivated + nStarting + nDefined + 10)
                weightStr = 'nRun={0} nAct={1} nStart={3} nDef={4} nPilot={7}{9} totalSizeMB={5} totalNumFiles={8} '\
                            'nRun_rt={10} nQueued_rt={11} '
            else:
                weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + nDefined + 10) / manyAssigned
                weightStr = 'nRun={0} nAct={1} nAss={2} nStart={3} nDef={4} manyAss={6} nPilot={7}{9} totalSizeMB={5} '\
                            'totalNumFiles={8} nRun_rt={10} nQueued_rt={11} '
            weightStr = weightStr.format(nRunning, nActivated, nAssigned,
                                         nStarting, nDefined,
                                         long(totalSize/1024/1024),
                                         manyAssigned, nPilot,
                                         maxNumFiles,
                                         corrNumPilotStr, tmpRTrunning, tmpRTqueue)
            # reduce weights by taking data availability into account
            skipRemoteData = False
            if totalSize > 0:
                # file size to move in MB
                mbToMove = long((totalSize-siteSizeMap[tmpSiteName])/(1024*1024))
                # number of files to move
                nFilesToMove = maxNumFiles-len(siteFilesMap[tmpSiteName])
                # consider size and # of files
                if tmpSiteSpec.use_only_local_data() and (mbToMove>0 or nFilesToMove>0):
                    skipRemoteData = True
                else:
                    weight = weight * (totalSize+siteSizeMap[tmpSiteName]) / totalSize / (nFilesToMove/100+1)
                    weightStr += 'fileSizeToMoveMB={0} nFilesToMove={1} '.format(mbToMove,nFilesToMove)
            # T1 weight
            if tmpSiteName in t1Sites+sitesShareSeT1:
                weight *= t1Weight
                weightStr += 't1W={0} '.format(t1Weight)
            if useT1Weight:
                weightStr += 'nRunningAll={} '.format(nRunningAll)
            # apply network metrics to weight
            if taskSpec.useWorldCloud() and nucleus:

                tmpAtlasSiteName = None
                try:
                    tmpAtlasSiteName = storageMapping[tmpSiteName]['default']
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
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName, tmpSiteName)
            # override attributes
            siteCandidateSpec.override_attribute('maxwdir', newMaxwdir.get(tmpSiteName))
            # set weight and params
            siteCandidateSpec.weight = weight
            siteCandidateSpec.nRunningJobs = nRunning
            siteCandidateSpec.nAssignedJobs = nAssigned
            # set available files
            for tmpDatasetName,availableFiles in iteritems(availableFileMap):
                if tmpSiteName in availableFiles:
                    siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]['localdisk'])
                    siteCandidateSpec.add_local_tape_files(availableFiles[tmpSiteName]['localtape'])
                    siteCandidateSpec.add_cache_files(availableFiles[tmpSiteName]['cache'])
                    siteCandidateSpec.add_remote_files(availableFiles[tmpSiteName]['remote'])
            # add files as remote since WAN access is allowed
            if taskSpec.allowInputWAN() and tmpSiteSpec.allowWanInputAccess():
                siteCandidateSpec.remoteProtocol = 'direct'
                for datasetSpec in inputChunk.getDatasets():
                    siteCandidateSpec.add_remote_files(datasetSpec.Files)
            # check if site is locked for WORLD
            lockedByBrokerage = False
            if taskSpec.useWorldCloud():
                lockedByBrokerage = self.checkSiteLock(taskSpec.vo, taskSpec.prodSourceLabel,
                                                       tmpPseudoSiteName, taskSpec.workQueue_ID, taskSpec.resource_type)
            # check cap with nRunning
            nPilot *= corrNumPilot
            cutOffFactor = 2
            if tmpSiteSpec.capability == 'ucore':
                if not inputChunk.isExpress():
                    siteCandidateSpec.nRunningJobsCap = max(cutOffValue, cutOffFactor * tmpRTrunning)
                siteCandidateSpec.nQueuedJobs = tmpRTqueue
            else:
                if not inputChunk.isExpress():
                    siteCandidateSpec.nRunningJobsCap = max(cutOffValue, cutOffFactor * nRunning)
                if useAssigned:
                    siteCandidateSpec.nQueuedJobs = nActivated + nAssigned + nStarting
                else:
                    siteCandidateSpec.nQueuedJobs = nActivated + nStarting
            if taskSpec.getNumJumboJobs() is None or not tmpSiteSpec.useJumboJobs():
                forJumbo = False
            else:
                forJumbo = True
            # OK message. Use jumbo as primary by default
            if not forJumbo:
                okMsg = '  use site={0} with weight={1} {2} criteria=+use'.format(tmpPseudoSiteName,weight,weightStr)
                okAsPrimay = False
            else:
                okMsg = '  use site={0} for jumbo jobs with weight={1} {2} criteria=+usejumbo'.format(
                    tmpPseudoSiteName,weight,weightStr)
                okAsPrimay = True
            # checks
            if lockedByBrokerage:
                ngMsg = '  skip site={0} due to locked by another brokerage '.format(tmpPseudoSiteName)
                ngMsg += 'criteria=-lock'
            elif skipRemoteData:
                ngMsg = '  skip site={0} due to non-local data '.format(tmpPseudoSiteName)
                ngMsg += 'criteria=-non_local'
            elif not inputChunk.isExpress() and tmpSiteSpec.capability != 'ucore' and \
                    siteCandidateSpec.nQueuedJobs > siteCandidateSpec.nRunningJobsCap:
                if not useAssigned:
                    ngMsg = '  skip site={0} weight={1} due to nDefined+nActivated+nStarting={2} '.format(
                        tmpPseudoSiteName, weight,
                        siteCandidateSpec.nQueuedJobs)
                    ngMsg += '(nAssigned ignored due to data locally available) '
                else:
                    ngMsg = '  skip site={0} weight={1} due to nDefined+nActivated+nAssigned+nStarting={2} '.format(
                        tmpPseudoSiteName, weight,
                        siteCandidateSpec.nQueuedJobs)
                ngMsg += 'greater than max({0},{1}*nRun) '.format(cutOffValue, cutOffFactor)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-cap'
            elif taskSpec.useWorldCloud() and self.nwActive and inputChunk.isExpress() \
                    and weightNw < self.nw_threshold * self.nw_weight_multiplier:
                ngMsg = '  skip site={0} due to low network weight for express task weightNw={1} threshold={2} '\
                    .format(tmpPseudoSiteName, weightNw, self.nw_threshold)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-lowNetworkWeight'
            elif useCapRT and tmpRTqueue > max(cutOffValue, tmpRTrunning * RT_Cap) and not inputChunk.isExpress():
                ngMsg = '  skip site={0} since '.format(tmpSiteName)
                if useAssigned:
                    ngMsg += 'nDefined_rt+nActivated_rt+nAssigned_rt+nStarting_rt={0} '.format(tmpRTqueue)
                else:
                    ngMsg += 'nDefined_rt+nActivated_rt+nStarting_rt={0} '.format(tmpRTqueue)
                    ngMsg += '(nAssigned_rt ignored due to data locally available) '
                ngMsg += 'with gshare+resource_type is greater than '
                ngMsg += 'max({0},{1}*nRun_rt={1}*{2}) '.format(cutOffValue, RT_Cap, tmpRTrunning)
                ngMsg += 'criteria=-cap_rt'
            elif nRunning + nActivated + nAssigned + nStarting + nDefined == 0 and \
                taskSpec.currentPriority <= self.max_prio_for_bootstrap and not inputChunk.isMerging and \
                    not inputChunk.isExpress():
                okMsg = '  use site={0} to bootstrap (no running or queued jobs) criteria=+use'.format(tmpPseudoSiteName)
                ngMsg = '  skip site={0} due to low weight '.format(tmpPseudoSiteName)
                ngMsg += 'weight={0} {1} '.format(weight, weightStr)
                ngMsg += 'criteria=-loweigh'
                okAsPrimay = True
                # set weight to 0 for subsequent processing
                weight = 0
                siteCandidateSpec.weight = weight
            else:
                if useT1Weight:
                    ngMsg = '  skip site={0} due to low total '.format(tmpPseudoSiteName)
                    ngMsg += 'nRunningAll={} for negative T1 weight '.format(nRunningAll)
                    ngMsg += 'criteria=-t1weight'
                    if not largestNumRun or largestNumRun[-1] < nRunningAll:
                        largestNumRun = (tmpPseudoSiteName, nRunningAll)
                        okAsPrimay = True
                        # copy primaries to secondary map
                        for tmpWeight, tmpCandidates in iteritems(weightMapPrimary):
                            weightMapSecondary.setdefault(tmpWeight, [])
                            weightMapSecondary[tmpWeight] += tmpCandidates
                        weightMapPrimary = {}
                else:
                    ngMsg = '  skip site={0} due to low weight '.format(tmpPseudoSiteName)
                    ngMsg += 'weight={} {} '.format(weight, weightStr)
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
            if weight not in weightMap:
                weightMap[weight] = []
            weightMap[weight].append((siteCandidateSpec, okMsg, ngMsg))
        # use only primary candidates
        weightMap = weightMapPrimary
        # use all weights
        weightRank = None
        # dump NG message
        for tmpWeight in weightMapSecondary.keys():
            for siteCandidateSpec,tmpOkMsg,tmpNgMsg in weightMapSecondary[tmpWeight]:
                tmpLog.info(tmpNgMsg)
        if weightMapPrimary == {}:
            tmpLog.info('available sites all capped')
        # add jumbo sites
        for weight,tmpList in iteritems(weightMapJumbo):
            if weight not in weightMap:
                weightMap[weight] = []
            for tmpItem in tmpList:
                weightMap[weight].append(tmpItem)
        # max candidates for WORLD
        if taskSpec.useWorldCloud():
            maxSiteCandidates = 10
        else:
            maxSiteCandidates = None
        newScanSiteList = []
        weightList = list(weightMap.keys())
        weightList.sort()
        weightList.reverse()
        # put 0 at the head of the list to give priorities bootstrap PQs
        if 0 in weightList:
            weightList.remove(0)
            weightList.insert(0, 0)
        for weightIdx,tmpWeight in enumerate(weightList):
            for siteCandidateSpec,tmpOkMsg,tmpNgMsg in weightMap[tmpWeight]:
                # candidates for jumbo jobs
                if taskSpec.getNumJumboJobs() is not None:
                    tmpSiteSpec = self.siteMapper.getSite(siteCandidateSpec.siteName)
                    if tmpSiteSpec.useJumboJobs():
                        # use site for jumbo jobs
                        tmpLog.info(tmpOkMsg)
                        inputChunk.addSiteCandidateForJumbo(siteCandidateSpec)
                        if inputChunk.useJumbo not in ['fake', 'only']:
                            continue
                # candidates for normal jobs
                if (weightRank is None or weightIdx < weightRank) and \
                        (maxSiteCandidates is None or len(newScanSiteList) < maxSiteCandidates):
                    # use site
                    tmpLog.info(tmpOkMsg)
                    newScanSiteList.append(siteCandidateSpec.siteName)
                    inputChunk.addSiteCandidate(siteCandidateSpec)
                else:
                    # dump NG message
                    tmpLog.info(tmpNgMsg)
        oldScanSiteList = copy.copy(scanSiteList)
        scanSiteList = newScanSiteList
        self.add_summary_message(oldScanSiteList, scanSiteList, 'final check')
        # final check
        if scanSiteList == []:
            self.dump_summary(tmpLog)
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # lock sites for WORLD
        if taskSpec.useWorldCloud():
            for tmpSiteName in scanSiteList:
                #self.lockSite(# FIXME)
                pass
        self.dump_summary(tmpLog, scanSiteList)
        # return
        tmpLog.info('done')
        return self.SC_SUCCEEDED, inputChunk
