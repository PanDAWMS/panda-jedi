import re
import sys
import copy
import random
import datetime

from six import iteritems

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils

from .JobBrokerBase import JobBrokerBase
from . import AtlasBrokerUtils

from pandaserver.dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer import JobUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

APP = 'jedi'
COMPONENT = 'jobbroker'
VO = 'atlas'

# brokerage for ATLAS analysis
class AtlasAnalJobBroker(JobBrokerBase):

    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        JobBrokerBase.__init__(self, ddmIF, taskBufferIF)
        self.dataSiteMap = {}

    # wrapper for return
    def sendLogMessage(self, tmpLog):
        # send info to logger
        tmpLog.bulkSendMsg('analy_brokerage')
        tmpLog.debug('sent')

    # main
    def doBrokerage(self, taskSpec, cloudName, inputChunk, taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID),
                            monToken='<jediTaskID={0} {1}>'.format(taskSpec.jediTaskID,
                                                                   datetime.datetime.utcnow().isoformat('/')))
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # new maxwdir
        newMaxwdir = {}
        # get primary site candidates
        sitePreAssigned = False
        siteListPreAssigned = False
        excludeList = []
        includeList = None
        scanSiteList = []
        # problematic sites
        problematic_sites_dict = {}
        # get list of site access
        siteAccessList = self.taskBufferIF.listSiteAccess(None, taskSpec.userName)
        siteAccessMap = {}
        for tmpSiteName,tmpAccess in siteAccessList:
            siteAccessMap[tmpSiteName] = tmpAccess
        # disable VP for merging and forceStaged
        if inputChunk.isMerging or taskSpec.avoid_vp():
            useVP = False
        else:
            useVP = True
        # get workQueue
        workQueue = self.taskBufferIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)

        # site limitation
        if taskSpec.useLimitedSites():
            if 'excludedSite' in taskParamMap:
                excludeList = taskParamMap['excludedSite']
                # str to list for task retry
                try:
                    if not isinstance(excludeList, list):
                        excludeList = excludeList.split(',')
                except Exception:
                    pass
            if 'includedSite' in taskParamMap:
                includeList = taskParamMap['includedSite']
                # str to list for task retry
                if includeList == '':
                    includeList = None
                try:
                    if not isinstance(includeList, list):
                        includeList = includeList.split(',')
                    siteListPreAssigned = True
                except Exception:
                    pass
        # loop over all sites
        for siteName,tmpSiteSpec in iteritems(self.siteMapper.siteSpecList):
            if tmpSiteSpec.type == 'analysis' or tmpSiteSpec.is_grandly_unified():
                scanSiteList.append(siteName)
        # preassigned
        preassignedSite = taskSpec.site
        if preassignedSite not in ['',None]:
            # site is pre-assigned
            if not self.siteMapper.checkSite(preassignedSite):
                # check ddm for unknown site
                includeList = []
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
                    if scope_input in tmpSiteSpec.ddm_endpoints_input and \
                        preassignedSite in tmpSiteSpec.ddm_endpoints_input[scope_input].all:
                        includeList.append(tmpSiteName)
                if not includeList:
                    includeList = None
                    tmpLog.info('site={0} is ignored since unknown'.format(preassignedSite))
                else:
                    tmpLog.info('site={0} is converted to {1}'.format(preassignedSite,
                                                                      ','.join(includeList)))
                preassignedSite = None
            else:
                tmpLog.info('site={0} is pre-assigned'.format(preassignedSite))
                sitePreAssigned = True
                if preassignedSite not in scanSiteList:
                    scanSiteList.append(preassignedSite)
        tmpLog.info('initial {0} candidates'.format(len(scanSiteList)))
        # allowed remote access protocol
        allowedRemoteProtocol = 'fax'
        # MP
        if taskSpec.coreCount is not None and taskSpec.coreCount > 1:
            # use MCORE only
            useMP = 'only'
        elif taskSpec.coreCount == 0:
            # use MCORE and normal
            useMP = 'any'
        else:
            # not use MCORE
            useMP = 'unuse'
        # get statistics of failures
        timeWindowForFC = self.taskBufferIF.getConfigValue('anal_jobbroker', 'TW_DONE_JOB_STAT', 'jedi', taskSpec.vo)
        if timeWindowForFC is None:
            timeWindowForFC = 6
        failureCounts = self.taskBufferIF.getFailureCountsForTask_JEDI(taskSpec.jediTaskID, timeWindowForFC)
        # two loops with/without data locality check
        scanSiteLists = [(copy.copy(scanSiteList), True)]
        if len(inputChunk.getDatasets()) > 0:
            if taskSpec.taskPriority >= 2000:
                if inputChunk.isMerging:
                    scanSiteLists.append((copy.copy(scanSiteList), False))
                else:
                    scanSiteLists = [(copy.copy(scanSiteList), False)]
            elif taskSpec.taskPriority > 1000:
                scanSiteLists.append((copy.copy(scanSiteList), False))
        retVal = None
        checkDataLocality = False
        scanSiteWoVP = []
        avoidVP = False
        for scanSiteList, checkDataLocality in scanSiteLists:
            if checkDataLocality:
                tmpLog.debug('!!! look for candidates WITH data locality check')
            else:
                tmpLog.debug('!!! look for candidates WITHOUT data locality check')
            ######################################
            # selection for data availability
            hasDDS = False
            dataWeight = {}
            ddsList = set()
            remoteSourceList = {}
            if inputChunk.getDatasets() != [] and checkDataLocality:
                oldScanSiteList = copy.copy(scanSiteList)
                oldScanUnifiedSiteList = self.get_unified_sites(oldScanSiteList)
                for datasetSpec in inputChunk.getDatasets():
                    datasetName = datasetSpec.datasetName
                    if datasetName not in self.dataSiteMap:
                        # get the list of sites where data is available
                        tmpLog.debug('getting the list of sites where {0} is available'.format(datasetName))
                        tmpSt,tmpRet = AtlasBrokerUtils.getAnalSitesWithData(self.get_unified_sites(scanSiteList),
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
                            # check if distributed
                            if tmpRet != {}:
                                isDistributed = True
                                for tmpMap in tmpRet.values():
                                    for tmpVal in tmpMap.values():
                                        if tmpVal['state'] == 'complete':
                                            isDistributed = False
                                            break
                                    if not isDistributed:
                                        break
                                if isDistributed or datasetName.endswith('/'):
                                    # check if really distributed
                                    isDistributed = self.ddmIF.isDistributedDataset(datasetName)
                                    if isDistributed or datasetName.endswith('/'):
                                        hasDDS = True
                                        datasetSpec.setDistributed()
                                        tmpLog.debug(' {0} is distributed'.format(datasetName))
                                        ddsList.add(datasetName)
                                        # disable VP since distributed datasets triggers transfers
                                        useVP = False
                                        avoidVP = True
                    # check if the data is available at somewhere
                    if self.dataSiteMap[datasetName] == {}:
                        for tmpSiteName in scanSiteList:
                            #tmpLog.info('  skip site={0} data is unavailable criteria=-input'.format(tmpSiteName))
                            pass
                        tmpLog.error('{0} is unavailable at any site'.format(datasetName))
                        retVal = retFatal
                        continue
                # get the list of sites where data is available
                scanSiteList = None
                scanSiteListOnDisk = None
                normFactor = 0
                for datasetName,tmpDataSite in iteritems(self.dataSiteMap):
                    normFactor += 1
                    useIncomplete = datasetName in ddsList
                    # get sites where replica is available
                    tmpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=True,
                                                                            use_incomplete=useIncomplete)
                    tmpDiskSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite,includeTape=False,
                                                                                use_vp=useVP,
                                                                                use_incomplete=useIncomplete)
                    tmpNonVpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=True,
                                                                                 use_vp=False,
                                                                                 use_incomplete=useIncomplete)
                    # get sites which can remotely access source sites
                    if inputChunk.isMerging or taskSpec.useLocalIO():
                        # disable remote access for merging
                        tmpSatelliteSites = {}
                    elif (not sitePreAssigned) or (sitePreAssigned and preassignedSite not in tmpSiteList):
                        tmpSatelliteSites = AtlasBrokerUtils.getSatelliteSites(tmpDiskSiteList,
                                                                               self.taskBufferIF,
                                                                               self.siteMapper,nSites=50,
                                                                               protocol=allowedRemoteProtocol)
                    else:
                        tmpSatelliteSites = {}
                    # make weight map for local
                    for tmpSiteName in tmpSiteList:
                        if tmpSiteName not in dataWeight:
                            dataWeight[tmpSiteName] = 0
                        # give more weight to disk
                        if tmpSiteName in tmpDiskSiteList:
                            dataWeight[tmpSiteName] += 1
                        else:
                            dataWeight[tmpSiteName] += 0.001
                    # make weight map for remote
                    for tmpSiteName,tmpWeightSrcMap in iteritems(tmpSatelliteSites):
                        # skip since local data is available
                        if tmpSiteName in tmpSiteList:
                            continue
                        tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                        # negative weight for remote access
                        wRemote = 50.0
                        if tmpSiteSpec.wansinklimit not in [0,None]:
                            wRemote /= float(tmpSiteSpec.wansinklimit)
                        # sum weight
                        if tmpSiteName not in dataWeight:
                            dataWeight[tmpSiteName] = float(tmpWeightSrcMap['weight'])/wRemote
                        else:
                            dataWeight[tmpSiteName] += float(tmpWeightSrcMap['weight'])/wRemote
                        # make remote source list
                        if tmpSiteName not in remoteSourceList:
                            remoteSourceList[tmpSiteName] = {}
                        remoteSourceList[tmpSiteName][datasetName] = tmpWeightSrcMap['source']
                    # first list
                    if scanSiteList is None:
                        scanSiteList = []
                        for tmpSiteName in tmpSiteList + list(tmpSatelliteSites.keys()):
                            if tmpSiteName not in oldScanUnifiedSiteList:
                                continue
                            if tmpSiteName not in scanSiteList:
                                scanSiteList.append(tmpSiteName)
                        scanSiteListOnDisk = set()
                        for tmpSiteName in tmpDiskSiteList + list(tmpSatelliteSites.keys()):
                            if tmpSiteName not in oldScanUnifiedSiteList:
                                continue
                            scanSiteListOnDisk.add(tmpSiteName)
                        scanSiteWoVP = tmpNonVpSiteList
                        continue
                    # pickup sites which have all data
                    newScanList = []
                    for tmpSiteName in tmpSiteList + list(tmpSatelliteSites.keys()):
                        if tmpSiteName in scanSiteList and tmpSiteName not in newScanList:
                            newScanList.append(tmpSiteName)
                    scanSiteList = newScanList
                    tmpLog.debug('{0} is available at {1} sites'.format(datasetName,len(scanSiteList)))
                    # pickup sites which have all data on DISK
                    newScanListOnDisk = set()
                    for tmpSiteName in tmpDiskSiteList + list(tmpSatelliteSites.keys()):
                        if tmpSiteName in scanSiteListOnDisk:
                            newScanListOnDisk.add(tmpSiteName)
                    scanSiteListOnDisk = newScanListOnDisk
                    # get common elements
                    scanSiteWoVP = list(set(scanSiteWoVP).intersection(tmpNonVpSiteList))
                    tmpLog.debug('{0} is available at {1} sites on DISK'.format(datasetName,len(scanSiteListOnDisk)))
                # check for preassigned
                if sitePreAssigned and preassignedSite not in scanSiteList:
                    scanSiteList = []
                    tmpLog.info('data is unavailable locally or remotely at preassigned site {0}'.format(preassignedSite))
                elif len(scanSiteListOnDisk) > 0:
                    # use only disk sites
                    scanSiteList = list(scanSiteListOnDisk)
                scanSiteList = self.get_pseudo_sites(scanSiteList, oldScanSiteList)
                # dump
                for tmpSiteName in oldScanSiteList:
                    if tmpSiteName not in scanSiteList:
                        #tmpLog.info('  skip site={0} data is unavailable criteria=-input'.format(tmpSiteName))
                        pass
                tmpLog.info('{0} candidates have input data'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retFatal
                    continue
            ######################################
            # selection for status
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # skip unified queues
                if tmpSiteSpec.is_unified:
                    continue
                # check site status
                skipFlag = False
                if tmpSiteSpec.status in ['offline']:
                    skipFlag = True
                elif tmpSiteSpec.status in ['brokeroff','test']:
                    if siteListPreAssigned:
                        pass
                    elif not sitePreAssigned:
                        skipFlag = True
                    elif preassignedSite not in [tmpSiteName, tmpSiteSpec.get_unified_name()]:
                        skipFlag = True
                if not skipFlag:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info('  skip site=%s due to status=%s criteria=-status' % (tmpSiteName,tmpSiteSpec.status))
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed site status check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue

            ######################################
            # selection for iointensity limits
            # get default disk IO limit from GDP config
            max_diskio_per_core_default = self.taskBufferIF.getConfigValue(COMPONENT, 'MAX_DISKIO_DEFAULT', APP, VO)
            if not max_diskio_per_core_default:
                max_diskio_per_core_default = 10 ** 10

            # get the current disk IO usage per site
            diskio_percore_usage = self.taskBufferIF.getAvgDiskIO_JEDI()
            unified_site_list = self.get_unified_sites(scanSiteList)
            newScanSiteList = []
            for tmpSiteName in unified_site_list:

                tmp_site_spec = self.siteMapper.getSite(tmpSiteName)

                # measured diskIO at queue
                diskio_usage_tmp = diskio_percore_usage.get(tmpSiteName, 0)

                # figure out queue or default limit
                if tmp_site_spec.maxDiskio and tmp_site_spec.maxDiskio > 0:
                    # there is a limit specified in AGIS
                    diskio_limit_tmp = tmp_site_spec.maxDiskio
                else:
                    # we need to use the default value from GDP Config
                    diskio_limit_tmp = max_diskio_per_core_default

                # normalize task diskIO by site corecount
                diskio_task_tmp = taskSpec.diskIO
                if taskSpec.diskIO is not None and taskSpec.coreCount not in [None, 0, 1] \
                        and tmp_site_spec.coreCount not in [None, 0]:
                    diskio_task_tmp = taskSpec.diskIO / tmp_site_spec.coreCount

                try:  # generate a log message parseable by logstash for monitoring
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
                    tmpLog.info('  skip site={0} due to diskIO overload criteria=-diskIO'.format(tmpSiteName))
                    continue

                newScanSiteList.append(tmpSiteName)

            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)

            tmpLog.info('{0} candidates passed diskIO check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
            ######################################
            # selection for VP
            if taskSpec.avoid_vp() or avoidVP:
                newScanSiteList = []
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    if not tmpSiteSpec.use_vp(JobUtils.ANALY_PS):
                        newScanSiteList.append(tmpSiteName)
                    else:
                        tmpLog.info('  skip site=%s to avoid VP' % tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed for avoidVP'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # selection for MP
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if useMP == 'any' or (useMP == 'only' and tmpSiteSpec.coreCount > 1) or \
                        (useMP =='unuse' and tmpSiteSpec.coreCount in [0,1,None]):
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.info('  skip site=%s due to core mismatch cores_site=%s <> cores_task=%s criteria=-cpucore' % \
                                    (tmpSiteName,tmpSiteSpec.coreCount,taskSpec.coreCount))
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed for useMP={1}'.format(len(scanSiteList),useMP))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # selection for GPU + architecture
            newScanSiteList = []
            jsonCheck = None
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                if tmpSiteSpec.isGPU() and not taskSpec.is_hpo_workflow():
                    if taskSpec.getArchitecture() in ['', None]:
                        tmpLog.info('  skip site={0} since architecture is required for GPU queues'.format(tmpSiteName))
                        continue
                    if jsonCheck is None:
                        jsonCheck = AtlasBrokerUtils.JsonSoftwareCheck(self.siteMapper)
                    siteListWithCMTCONFIG = [tmpSiteSpec.get_unified_name()]
                    siteListWithCMTCONFIG, sitesNoJsonCheck = jsonCheck.check(siteListWithCMTCONFIG, None,
                                                                              None, None,
                                                                              taskSpec.getArchitecture(),
                                                                              False, True)
                    siteListWithCMTCONFIG += self.taskBufferIF.checkSitesWithRelease(sitesNoJsonCheck,
                                                                                    cmtConfig=taskSpec.getArchitecture(),
                                                                                    onlyCmtConfig=True)
                    if len(siteListWithCMTCONFIG) == 0:
                        tmpLog.info('  skip site={0} since architecture={1} is unavailable'.format(tmpSiteName, taskSpec.getArchitecture()))
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.info('{0} candidates passed for architecture check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # selection for closed
            if not sitePreAssigned and not inputChunk.isMerging:
                newScanSiteList = []
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    if tmpSiteName in failureCounts and 'closed' in failureCounts[tmpSiteName]:
                        nClosed = failureCounts[tmpSiteName]['closed']
                        if nClosed > 0:
                            tmpLog.info('  skip site=%s due to n_closed=%s criteria=-closed' % \
                                            (tmpSiteName, nClosed))
                            continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info('{0} candidates passed for closed'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # selection for release
            if taskSpec.transHome is not None or \
                    (taskSpec.processingType is not None and taskSpec.processingType.endswith('jedi-cont')):
                useANY = True
                jsonCheck = AtlasBrokerUtils.JsonSoftwareCheck(self.siteMapper)
                unified_site_list = self.get_unified_sites(scanSiteList)
                if taskSpec.transHome is not None:
                    transHome = taskSpec.transHome
                else:
                    transHome = ''
                # remove AnalysisTransforms-
                transHome = re.sub('^[^-]+-*','',transHome)
                transHome = re.sub('_','-',transHome)
                if re.search('rel_\d+(\n|$)', transHome) is None and \
                        taskSpec.transHome not in ['AnalysisTransforms', None] and \
                        re.search('\d{4}-\d{2}-\d{2}T\d{4}$', transHome) is None and \
                        re.search('-\d+\.\d+\.\d+$', transHome) is None:
                    # cache is checked
                    siteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, "atlas",
                                                                       transHome.split('-')[0],
                                                                       transHome.split('-')[1],
                                                                       taskSpec.getArchitecture(),
                                                                       False, False,
                                                                       container_name=taskSpec.container_name,
                                                                       only_tags_fc=taskSpec.use_only_tags_fc())
                    siteListWithSW += self.taskBufferIF.checkSitesWithRelease(sitesNoJsonCheck,
                                                                              caches=transHome,
                                                                              cmtConfig=taskSpec.getArchitecture())
                elif (transHome == '' and taskSpec.transUses is not None) or \
                        (re.search('-\d+\.\d+\.\d+$',transHome) is not None and
                        (taskSpec.transUses is None or re.search('-\d+\.\d+$', taskSpec.transUses) is None)):
                    siteListWithSW = []
                    sitesNoJsonCheck = unified_site_list
                    # remove Atlas-
                    if taskSpec.transUses is not None:
                        transUses = taskSpec.transUses.split('-')[-1]
                    else:
                        transUses = None
                    if transUses is not None:
                        # release is checked
                        tmpSiteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, "atlas",
                                                                              "AtlasOffline",
                                                                              transUses,
                                                                              taskSpec.getArchitecture(),
                                                                              False, False,
                                                                              container_name=taskSpec.container_name,
                                                                              only_tags_fc=taskSpec.use_only_tags_fc())
                        siteListWithSW += tmpSiteListWithSW
                    if len(transHome.split('-')) == 2:
                        tmpSiteListWithSW, sitesNoJsonCheck = jsonCheck.check(sitesNoJsonCheck, "atlas",
                                                                              transHome.split('-')[0],
                                                                              transHome.split('-')[1],
                                                                              taskSpec.getArchitecture(),
                                                                              False, False,
                                                                              container_name=taskSpec.container_name,
                                                                              only_tags_fc=taskSpec.use_only_tags_fc())
                        siteListWithSW += tmpSiteListWithSW
                    if transUses is not None:
                        siteListWithSW += self.taskBufferIF.checkSitesWithRelease(sitesNoJsonCheck,
                                                                                  releases=transUses,
                                                                                  cmtConfig=taskSpec.getArchitecture())
                    siteListWithSW += self.taskBufferIF.checkSitesWithRelease(sitesNoJsonCheck,
                                                                              caches=transHome,
                                                                              cmtConfig=taskSpec.getArchitecture())
                else:
                    # nightlies or standalone
                    siteListWithCVMFS = self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                                                                                releases='CVMFS')
                    if taskSpec.getArchitecture() in ['', None]:
                        # architecture is not set
                        siteListWithCMTCONFIG = copy.copy(unified_site_list)
                    else:
                        siteListWithCMTCONFIG = \
                            self.taskBufferIF.checkSitesWithRelease(unified_site_list,
                            cmtConfig=taskSpec.getArchitecture(),
                            onlyCmtConfig=True)
                    if taskSpec.transHome is not None:
                        # CVMFS check for nightlies
                        siteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, "nightlies",
                                                                           None, None,
                                                                           taskSpec.getArchitecture(),
                                                                           True, False,
                                                                           container_name=taskSpec.container_name,
                                                                           only_tags_fc=taskSpec.use_only_tags_fc())
                        siteListWithSW += list(set(siteListWithCVMFS) & set(siteListWithCMTCONFIG))
                    else:
                        # no CVMFS check for standalone SW
                        siteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, None,
                                                                           None, None,
                                                                           taskSpec.getArchitecture(),
                                                                           False, True,
                                                                           container_name=taskSpec.container_name,
                                                                           only_tags_fc=taskSpec.use_only_tags_fc())
                        siteListWithSW += siteListWithCMTCONFIG
                newScanSiteList = []
                for tmpSiteName in unified_site_list:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # release check is disabled or release is available
                    if useANY and tmpSiteSpec.releases == ['ANY']:
                        newScanSiteList.append(tmpSiteName)
                    elif tmpSiteName in siteListWithSW:
                        newScanSiteList.append(tmpSiteName)
                    else:
                        # release is unavailable
                        tmpLog.info('  skip site=%s due to missing rel/cache %s:%s:%s criteria=-cache' % \
                                     (tmpSiteName,taskSpec.transUses,taskSpec.transHome,taskSpec.getArchitecture()))
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info('{0} candidates passed for SW {1}:{2}:{3}'.format(len(scanSiteList),
                                                                               taskSpec.transUses,
                                                                               taskSpec.transHome,
                                                                               taskSpec.getArchitecture()))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # selection for memory
            minRamCount = inputChunk.getMaxRamCount()
            minRamCount = JobUtils.compensate_ram_count(minRamCount)
            if minRamCount not in [0,None]:
                newScanSiteList = []
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # site max memory requirement
                    site_maxmemory = 0
                    if tmpSiteSpec.maxrss not in [0,None]:
                        site_maxmemory = tmpSiteSpec.maxrss
                    if site_maxmemory not in [0,None] and minRamCount != 0 and minRamCount > site_maxmemory:
                        tmpLog.info('  skip site={0} due to site RAM shortage. site_maxmemory={1} < job_minramcount={2} criteria=-lowmemory'.format(tmpSiteName,
                                                                                                              site_maxmemory,
                                                                                                              minRamCount))
                        continue
                    # site min memory requirement
                    site_minmemory = 0
                    if tmpSiteSpec.minrss not in [0,None]:
                        site_minmemory = tmpSiteSpec.minrss
                    if site_minmemory not in [0,None] and minRamCount != 0 and minRamCount < site_minmemory:
                        tmpLog.info('  skip site={0} due to job RAM shortage. site_minmemory={1} > job_minramcount={2} criteria=-highmemory'.format(tmpSiteName,
                                                                                                             site_minmemory,
                                                                                                             minRamCount))
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                ramUnit = taskSpec.ramUnit
                if ramUnit is None:
                    ramUnit = 'MB'
                tmpLog.info('{0} candidates passed memory check = {1} {2}'.format(len(scanSiteList),
                                                                                  minRamCount, ramUnit))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # selection for scratch disk
            tmpMaxAtomSize  = inputChunk.getMaxAtomSize()
            tmpEffAtomSize  = inputChunk.getMaxAtomSize(effectiveSize=True)
            tmpOutDiskSize  = taskSpec.getOutDiskSize()
            tmpWorkDiskSize = taskSpec.getWorkDiskSize()
            minDiskCountS = tmpOutDiskSize*tmpEffAtomSize + tmpWorkDiskSize + tmpMaxAtomSize
            minDiskCountS = minDiskCountS // 1024 // 1024
            maxSizePerJob = taskSpec.getMaxSizePerJob()
            if maxSizePerJob is None or inputChunk.isMerging:
                maxSizePerJob = None
            else:
                maxSizePerJob //= (1024 * 1024)
            # size for direct IO sites
            if taskSpec.useLocalIO():
                minDiskCountR = minDiskCountS
            else:
                minDiskCountR = tmpOutDiskSize*tmpEffAtomSize + tmpWorkDiskSize
                minDiskCountR = minDiskCountR // 1024 // 1024
            tmpLog.info('maxAtomSize={0} effectiveAtomSize={1} outDiskCount={2} workDiskSize={3}'.format(tmpMaxAtomSize,
                                                                                                          tmpEffAtomSize,
                                                                                                          tmpOutDiskSize,
                                                                                                          tmpWorkDiskSize))
            tmpLog.info('minDiskCountScratch={0} minDiskCountRemote={1} nGBPerJobInMB={2}'.format(minDiskCountS,
                                                                                                  minDiskCountR,
                                                                                                  maxSizePerJob))
            newScanSiteList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxwdir:
                    if JediCoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
                        minDiskCount = minDiskCountR
                        if maxSizePerJob is not None and not taskSpec.useLocalIO():
                            tmpMinDiskCountR = tmpOutDiskSize * maxSizePerJob + tmpWorkDiskSize
                            tmpMinDiskCountR /= (1024 * 1024)
                            if tmpMinDiskCountR > minDiskCount:
                                minDiskCount = tmpMinDiskCountR
                    else:
                        minDiskCount = minDiskCountS
                        if maxSizePerJob is not None and maxSizePerJob > minDiskCount:
                            minDiskCount = maxSizePerJob

                    # get site and task corecount to scale maxwdir
                    if tmpSiteSpec.coreCount in [None, 0, 1]:
                        site_cc = 1
                    else:
                        site_cc = tmpSiteSpec.coreCount

                    if taskSpec.coreCount in [None, 0, 1]:
                        task_cc = 1
                    else:
                        task_cc = site_cc

                    maxwdir_scaled = tmpSiteSpec.maxwdir * task_cc / site_cc

                    if minDiskCount > maxwdir_scaled:
                        tmpLog.info('  skip site={0} due to small scratch disk={1} < {2} criteria=-disk'.format(
                            tmpSiteName, maxwdir_scaled, minDiskCount))
                        continue
                    newMaxwdir[tmpSiteName] = maxwdir_scaled
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed scratch disk check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # selection for available space in SE
            newScanSiteList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                # check endpoint
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)
                tmpEndPoint = tmpSiteSpec.ddm_endpoints_output[scope_output].getEndPoint(tmpSiteSpec.ddm_output[scope_output])
                if tmpEndPoint is not None:
                    # free space must be >= 200GB
                    diskThreshold = 200
                    tmpSpaceSize = 0
                    if tmpEndPoint['space_expired'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_expired']
                    if tmpEndPoint['space_free'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_free']
                    if tmpSpaceSize < diskThreshold and 'skip_RSE_check' not in tmpSiteSpec.catchall:  # skip_RSE_check: exceptional bypass of RSEs without storage reporting
                        tmpLog.info('  skip site={0} due to disk shortage in SE {1} < {2}GB criteria=-disk'.format(tmpSiteName, tmpSpaceSize,
                                                                                                                   diskThreshold))
                        continue
                    # check if blacklisted
                    if tmpEndPoint['blacklisted'] == 'Y':
                        tmpLog.info('  skip site={0} since {1} is blacklisted in DDM criteria=-blacklist'.format(tmpSiteName, tmpSiteSpec.ddm_output[scope_output]))
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed SE space check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # selection for walltime
            minWalltime = taskSpec.walltime
            if minWalltime not in [0,None] and minWalltime > 0:
                minWalltime *= tmpEffAtomSize
                newScanSiteList = []
                for tmpSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    # check at the site
                    if tmpSiteSpec.maxtime != 0 and minWalltime > tmpSiteSpec.maxtime:
                        tmpLog.info('  skip site={0} due to short site walltime={1}(site upper limit) < {2} criteria=-shortwalltime'.format(tmpSiteName,
                                                                                                                tmpSiteSpec.maxtime,
                                                                                                                minWalltime))
                        continue
                    if tmpSiteSpec.mintime != 0 and minWalltime < tmpSiteSpec.mintime:
                        tmpLog.info('  skip site={0} due to short job walltime={1}(site lower limit) > {2} criteria=-longwalltime'.format(tmpSiteName,
                                                                                                               tmpSiteSpec.mintime,
                                                                                                               minWalltime))
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed walltime check ={1}{2}'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # selection for nPilot
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            newScanSiteList = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                # check at the site
                nPilot = 0
                if tmpSiteName in nWNmap:
                    nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
                if nPilot == 0 and taskSpec.prodSourceLabel not in ['test']:
                    tmpLog.info('  skip site=%s due to no pilot criteria=-nopilot' % tmpSiteName)
                    if not self.testMode:
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # check inclusion and exclusion
            newScanSiteList = []
            sitesForANY = []
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                autoSite = False
                # check exclusion
                if AtlasBrokerUtils.isMatched(tmpSiteName,excludeList):
                    tmpLog.info('  skip site={0} excluded criteria=-excluded'.format(tmpSiteName))
                    continue
                # check inclusion
                if includeList is not None and not AtlasBrokerUtils.isMatched(tmpSiteName,includeList):
                    if 'AUTO' in includeList:
                        autoSite = True
                    else:
                        tmpLog.info('  skip site={0} not included criteria=-notincluded'.format(tmpSiteName))
                        continue
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # limited access
                if tmpSiteSpec.accesscontrol == 'grouplist':
                    if tmpSiteSpec.sitename not in siteAccessMap or \
                            siteAccessMap[tmpSiteSpec.sitename] != 'approved':
                        tmpLog.info('  skip site={0} limited access criteria=-limitedaccess'.format(tmpSiteName))
                        continue
                # check cloud
                if taskSpec.cloud not in [None,'','any',tmpSiteSpec.cloud]:
                    tmpLog.info('  skip site={0} cloud mismatch criteria=-cloudmismatch'.format(tmpSiteName))
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
                    tmpLog.info('  skip site={0} not included criteria=-notincluded'.format(tmpSiteName))
            scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
            tmpLog.info('{0} candidates passed inclusion/exclusion/cloud'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            ######################################
            # sites already used by task
            tmpSt,sitesUsedByTask = self.taskBufferIF.getSitesUsedByTask_JEDI(taskSpec.jediTaskID)
            if not tmpSt:
                tmpLog.error('failed to get sites which already used by task')
                retVal = retTmpError
                continue
            sitesUsedByTask = self.get_unified_sites(sitesUsedByTask)
            ######################################
            # calculate weight
            tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
            if not tmpSt:
                tmpLog.error('failed to get job statistics with priority')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
            tmpSt, siteToRunRateMap = AtlasBrokerUtils.getSiteToRunRateStats(tbIF=self.taskBufferIF, vo=taskSpec.vo)
            if not tmpSt:
                tmpLog.error('failed to get site to-running rate')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
            # check for preassigned
            if sitePreAssigned:
                if preassignedSite not in scanSiteList and preassignedSite not in self.get_unified_sites(scanSiteList):
                    tmpLog.info("preassigned site {0} did not pass all tests".format(preassignedSite))
                    tmpLog.error('no candidates')
                    retVal = retFatal
                    continue
                else:
                    newScanSiteList = []
                    for tmpPseudoSiteName in scanSiteList:
                        tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                        tmpSiteName = tmpSiteSpec.get_unified_name()
                        if tmpSiteName != preassignedSite:
                            tmpLog.info('  skip site={0} non pre-assigned site criteria=-nonpreassigned'.format(
                                tmpPseudoSiteName))
                            continue
                        newScanSiteList.append(tmpSiteName)
                    scanSiteList = newScanSiteList
                tmpLog.info('{0} candidates passed preassigned check'.format(len(scanSiteList)))
            ######################################
            # selection for hospital
            newScanSiteList = []
            hasNormalSite = False
            for tmpSiteName in self.get_unified_sites(scanSiteList):
                if not tmpSiteName.endswith('_HOSPITAL'):
                    hasNormalSite = True
                    break
            if hasNormalSite:
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    # remove hospital
                    if tmpSiteName.endswith('_HOSPITAL'):
                        tmpLog.info('  skip site=%s due to hospital queue criteria=-hospital' % tmpSiteName)
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info('{0} candidates passed hospital check'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # cap with resource type
            if not sitePreAssigned:
                # count jobs per resource type
                tmpRet, tmpStatMap = self.taskBufferIF.getJobStatisticsByResourceTypeSite(workQueue)
                newScanSiteList = []
                RT_Cap = 2
                for tmpSiteName in self.get_unified_sites(scanSiteList):
                    tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                    tmpUnifiedName = tmpSiteSpec.get_unified_name()
                    if tmpUnifiedName in tmpStatMap and taskSpec.resource_type in tmpStatMap[tmpUnifiedName]:
                        tmpSiteStatMap = tmpStatMap[tmpUnifiedName][taskSpec.resource_type]
                        tmpRTrunning = tmpSiteStatMap.get('running', 0)
                        tmpRTqueue = tmpSiteStatMap.get('defined', 0)
                        tmpRTqueue += tmpSiteStatMap.get('assigned', 0)
                        tmpRTqueue += tmpSiteStatMap.get('activated', 0)
                        tmpRTqueue += tmpSiteStatMap.get('starting', 0)
                        if tmpRTqueue > max(20, tmpRTrunning * RT_Cap):
                            tmpMsg = '  skip site={0} '.format(tmpSiteName)
                            tmpMsg += 'since nQueue/max(20,nRun) with gshare+resource_type is '
                            tmpMsg += '{0}/max(20,{1}) > {2} '.format(tmpRTqueue, tmpRTrunning, RT_Cap)
                            tmpMsg += 'criteria=-cap_rt'
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = self.get_pseudo_sites(newScanSiteList, scanSiteList)
                tmpLog.info('{0} candidates passed for cap with gshare+resource_type check'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    self.sendLogMessage(tmpLog)
                    return retTmpError
            ######################################
            # selection for un-overloaded sites
            if not inputChunk.isMerging:
                newScanSiteList = []
                overloadedNonVP = []
                msgList = []
                msgListVP = []
                minQueue = self.taskBufferIF.getConfigValue('anal_jobbroker', 'OVERLOAD_MIN_QUEUE', 'jedi', taskSpec.vo)
                if minQueue is None:
                    minQueue = 20
                ratioOffset = self.taskBufferIF.getConfigValue('anal_jobbroker', 'OVERLOAD_RATIO_OFFSET', 'jedi',
                                                               taskSpec.vo)
                if ratioOffset is None:
                    ratioOffset = 1.2
                grandRatio = AtlasBrokerUtils.get_total_nq_nr_ratio(jobStatPrioMap, taskSpec.gshare)
                tmpLog.info('grand nQueue/nRunning ratio : {0}'.format(grandRatio))
                tmpLog.info('sites with non-VP data : {0}'.format(','.join(scanSiteWoVP)))
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    # get nQueue and nRunning
                    nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running', workQueue_tag=taskSpec.gshare)
                    nQueue = 0
                    for jobStatus in ['defined', 'assigned', 'activated', 'starting']:
                        nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, workQueue_tag=taskSpec.gshare)
                    # skip if overloaded
                    if nQueue > minQueue and \
                            (nRunning == 0 or float(nQueue) / float(nRunning) > grandRatio * ratioOffset):
                        tmpMsg = '  skip site={0} '.format(tmpPseudoSiteName)
                        tmpMsg += 'nQueue>minQueue({0}) and '.format(minQueue)
                        if nRunning == 0:
                            tmpMsg += 'nRunning=0 '
                            problematic_sites_dict.setdefault(tmpSiteName, set())
                            problematic_sites_dict[tmpSiteName].add('nQueue({0})>minQueue({1}) and nRunning=0'.format(nQueue, minQueue))
                        else:
                            tmpMsg += 'nQueue({0})/nRunning({1}) > grandRatio({2:.2f})*offset({3}) '.format(nQueue,
                                                                                                            nRunning,
                                                                                                            grandRatio,
                                                                                                            ratioOffset)
                        if tmpSiteName in scanSiteWoVP or checkDataLocality is False or inputChunk.getDatasets() == []:
                            tmpMsg += 'criteria=-overloaded'
                            overloadedNonVP.append(tmpPseudoSiteName)
                            msgListVP.append(tmpMsg)
                        else:
                            tmpMsg += 'and VP criteria=-overloaded_vp'
                            msgList.append(tmpMsg)
                    else:
                        newScanSiteList.append(tmpPseudoSiteName)
                if len(newScanSiteList) > 0:
                    scanSiteList = newScanSiteList
                    for tmpMsg in msgList+msgListVP:
                        tmpLog.info(tmpMsg)
                else:
                    scanSiteList = overloadedNonVP
                    for tmpMsg in msgList:
                        tmpLog.info(tmpMsg)
                tmpLog.info('{0} candidates passed overload check'.format(len(scanSiteList)))
                if not scanSiteList:
                    tmpLog.error('no candidates')
                    retVal = retTmpError
                    continue
            ######################################
            # skip sites where the user queues too much
            user_name = self.taskBufferIF.cleanUserID(taskSpec.userName)
            tmpSt, jobsStatsPerUser = AtlasBrokerUtils.getUsersJobsStats(   tbIF=self.taskBufferIF,
                                                                            vo=taskSpec.vo,
                                                                            prod_source_label=taskSpec.prodSourceLabel,
                                                                            cache_lifetime=60)
            if not tmpSt:
                tmpLog.error('failed to get users jobs statistics')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
            elif not inputChunk.isMerging:
                # parameters
                base_queue_length_per_pq = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'BASE_QUEUE_LENGTH_PER_PQ', 'jedi', taskSpec.vo)
                if base_queue_length_per_pq is None:
                    base_queue_length_per_pq = 100
                base_expected_wait_hour_on_pq = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'BASE_EXPECTED_WAIT_HOUR_ON_PQ', 'jedi', taskSpec.vo)
                if base_expected_wait_hour_on_pq is None:
                    base_expected_wait_hour_on_pq = 8
                base_default_queue_length_per_pq_user = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'BASE_DEFAULT_QUEUE_LENGTH_PER_PQ_USER', 'jedi', taskSpec.vo)
                if base_default_queue_length_per_pq_user is None:
                    base_default_queue_length_per_pq_user = 5
                base_queue_ratio_on_pq = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'BASE_QUEUE_RATIO_ON_PQ', 'jedi', taskSpec.vo)
                if base_queue_ratio_on_pq is None:
                    base_queue_ratio_on_pq = 0.05
                static_max_queue_running_ratio = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'STATIC_MAX_QUEUE_RUNNING_RATIO', 'jedi', taskSpec.vo)
                if static_max_queue_running_ratio is None:
                    static_max_queue_running_ratio = 2.0
                max_expected_wait_hour = self.taskBufferIF.getConfigValue(
                                                        'anal_jobbroker', 'MAX_EXPECTED_WAIT_HOUR', 'jedi', taskSpec.vo)
                if max_expected_wait_hour is None:
                    max_expected_wait_hour = 12.0
                # loop over sites
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    # get info about site
                    nRunning_pq_total = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running')
                    nRunning_pq_in_gshare = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running', workQueue_tag=taskSpec.gshare)
                    nQueue_pq_in_gshare = 0
                    for jobStatus in ['defined', 'assigned', 'activated', 'starting']:
                        nQueue_pq_in_gshare += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, workQueue_tag=taskSpec.gshare)
                    # get to-running-rate
                    try:
                        site_to_running_rate = siteToRunRateMap[tmpSiteName]
                        if isinstance(site_to_running_rate, dict):
                            site_to_running_rate = sum(site_to_running_rate.values())
                    except KeyError:
                        site_to_running_rate = 0
                    finally:
                        to_running_rate = nRunning_pq_in_gshare*site_to_running_rate/nRunning_pq_total if nRunning_pq_total > 0 else 0
                    # get conditions of the site whether to throttle
                    if nQueue_pq_in_gshare < base_queue_length_per_pq:
                        # not throttle since overall queue length of the site is not large enough
                        tmpLog.debug('not throttle on {0} since nQ({1}) < base queue length ({2})'.format(
                                        tmpSiteName, nQueue_pq_in_gshare, base_queue_length_per_pq))
                        continue
                    allowed_queue_length_from_wait_time = base_expected_wait_hour_on_pq*to_running_rate
                    if nQueue_pq_in_gshare < allowed_queue_length_from_wait_time:
                        # not statisfy since overall waiting time of the site is not long enough
                        tmpLog.debug('not throttle on {0} since nQ({1}) < {2:.3f} = toRunningRate({3:.3f} /hr) * base wait time ({4} hr)'.format(
                                        tmpSiteName, nQueue_pq_in_gshare, allowed_queue_length_from_wait_time,
                                        to_running_rate, base_expected_wait_hour_on_pq))
                        continue
                    # get user jobs stats under the gshare
                    try:
                        user_jobs_stats_map = jobsStatsPerUser[tmpSiteName][taskSpec.gshare][user_name]
                    except KeyError:
                        continue
                    else:
                        nQ_pq_user = user_jobs_stats_map['nQueue']
                        nR_pq_user = user_jobs_stats_map['nRunning']
                        nUsers_pq = len(jobsStatsPerUser[tmpSiteName][taskSpec.gshare])
                        try:
                            nR_pq = jobsStatsPerUser[tmpSiteName][taskSpec.gshare]['_total']['nRunning']
                        except KeyError:
                            nR_pq = nRunning_pq_in_gshare
                    # evaluate max nQueue per PQ
                    nQ_pq_limit_map = {
                            'base_limit': base_queue_length_per_pq,
                            'static_limit': static_max_queue_running_ratio*nR_pq,
                            'dynamic_limit': max_expected_wait_hour*to_running_rate,
                        }
                    max_nQ_pq = max(nQ_pq_limit_map.values())
                    # description for max nQueue per PQ
                    description_of_max_nQ_pq = 'max_nQ_pq({maximum:.3f}) '.format(maximum=max_nQ_pq)
                    for k, v in nQ_pq_limit_map.items():
                        if v == max_nQ_pq:
                            if k in ['base_limit']:
                                description_of_max_nQ_pq += '= {key} = BASE_QUEUE_LENGTH_PER_PQ({value})'.format(
                                                                    key=k, value=base_queue_length_per_pq)
                            elif k in ['static_limit']:
                                description_of_max_nQ_pq += '= {key} = STATIC_MAX_QUEUE_RUNNING_RATIO({value:.3f}) * nR_pq({nR_pq})'.format(
                                                                    key=k, value=static_max_queue_running_ratio, nR_pq=nR_pq)
                            elif k in ['dynamic_limit']:
                                description_of_max_nQ_pq += '= {key} = MAX_EXPECTED_WAIT_HOUR({value:.3f} hr) * toRunningRate_pq({trr:.3f} /hr)'.format(
                                                                    key=k, value=max_expected_wait_hour, trr=to_running_rate)
                            break
                    # evaluate fraction per user
                    user_fraction_map = {
                            'equal_distr': 1/nUsers_pq,
                            'prop_to_nR': nR_pq_user/nR_pq if nR_pq > 0 else 0,
                        }
                    max_user_fraction = max(user_fraction_map.values())
                    # description for max fraction per user
                    description_of_max_user_fraction = 'max_user_fraction({maximum:.3f}) '.format(maximum=max_user_fraction)
                    for k, v in user_fraction_map.items():
                        if v == max_user_fraction:
                            if k in ['equal_distr']:
                                description_of_max_user_fraction += '= {key} = 1 / nUsers_pq({nU})'.format(
                                                                    key=k, nU=nUsers_pq)
                            elif k in ['prop_to_nR']:
                                description_of_max_user_fraction += '= {key} = nR_pq_user({nR_pq_user}) / nR_pq({nR_pq})'.format(
                                                                    key=k, nR_pq_user=nR_pq_user, nR_pq=nR_pq)
                            break
                    # evaluate max nQueue per PQ per user
                    nQ_pq_user_limit_map = {
                            'constant_base_user_limit': base_default_queue_length_per_pq_user,
                            'ratio_base_user_limit': base_queue_ratio_on_pq*nR_pq,
                            'dynamic_user_limit': max_nQ_pq*max_user_fraction,
                        }
                    max_nQ_pq_user = max(nQ_pq_user_limit_map.values())
                    # description for max fraction per user
                    description_of_max_nQ_pq_user = 'max_nQ_pq_user({maximum:.3f}) '.format(maximum=max_nQ_pq_user)
                    for k, v in nQ_pq_user_limit_map.items():
                        if v == max_nQ_pq_user:
                            if k in ['constant_base_user_limit']:
                                description_of_max_nQ_pq_user += '= {key} = BASE_DEFAULT_QUEUE_LENGTH_PER_PQ_USER({value})'.format(
                                                                key=k, value=base_default_queue_length_per_pq_user)
                            elif k in ['ratio_base_user_limit']:
                                description_of_max_nQ_pq_user += '= {key} = BASE_QUEUE_RATIO_ON_PQ({value:.3f}) * nR_pq({nR_pq})'.format(
                                                                key=k, value=base_queue_ratio_on_pq, nR_pq=nR_pq)
                            elif k in ['dynamic_user_limit']:
                                description_of_max_nQ_pq_user += '= {key} = max_nQ_pq({max_nQ_pq:.3f}) * max_user_fraction({max_user_fraction:.3f})'.format(
                                                                key=k, max_nQ_pq=max_nQ_pq, max_user_fraction=max_user_fraction)
                                description_of_max_nQ_pq_user += ' , where {0} , and {1}'.format(description_of_max_nQ_pq, description_of_max_user_fraction)
                            break
                    # check
                    if nQ_pq_user > max_nQ_pq_user:
                        tmpMsg = ' consider {0} unsuitable for the user due to long queue of the user: '.format(tmpSiteName)
                        tmpMsg += 'nQ_pq_user({0}) > {1} '.format(nQ_pq_user, description_of_max_nQ_pq_user)
                        # view as problematic site in order to throttle
                        problematic_sites_dict.setdefault(tmpSiteName, set())
                        problematic_sites_dict[tmpSiteName].add(tmpMsg)
            ############
            # loop end
            if len(scanSiteList) > 0:
                retVal = None
                break
        # failed
        if retVal is not None:
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retVal
        # get list of available files
        availableFileMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # get list of site to be scanned
                tmpLog.debug('getting the list of available files for {0}'.format(datasetSpec.datasetName))
                fileScanSiteList = []
                for tmpPseudoSiteName in scanSiteList:
                    tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                    tmpSiteName = tmpSiteSpec.get_unified_name()
                    if tmpSiteName in fileScanSiteList:
                        continue
                    fileScanSiteList.append(tmpSiteName)
                    if tmpSiteName in remoteSourceList and datasetSpec.datasetName in remoteSourceList[tmpSiteName]:
                        for tmpRemoteSite in remoteSourceList[tmpSiteName][datasetSpec.datasetName]:
                            if tmpRemoteSite not in fileScanSiteList:
                                fileScanSiteList.append(tmpRemoteSite)
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(fileScanSiteList, self.siteMapper,
                                                                                JobUtils.ANALY_PS, JobUtils.ANALY_PS)
                # disable file lookup for merge jobs
                if inputChunk.isMerging:
                    checkCompleteness = False
                else:
                    checkCompleteness = True
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            check_completeness=checkCompleteness,
                                                            use_vp=useVP)
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError('ddmIF.getAvailableFiles failed')
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                # send info to logger
                self.sendLogMessage(tmpLog)
                return retTmpError
        # make data weight
        totalSize = 0
        totalNumFiles = 0
        totalDiskSizeMap = dict()
        totalTapeSizeMap = dict()
        for datasetSpec in inputChunk.getDatasets():
            totalNumFiles += len(datasetSpec.Files)
            for fileSpec in datasetSpec.Files:
                totalSize += fileSpec.fsize
            if datasetSpec.datasetName in availableFileMap:
                for tmpSiteName, tmpAvFileMap in iteritems(availableFileMap[datasetSpec.datasetName]):
                    totalDiskSizeMap.setdefault(tmpSiteName, 0)
                    totalTapeSizeMap.setdefault(tmpSiteName, 0)
                    for fileSpec in tmpAvFileMap['localdisk']:
                        totalDiskSizeMap[tmpSiteName] += fileSpec.fsize
                    for fileSpec in tmpAvFileMap['localtape']:
                        totalTapeSizeMap[tmpSiteName] += fileSpec.fsize
        totalSize //= (1024 * 1024 * 1024)
        tmpLog.info('totalInputSize={0} GB'.format(totalSize))
        for tmpSiteName in totalDiskSizeMap.keys():
            totalDiskSizeMap[tmpSiteName] //= (1024 * 1024 *1024)
        for tmpSiteName in totalTapeSizeMap.keys():
            totalTapeSizeMap[tmpSiteName] //= (1024 * 1024 *1024)
        ######################################
        # final procedure
        tmpLog.info('final {0} candidates'.format(len(scanSiteList)))
        weightMap = {}
        weightStr = {}
        candidateSpecList = []
        preSiteCandidateSpec = None
        basic_weight_comparison_map = {}
        for tmpPseudoSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running', workQueue_tag=taskSpec.gshare)
            nDefined   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'defined', workQueue_tag=taskSpec.gshare)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'assigned', workQueue_tag=taskSpec.gshare)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'activated', workQueue_tag=taskSpec.gshare)
            nStarting  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'starting', workQueue_tag=taskSpec.gshare)
            nFailed    = 0
            nClosed    = 0
            nFinished  = 0
            if tmpSiteName in failureCounts:
                if 'failed' in failureCounts[tmpSiteName]:
                    nFailed = failureCounts[tmpSiteName]['failed']
                if 'closed' in failureCounts[tmpSiteName]:
                    nClosed = failureCounts[tmpSiteName]['closed']
                if 'finished' in failureCounts[tmpSiteName]:
                    nFinished = failureCounts[tmpSiteName]['finished']
            # problematic sites with too many failed and closed jobs
            if not inputChunk.isMerging and nFailed + nClosed > 2*nFinished:
                problematic_sites_dict.setdefault(tmpSiteName, set())
                problematic_sites_dict[tmpSiteName].add('too many failed or closed jobs for last 6h')
            # calculate weight
            orig_basic_weight = float(nRunning + 1) / float(nActivated + nAssigned + nDefined + nStarting + 1)
            weight = orig_basic_weight
            try:
                site_to_running_rate = siteToRunRateMap[tmpSiteName]
                if isinstance(site_to_running_rate, dict):
                    site_to_running_rate = sum(site_to_running_rate.values())
            except KeyError:
                to_running_rate_str = '0(unknown)'
                to_running_rate = 0
            else:
                site_n_running = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running')
                to_running_rate = nRunning*site_to_running_rate/site_n_running if site_n_running > 0 else 0
                to_running_rate_str = '{0:.3f}'.format(to_running_rate)
            nThrottled = 0
            if tmpSiteName in remoteSourceList:
                nThrottled = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'throttled', workQueue_tag=taskSpec.gshare)
                weight /= float(nThrottled + 1)
            # normalize weights by taking data availability into account
            diskNorm = 10
            tapeNorm = 1000
            localSize = totalSize
            if checkDataLocality:
                tmpDataWeight = 1
                if tmpSiteName in dataWeight:
                    weight *= dataWeight[tmpSiteName]
                    tmpDataWeight = dataWeight[tmpSiteName]
            else:
                tmpDataWeight = 1
                if totalSize > 0:
                    if tmpSiteName in totalDiskSizeMap:
                        tmpDataWeight += (totalDiskSizeMap[tmpSiteName] / diskNorm)
                        localSize = totalDiskSizeMap[tmpSiteName]
                    elif tmpSiteName in totalTapeSizeMap:
                        tmpDataWeight += (totalTapeSizeMap[tmpSiteName] / tapeNorm)
                        localSize = totalTapeSizeMap[tmpSiteName]
                weight *= tmpDataWeight
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName, tmpSiteName)
            # preassigned
            if sitePreAssigned and tmpSiteName == preassignedSite:
                preSiteCandidateSpec = siteCandidateSpec
            # override attributes
            siteCandidateSpec.override_attribute('maxwdir', newMaxwdir.get(tmpSiteName))
            # available site, take in account of new basic weight
            basic_weight_comparison_map[tmpSiteName] = {}
            basic_weight_comparison_map[tmpSiteName]['orig'] = orig_basic_weight
            basic_weight_comparison_map[tmpSiteName]['trr'] = to_running_rate
            basic_weight_comparison_map[tmpSiteName]['nq'] = (nActivated + nAssigned + nDefined + nStarting)
            basic_weight_comparison_map[tmpSiteName]['nr'] = nRunning
            # set weight
            siteCandidateSpec.weight = weight
            tmpStr  = 'weight={0:.3f} nRunning={1} nDefined={2} nActivated={3} nStarting={4} nAssigned={5} '.format(weight,
                                                                                                                nRunning,
                                                                                                                nDefined,
                                                                                                                nActivated,
                                                                                                                nStarting,
                                                                                                                nAssigned)
            tmpStr += 'nFailed={0} nClosed={1} nFinished={2} dataW={3} '.format(nFailed,
                                                                                nClosed,
                                                                                nFinished,
                                                                                tmpDataWeight)
            tmpStr += 'totalInGB={0} localInGB={1} nFiles={2} '.format(totalSize, localSize, totalNumFiles)
            tmpStr += 'toRunningRate={0} '.format(to_running_rate_str)
            weightStr[tmpPseudoSiteName] = tmpStr
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if weight not in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)
        ## compute new basic weight
        try:
            weight_comparison_avail_sites = set(basic_weight_comparison_map.keys())
            trr_sum = 0
            nq_sum = 0
            n_avail_sites = len(basic_weight_comparison_map)
            for vv in basic_weight_comparison_map.values():
                trr_sum += vv['trr']
                nq_sum += vv['nq']
            if n_avail_sites == 0:
                tmpLog.debug('WEIGHT-COMPAR: zero available sites, skip')
            elif trr_sum == 0:
                tmpLog.debug('WEIGHT-COMPAR: zero sum of to-running-rate, skip')
            else:
                _found_weights = False
                while not _found_weights:
                    trr_sum_avail = 0
                    nq_sum_avail = 0
                    n_avail_sites = len(weight_comparison_avail_sites)
                    if n_avail_sites == 0:
                        break
                    for site in weight_comparison_avail_sites:
                        vv = basic_weight_comparison_map[site]
                        trr_sum_avail += vv['trr']
                        nq_sum_avail += vv['nq']
                    if trr_sum_avail == 0:
                        break
                    _found_weights = True
                    for site in list(weight_comparison_avail_sites):
                        vv = basic_weight_comparison_map[site]
                        new_basic_weight = (vv['trr']/trr_sum_avail)*(25 + nq_sum_avail - n_avail_sites/2.0) - vv['nq'] + 1/2.0
                        if new_basic_weight < 0:
                            vv['new'] = 0
                            weight_comparison_avail_sites.discard(site)
                            _found_weights = False
                        else:
                            vv['new'] = new_basic_weight
                orig_sum = 0
                new_sum = 0
                for vv in basic_weight_comparison_map.values():
                    orig_sum += vv['orig']
                    new_sum += vv['new']
                for site in basic_weight_comparison_map:
                    vv = basic_weight_comparison_map[site]
                    if vv['nr'] == 0:
                        trr_over_r = None
                    else:
                        trr_over_r = vv['trr']/vv['nr']
                    vv['trr_over_r'] = '{:6.3f}'.format(trr_over_r) if trr_over_r is not None else 'None'
                    if orig_sum == 0:
                        normalized_orig = 0
                    else:
                        normalized_orig = vv['orig']/orig_sum
                    vv['normalized_orig'] = normalized_orig
                    if new_sum == 0:
                        normalized_new = 0
                    else:
                        normalized_new = vv['new']/new_sum
                    vv['normalized_new'] = normalized_new
                prt_str_list = []
                prt_str_temp = ('    '
                                ' {site:>24} |'
                                ' {nq:>6} |'
                                ' {nr:>6} |'
                                ' {trr:9.3f} |'
                                ' {trr_over_r} |'
                                ' {orig:9.3f} |'
                                ' {new:9.3f} |'
                                ' {normalized_orig:6.1%} |'
                                ' {normalized_new:6.1%} |')
                prt_str_title = (   '    '
                                    ' {site:>24} |'
                                    ' {nq:>6} |'
                                    ' {nr:>6} |'
                                    ' {trr:>9} |'
                                    ' {trr_over_r:>6} |'
                                    ' {orig:>9} |'
                                    ' {new:>9} |'
                                    ' {normalized_orig:>6} |'
                                    ' {normalized_new:>6} |'
                                    ).format(
                                        site='Site',
                                        nq='Q',
                                        nr='R',
                                        trr='TRR',
                                        trr_over_r='TRR/R',
                                        orig='Wb_orig',
                                        new='Wb_new',
                                        normalized_orig='orig_%',
                                        normalized_new='new_%')
                prt_str_list.append(prt_str_title)
                for site in sorted(basic_weight_comparison_map):
                    vv = basic_weight_comparison_map[site]
                    prt_str = prt_str_temp.format(site=site, **vv)
                    prt_str_list.append(prt_str)
                tmpLog.debug('WEIGHT-COMPAR: for gshare={0} got \n{1}'.format(taskSpec.gshare, '\n'.join(prt_str_list)))
        except Exception as e:
            tmpLog.error('{0} {1}'.format(e.__class__.__name__, e))
        ##
        oldScanSiteList = copy.copy(scanSiteList)
        # sort candidates by weights
        weightList = list(weightMap.keys())
        weightList.sort()
        weightList.reverse()
        for weightVal in weightList:
            sitesWithWeight = weightMap[weightVal]
            random.shuffle(sitesWithWeight)
            candidateSpecList += sitesWithWeight
        # limit the number of sites. use all sites for distributed datasets
        if not hasDDS:
            maxNumSites = 10
        else:
            maxNumSites = None
        # remove problematic sites
        candidateSpecList = AtlasBrokerUtils.skipProblematicSites(candidateSpecList,
                                                                  set(problematic_sites_dict),
                                                                  sitesUsedByTask,
                                                                  preSiteCandidateSpec,
                                                                  maxNumSites,
                                                                  timeWindowForFC,
                                                                  tmpLog)
        # append preassigned
        if sitePreAssigned and preSiteCandidateSpec is not None and preSiteCandidateSpec not in candidateSpecList:
            candidateSpecList.append(preSiteCandidateSpec)
        # collect site names
        scanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            scanSiteList.append(siteCandidateSpec.siteName)
        # append candidates
        newScanSiteList = []
        msgList = []
        for siteCandidateSpec in candidateSpecList:
            tmpPseudoSiteName = siteCandidateSpec.siteName
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # preassigned
            if sitePreAssigned and tmpSiteName != preassignedSite:
                tmpLog.info('  skip site={0} non pre-assigned site criteria=-nonpreassigned'.format(tmpPseudoSiteName))
                try:
                    del weightStr[tmpPseudoSiteName]
                except Exception:
                    pass
                continue
            # set available files
            if inputChunk.getDatasets() == [] or (not checkDataLocality and not tmpSiteSpec.use_only_local_data()):
                isAvailable = True
            else:
                isAvailable = False
            for tmpDatasetName,availableFiles in iteritems(availableFileMap):
                tmpDatasetSpec = inputChunk.getDatasetWithName(tmpDatasetName)
                # check remote files
                if tmpSiteName in remoteSourceList and tmpDatasetName in remoteSourceList[tmpSiteName] \
                        and not tmpSiteSpec.use_only_local_data():
                    for tmpRemoteSite in remoteSourceList[tmpSiteName][tmpDatasetName]:
                        if tmpRemoteSite in availableFiles and \
                                len(tmpDatasetSpec.Files) <= len(availableFiles[tmpRemoteSite]['localdisk']):
                            # use only remote disk files
                            siteCandidateSpec.remoteFiles += availableFiles[tmpRemoteSite]['localdisk']
                            # set remote site and access protocol
                            siteCandidateSpec.remoteProtocol = allowedRemoteProtocol
                            siteCandidateSpec.remoteSource   = tmpRemoteSite
                            isAvailable = True
                            break
                # local files
                if tmpSiteName in availableFiles:
                    if len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['localdisk']) or \
                            len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['cache']) or \
                            len(tmpDatasetSpec.Files) <= len(availableFiles[tmpSiteName]['localtape']) or \
                            (tmpDatasetSpec.isDistributed() and len(availableFiles[tmpSiteName]['all']) > 0) or \
                            (checkDataLocality is False and not tmpSiteSpec.use_only_local_data()):
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['localdisk']
                        # add cached files to local list since cached files go to pending when reassigned
                        siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.localTapeFiles  += availableFiles[tmpSiteName]['localtape']
                        siteCandidateSpec.cacheFiles  += availableFiles[tmpSiteName]['cache']
                        siteCandidateSpec.remoteFiles += availableFiles[tmpSiteName]['remote']
                        siteCandidateSpec.addAvailableFiles(availableFiles[tmpSiteName]['all'])
                        isAvailable = True
                    else:
                        tmpMsg = '{0} is incomplete at {1} : nFiles={2} nLocal={3} nCached={4} nTape={5}'
                        tmpLog.debug(tmpMsg.format(tmpDatasetName,
                                                   tmpPseudoSiteName,
                                                   len(tmpDatasetSpec.Files),
                                                   len(availableFiles[tmpSiteName]['localdisk']),
                                                   len(availableFiles[tmpSiteName]['cache']),
                                                   len(availableFiles[tmpSiteName]['localtape']),
                                                   ))
                if not isAvailable:
                    break
            # append
            if not isAvailable:
                tmpLog.info('  skip site={0} file unavailable criteria=-fileunavailable'.format(siteCandidateSpec.siteName))
                try:
                    del weightStr[siteCandidateSpec.siteName]
                except Exception:
                    pass
                continue
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpMsg = '  use site={0} with {1} nLocalDisk={2} nLocalTape={3} nCache={4} nRemote={5} criteria=+use'.format(siteCandidateSpec.siteName,
                                                                                                                         weightStr[siteCandidateSpec.siteName],
                                                                                                                         len(siteCandidateSpec.localDiskFiles),
                                                                                                                         len(siteCandidateSpec.localTapeFiles),
                                                                                                                         len(siteCandidateSpec.cacheFiles),
                                                                                                                         len(siteCandidateSpec.remoteFiles))
            msgList.append(tmpMsg)
            del weightStr[siteCandidateSpec.siteName]
        # dump
        for tmpPseudoSiteName in oldScanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            tmpWeightStr = None
            if tmpSiteName in weightStr:
                tmpWeightStr = weightStr[tmpSiteName]
            elif tmpPseudoSiteName in weightStr:
                tmpWeightStr = weightStr[tmpPseudoSiteName]
            if tmpWeightStr is not None:
                if tmpSiteName in problematic_sites_dict:
                    bad_reasons = ' ; '.join(list(problematic_sites_dict[tmpSiteName]))
                    tmpMsg = '  skip site={0} {1} ; with {2} criteria=-badsite'.format(tmpPseudoSiteName, bad_reasons,
                                                                                       tmpWeightStr)
                else:
                    tmpMsg = ('  skip site={0} due to low weight and not-used by old jobs '
                              'with {1} criteria=-lowweight').format(tmpPseudoSiteName, tmpWeightStr)
                tmpLog.info(tmpMsg)
        for tmpMsg in msgList:
            tmpLog.info(tmpMsg)
        scanSiteList = newScanSiteList
        if not scanSiteList:
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
