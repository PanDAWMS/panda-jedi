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
from pandaserver.dataservice.DataServiceUtils import select_scope
from . import AtlasBrokerUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

APP = 'jedi'
COMPONENT = 'jobbroker'
VO = 'atlas'

# brokerage for ATLAS analysis
class AtlasAnalJobBroker (JobBrokerBase):

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
        excludeList = []
        includeList = None
        scanSiteList = []
        # get list of site access
        siteAccessList = self.taskBufferIF.listSiteAccess(None, taskSpec.userName)
        siteAccessMap = {}
        for tmpSiteName,tmpAccess in siteAccessList:
            siteAccessMap[tmpSiteName] = tmpAccess
        # disable VP for merging and forceStaged
        if inputChunk.isMerging:
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
                except Exception:
                    pass
        # loop over all sites
        for siteName,tmpSiteSpec in iteritems(self.siteMapper.siteSpecList):
            # TODO prodanaly: way to identify production queues running analysis
            if tmpSiteSpec.type == 'analysis' or tmpSiteSpec.is_grandly_unified():
                scanSiteList.append(siteName)
        # preassigned
        if taskSpec.site not in ['',None]:
            # site is pre-assigned
            tmpLog.info('site={0} is pre-assigned'.format(taskSpec.site))
            sitePreAssigned = True
            if taskSpec.site not in scanSiteList:
                scanSiteList.append(taskSpec.site)
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
        for scanSiteList, checkDataLocality in scanSiteLists:
            if checkDataLocality:
                tmpLog.debug('!!! look for candidates WITH data locality check')
            else:
                tmpLog.debug('!!! look for candidates WITHOUT data locality check')
            ######################################
            # selection for data availability
            hasDDS = False
            dataWeight = {}
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
                                if isDistributed:
                                    # check if really distributed
                                    isDistributed = self.ddmIF.isDistributedDataset(datasetName)
                                    if isDistributed:
                                        hasDDS = True
                                        datasetSpec.setDistributed()
                                        tmpLog.debug(' {0} is distributed'.format(datasetName))
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
                    # get sites where replica is available
                    tmpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite,includeTape=True)
                    tmpDiskSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite,includeTape=False,
                                                                                use_vp=useVP)
                    tmpNonVpSiteList = AtlasBrokerUtils.getAnalSitesWithDataDisk(tmpDataSite, includeTape=True,
                                                                                 use_vp=False)
                    # get sites which can remotely access source sites
                    if inputChunk.isMerging or taskSpec.useLocalIO():
                        # disable remote access for merging
                        tmpSatelliteSites = {}
                    elif (not sitePreAssigned) or (sitePreAssigned and taskSpec.site not in tmpSiteList):
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
                if sitePreAssigned and taskSpec.site not in scanSiteList:
                    scanSiteList = []
                    tmpLog.info('data is unavailable locally or remotely at preassigned site {0}'.format(taskSpec.site))
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
                    if not sitePreAssigned:
                        skipFlag = True
                    elif taskSpec.site not in [tmpSiteName, tmpSiteSpec.get_unified_name()]:
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
                if tmpSiteSpec.isGPU():
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
                                                                       False, False)
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
                                                                              False, False)
                        siteListWithSW += tmpSiteListWithSW
                    if len(transHome.split('-')) == 2:
                        tmpSiteListWithSW, sitesNoJsonCheck = jsonCheck.check(sitesNoJsonCheck, "atlas",
                                                                              transHome.split('-')[0],
                                                                              transHome.split('-')[1],
                                                                              taskSpec.getArchitecture(),
                                                                              False, False)
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
                    useANY = False
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
                                                                           True, False)
                        siteListWithSW += list(set(siteListWithCVMFS) & set(siteListWithCMTCONFIG))
                    else:
                        # no CVMFS check for standalone SW
                        siteListWithSW, sitesNoJsonCheck = jsonCheck.check(unified_site_list, None,
                                                                           None, None,
                                                                           taskSpec.getArchitecture(),
                                                                           False, True)
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
            minRamCount = JediCoreUtils.compensateRamCount(minRamCount)
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
                if tmpSiteSpec.maxwdir != 0:
                    if tmpSiteSpec.isDirectIO():
                        minDiskCount = minDiskCountR
                        if maxSizePerJob is not None and not taskSpec.useLocalIO():
                            tmpMinDiskCountR = tmpOutDiskSize * maxSizePerJob  + tmpWorkDiskSize
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
                scope_input, scope_output = select_scope(tmpSiteSpec, 'user')
                tmpEndPoint = tmpSiteSpec.ddm_endpoints_output[scope_output].getEndPoint(tmpSiteSpec.ddm_output[scope_output])
                if tmpEndPoint is not None:
                    # free space must be >= 200GB
                    diskThreshold = 200
                    tmpSpaceSize = 0
                    if tmpEndPoint['space_expired'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_expired']
                    if tmpEndPoint['space_free'] is not None:
                        tmpSpaceSize += tmpEndPoint['space_free']
                    if tmpSpaceSize < diskThreshold:
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
            # check for preassigned
            if sitePreAssigned:
                if taskSpec.site not in scanSiteList and taskSpec.site not in self.get_unified_sites(scanSiteList):
                    tmpLog.info("preassigned site {0} did not pass all tests".format(taskSpec.site))
                    tmpLog.error('no candidates')
                    retVal = retFatal
                    continue
                else:
                    newScanSiteList = []
                    for tmpPseudoSiteName in scanSiteList:
                        tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
                        tmpSiteName = tmpSiteSpec.get_unified_name()
                        if tmpSiteName != taskSpec.site:
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
                for tmpMsg in msgListVP:
                    tmpLog.info(tmpMsg)
            tmpLog.info('{0} candidates passed overload check'.format(len(scanSiteList)))
            if not scanSiteList:
                tmpLog.error('no candidates')
                retVal = retTmpError
                continue
            # loop end
            if len(scanSiteList) > 0:
                retVal = None
                break
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
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(fileScanSiteList, self.siteMapper, 'user')
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
        if retVal is not None:
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            # send info to logger
            self.sendLogMessage(tmpLog)
            return retVal
        tmpLog.info('final {0} candidates'.format(len(scanSiteList)))
        weightMap = {}
        weightStr = {}
        candidateSpecList = []
        preSiteCandidateSpec = None
        problematicSites = set()
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
            # problematic sites
            if nFailed+nClosed > 2*nFinished:
                problematicSites.add(tmpSiteName)
            # calculate weight
            weight = float(nRunning + 1) / float(nActivated + nAssigned + nDefined + nStarting + 1)
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
            siteCandidateSpec = SiteCandidate(tmpPseudoSiteName)
            # preassigned
            if sitePreAssigned and tmpSiteName == taskSpec.site:
                preSiteCandidateSpec = siteCandidateSpec
            # override attributes
            siteCandidateSpec.override_attribute('maxwdir', newMaxwdir.get(tmpSiteName))
            # set weight
            siteCandidateSpec.weight = weight
            tmpStr  = 'weight={0} nRunning={1} nDefined={2} nActivated={3} nStarting={4} nAssigned={5} '.format(weight,
                                                                                                                nRunning,
                                                                                                                nDefined,
                                                                                                                nActivated,
                                                                                                                nStarting,
                                                                                                                nAssigned)
            tmpStr += 'nFailed={0} nClosed={1} nFinished={2} dataW={3} '.format(nFailed,
                                                                                nClosed,
                                                                                nFinished,
                                                                                tmpDataWeight)
            tmpStr += 'totalInGB={0} localInGB={1} nFiles={2}'.format(totalSize, localSize, totalNumFiles)
            weightStr[tmpPseudoSiteName] = tmpStr
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if weight not in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)
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
            # remove problematic sites
            candidateSpecList = AtlasBrokerUtils.skipProblematicSites(candidateSpecList,
                                                                      problematicSites,
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
            if sitePreAssigned and tmpSiteName != taskSpec.site:
                tmpLog.info('  skip site={0} non pre-assigned site criteria=-nonpreassigned'.format(tmpPseudoSiteName))
                try:
                    del weightStr[tmpPseudoSiteName]
                except Exception:
                    pass
                continue
            # set available files
            if inputChunk.getDatasets() == [] or not checkDataLocality:
                isAvailable = True
            else:
                isAvailable = False
            for tmpDatasetName,availableFiles in iteritems(availableFileMap):
                tmpDatasetSpec = inputChunk.getDatasetWithName(tmpDatasetName)
                # check remote files
                if tmpSiteName in remoteSourceList and tmpDatasetName in remoteSourceList[tmpSiteName]:
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
                            checkDataLocality is False:
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
            if tmpSiteName in weightStr:
                if tmpSiteName in problematicSites:
                    tmpMsg = '  skip site={0} due to too many failed or closed jobs for last 6h with {1} criteria=-failedclosed'.format(tmpSiteName, weightStr[tmpSiteName])
                else:
                    tmpMsg = '  skip site={0} due to low weight and not-used by old jobs with {1} criteria=-lowweight'.format(tmpSiteName, weightStr[tmpSiteName])
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
