import os
import sys
import re
import socket
import traceback

from six import iteritems

from .WatchDogBase import WatchDogBase
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock
from pandajedi.jedicore import Interaction
from pandajei.jedibrokerage import AtlasBrokerUtils

# from pandaserver.dataservice.Activator import Activator

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# task withholder watchdog for ATLAS
class AtlasTaskWithholderWatchDog (WatchDogBase):

    # constructor
    def __init__(self, ddmIF, taskBufferIF):
        WatchDogBase.__init__(self, ddmIF, taskBufferIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        # self.cronActions = {'forPrestage': 'atlas_prs'}
        self.vo = 'atlas'
        self.prodSourceLabelList = []
        self.cloudList = []
        # call refresh
        self.refresh()

    # refresh information stored in the instance
    def refresh(self):
        # site mapper
        self.siteMapper = self.taskBufferIF.getSiteMapper()
        # all sites
        allSiteList = []
        for siteName, tmpSiteSpec in iteritems(self.siteMapper.siteSpecList):
            # if tmpSiteSpec.type == 'analysis' or tmpSiteSpec.is_grandly_unified():
            allSiteList.append(siteName)
        self.allSiteList = allSiteList

    # get list of tasks to check
    def get_tasks_list(self):
        # parameters
        nTasksToGetTasks = 1000
        nFilesToGetTasks = 200
        # get work queue mapper
        workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # initialize
        ret_list = []
        # loop over all sourceLabels
        for prodSourceLabel in self.prodSourceLabelList:
            # loop over all clouds
            # random.shuffle(self.cloudList)
            for cloudName in self.cloudList:
                # loop over all work queues
                workQueueList = workQueueMapper.getAlignedQueueList(vo, prodSourceLabel)
                resource_types = self.taskBufferIF.load_resource_types()
                # tmpLog.debug("{0} workqueues for vo:{1} label:{2}".format(len(workQueueList),vo,prodSourceLabel))
                for workQueue in workQueueList:
                    pass
                    tmp_list = self.taskBufferIF.getTasksToBeProcessed_JEDI(
                                                                self.pid,
                                                                self.vo,
                                                                workQueue,
                                                                prodSourceLabel,
                                                                cloudName,
                                                                nTasks=nTasksToGetTasks,
                                                                nFiles=nFilesToGetTasks,
                                                                minPriority=None,
                                                                maxNumJobs=None,
                                                                typicalNumFilesMap=None,
                                                                mergeUnThrottled=None,
                                                                numNewTaskWithJumbo=0,
                                                                resource_name=resource_type.resource_name)
                    if tmp_list is None:
                        # failed
                        tmpLog.error('failed to get the list of input chunks to generate jobs')
                    else:
                        tmpLog.debug('got {0} input tasks'.format(len(tmp_list)))
                        if len(tmp_list) != 0:
                            ret_list.extend(tmp_list)
                            # taskInputList = self.inputList.get(nInput)
                            # for tmpJediTaskID,__inputList in taskInputList:
        # return
        return ret_list


    # check data availability, return available sites
    def data_availability_check(self, taskSpec, inputChunk):
        tmpLog = MsgWrapper(logger, 'data_availability_check')
        # initialize
        dataSiteMap = {}
        ######################################
        # selection for data availability
        hasDDS = False
        dataWeight = {}
        ddsList = set()
        remoteSourceList = {}
        oldScanSiteList = copy.copy(self.allSiteList)
        oldScanUnifiedSiteList = self.get_unified_sites(oldScanSiteList)
        for datasetSpec in inputChunk.getDatasets():
            datasetName = datasetSpec.datasetName
            if datasetName not in dataSiteMap:
                # get the list of sites where data is available
                tmpLog.debug('getting the list of sites where {0} is available'.format(datasetName))
                tmpSt, tmpRet = AtlasBrokerUtils.getAnalSitesWithData(
                                                                    self.get_unified_sites(self.allSiteList),
                                                                    self.siteMapper, self.ddmIF,
                                                                    datasetName)
                if tmpSt in [Interaction.JEDITemporaryError, Interaction.JEDITimeoutError]:
                    tmpLog.error('temporary failed to get the list of sites where data is available, since %s' % tmpRet)
                    return
                if tmpSt == Interaction.JEDIFatalError:
                    tmpLog.error('fatal error when getting the list of sites where data is available, since %s' % tmpRet)
                    return
                # append
                dataSiteMap[datasetName] = tmpRet
                if datasetName.startswith('ddo'):
                    tmpLog.debug(' {0} sites'.format(len(tmpRet)))
                else:
                    tmpLog.debug(' {0} sites : {1}'.format(len(tmpRet), str(tmpRet)))
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
            if dataSiteMap[datasetName] == {}:
                tmpLog.warning('{0} is unavailable at any site'.format(datasetName))
                continue
        # get the list of sites where data is available
        scanSiteList = None
        scanSiteListOnDisk = None
        normFactor = 0
        for datasetName, tmpDataSite in iteritems(dataSiteMap):
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
                                                                       self.siteMapper, nSites=50,
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
        # for tmpSiteName in oldScanSiteList:
        #     if tmpSiteName not in scanSiteList:
        #         #tmpLog.info('  skip site={0} data is unavailable criteria=-input'.format(tmpSiteName))
        #         pass
        tmpLog.info('{0} candidates have input data'.format(len(scanSiteList)))
        # return
        if not scanSiteList:
            tmpLog.warning('no candidates')
            return
        return scanSiteList


    # handle waiting jobs
    def do_make_tasks_pending(self, taskspec_list):
        tmpLog = MsgWrapper(logger, 'do_make_tasks_pending')
        tmpLog.debug('start')
        # check every x min
        checkInterval = 20
        # make task pending
        for taskspec, pending_reason in taskspec_list:
            tmpLog = MsgWrapper(logger, '< #ATM #KV do_make_tasks_pending jediTaskID={0}>'.format(taskspec.jediTaskID))
            retVal = self.taskBufferIF.makeTaskPending_JEDI(taskspec.jediTaskID, reason=pending_reason)
            tmpLog.debug('done with {0}'.format(retVal))


    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # initialize
            tasks_to_make_pending = ListWithLock([])
            # tasks to check
            _tasks_list = self.get_tasks_list()
            tasks_list = ListWithLock(_tasks_list)
            # check all tasks
            for tmpJediTaskID, inputList in tasks_list:
                for tmpInputItem in inputList:
                    taskSpec, cloudName, inputChunk = tmpInputItem
                    # data availability check
                    if not self.data_availability_check(taskSpec, inputChunk):
                        pending_reason = 'AtlasTaskWithholder: data not available at any site'
                        tasks_to_make_pending.append((taskSpec, pending_reason))
            # make tasks pending under certain conditions
            self.do_make_tasks_pending(tasks_to_make_pending)
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            origTmpLog.error('failed with {0} {1}'.format(errtype, errvalue))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED
