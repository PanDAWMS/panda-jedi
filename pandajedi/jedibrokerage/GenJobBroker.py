import re
import six
import random

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from .JobBrokerBase import JobBrokerBase
from . import AtlasBrokerUtils
from pandajedi.jedirefine import RefinerUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for general purpose
class GenJobBroker (JobBrokerBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        JobBrokerBase.__init__(self,ddmIF,taskBufferIF)


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID))
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # set cloud
        try:
            if not taskParamMap:
                taskParam = self.taskBufferIF.getTaskParamsWithID_JEDI(taskSpec.jediTaskID)
                taskParamMap = RefinerUtils.decodeJSON(taskParam)
            if not taskSpec.cloud and 'cloud' in taskParamMap:
                taskSpec.cloud = taskParamMap['cloud']
        except Exception:
            pass
        # get sites in the cloud
        site_preassigned = True
        if taskSpec.site not in ['', None]:
            tmpLog.debug('site={0} is pre-assigned'.format(taskSpec.site))
            if self.siteMapper.checkSite(taskSpec.site):
                scanSiteList = [taskSpec.site]
            else:
                scanSiteList = []
                for tmpSite in self.siteMapper.getCloud(taskSpec.cloud)['sites']:
                    if re.search(taskSpec.site, tmpSite):
                        scanSiteList.append(tmpSite)
                if not scanSiteList:
                    tmpLog.error('unknown site={}'.format(taskSpec.site))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
        elif inputChunk.getPreassignedSite() is not None:
            scanSiteList = [inputChunk.getPreassignedSite()]
            tmpLog.debug('site={0} is pre-assigned in masterDS'.format(inputChunk.getPreassignedSite()))
        else:
            site_preassigned = False
            scanSiteList = self.siteMapper.getCloud(taskSpec.cloud)['sites']
            # remove NA
            if 'NA' in scanSiteList:
                scanSiteList.remove('NA')
            tmpLog.debug('cloud=%s has %s candidates' % (taskSpec.cloud, len(scanSiteList)))
        tmpLog.debug('initial {0} candidates'.format(len(scanSiteList)))
        ######################################
        # selection for status and PandaSite
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check site status
            if tmpSiteSpec.status != 'online' and not site_preassigned:
                tmpLog.debug('  skip %s due to status=%s' % (tmpSiteName, tmpSiteSpec.status))
                continue
            # check PandaSite
            if 'PandaSite' in taskParamMap and taskParamMap['PandaSite']:
                if tmpSiteSpec.pandasite != taskParamMap['PandaSite']:
                    tmpLog.debug('  skip %s due to wrong PandaSite=%s <> %s' % (tmpSiteName,
                                                                                tmpSiteSpec.pandasite,
                                                                                taskParamMap['PandaSite']))
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed site status check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for scratch disk
        minDiskCountS = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCountS = minDiskCountS // 1024 // 1024
        # size for direct IO sites
        if taskSpec.useLocalIO():
            minDiskCountR = minDiskCountS
        else:
            minDiskCountR = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()
            minDiskCountR = minDiskCountR // 1024 // 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir:
                if JediCoreUtils.use_direct_io_for_job(taskSpec, tmpSiteSpec, inputChunk):
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
            if tmpSiteSpec.space and tmpSpaceSize < diskThreshold:
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
        if minWalltime not in [0,None]:
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
        # selection for memory
        origMinRamCount = inputChunk.getMaxRamCount()
        if not site_preassigned and origMinRamCount:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement
                if taskSpec.ramPerCore():
                    minRamCount = origMinRamCount * (tmpSiteSpec.coreCount if tmpSiteSpec.coreCount else 1)
                    minRamCount += (taskSpec.baseRamCount if taskSpec.baseRamCount else 0)
                else:
                    minRamCount = origMinRamCount
                # site max memory requirement
                site_maxmemory = tmpSiteSpec.maxrss if tmpSiteSpec.maxrss else 0
                # check at the site
                if site_maxmemory and minRamCount and minRamCount > site_maxmemory:
                    tmpMsg = '  skip site={0} due to site RAM shortage {1}(site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                    site_maxmemory,
                                                                                                                    minRamCount)
                    tmpLog.debug(tmpMsg)
                    continue
                # site min memory requirement
                site_minmemory = tmpSiteSpec.minrss if tmpSiteSpec.minrss else 0
                if site_minmemory and minRamCount and minRamCount < site_minmemory:
                    tmpMsg = '  skip site={0} due to job RAM shortage {1}(site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                      site_minmemory,
                                                                                                                      minRamCount)
                    tmpLog.info(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.debug('{0} candidates passed memory check'.format(len(scanSiteList)))
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
            if tmpSiteName in nWNmap:
                nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
            if nPilot == 0 and taskSpec.prodSourceLabel not in ['test']:
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
                # mapping between sites and input storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteInputStorageEndpointMap(fileScanSiteList, self.siteMapper,
                                                                                taskSpec.prodSourceLabel, None)
                # disable file lookup for merge jobs
                if inputChunk.isMerging:
                    checkCompleteness = False
                else:
                    checkCompleteness = True
                if not datasetSpec.isMaster():
                    useCompleteOnly = True
                else:
                    useCompleteOnly = False
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            check_completeness=checkCompleteness,
                                                            file_scan_in_container=False,
                                                            complete_only=useCompleteOnly,
                                                            use_deep=True)
                if tmpAvFileMap is None:
                    raise Interaction.JEDITemporaryError('ddmIF.getAvailableFiles failed')
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except Exception as e:
                tmpLog.error('failed to get available files with {}'.format(e))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # calculate weight
        tmpSt,jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(taskSpec.vo)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
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
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',None,None)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + 1) / float(nAssigned + 1)
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # set weight
            siteCandidateSpec.weight = weight
            # files
            for tmpDatasetName, availableFiles in six.iteritems(availableFileMap):
                if tmpSiteName in availableFiles:
                    siteCandidateSpec.add_local_disk_files(availableFiles[tmpSiteName]['localdisk'])
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if weight not in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)
        # limit the number of sites
        maxNumSites = 5
        weightList = list(weightMap.keys())
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
        # append candidates
        newScanSiteList = []
        for siteCandidateSpec in candidateSpecList:
            # append
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpLog.debug('  use {} with weight={} nFiles={}'.format(siteCandidateSpec.siteName,
                                                                    siteCandidateSpec.weight,
                                                                    len(siteCandidateSpec.localDiskFiles)))
        scanSiteList = newScanSiteList
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # return
        tmpLog.debug('done')
        return self.SC_SUCCEEDED,inputChunk
