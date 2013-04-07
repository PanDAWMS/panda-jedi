import sys

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from JobBrokerBase import JobBrokerBase
import AtlasBrokerUtils

# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for ATLAS production
class AtlasProdJobBroker (JobBrokerBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF,siteMapper):
        JobBrokerBase.__init__(self,ddmIF,taskBufferIF,siteMapper)
        self.hospitalQueueMap = AtlasBrokerUtils.getHospitalQueues(siteMapper)
        self.dataSiteMap = {}


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk):
        # make logger
        tmpLog = MsgWrapper(logger,'taskID=%s' % taskSpec.taskID)
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,{}
        retTmpError = self.SC_FAILED,{}
        # get sites in the cloud
        scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
        tmpLog.debug('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,taskSpec.prodSourceLabel)
        if not tmpSt:
            tmpLog.error('failed to get job statistics')
            return retTmpError
        ######################################
        # selection for status
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check site status
            skipFlag = False
            if tmpSiteSpec.status != 'online' and taskSpec.prodSourceLabel == 'managed':
                skipFlag = True
            elif tmpSiteSpec.status in ['offline','brokeroff'] and taskSpec.prodSourceLabel == 'test':
                skipFlag = True
            if not skipFlag:    
                newScanSiteList.append(tmpSiteName)
            else:
                tmpLog.debug('  skip %s due to status=%s' % (tmpSiteName,tmpSiteSpec.status))
        scanSiteList = newScanSiteList        
        tmpLog.debug('selected %s candidates due to site status' % len(scanSiteList))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            return retTmpError
        ######################################
        # selection for reprocessing
        if taskSpec.processingType == 'reprocessing':
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check schedconfig.validatedreleases
                if tmpSiteSpec.validatedreleases == 'True':
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.debug('  skip %s due to validatedreleases != True' % tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('selected %s candidates for reprocessing' % len(scanSiteList))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                return retTmpError
        ######################################
        # selection for high priorities
        if taskSpec.currentPriority >= 950:
            # T1 
            t1Sites = [self.siteMapper.getCloud(cloudName)['source']]
            # hospital sites
            if self.hospitalQueueMap.has_key(cloudName):
                t1Sites += self.hospitalQueueMap(cloudName)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:            
                if tmpSiteName in t1Sites:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.debug('  skip %s due to high prio which needs to run at T1' % tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.debug('selected %s candidates for high prio' % len(scanSiteList))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                return retTmpError
        ######################################
        # selection for data availability
        for datasetSpec in inputChunk.getDatasets():
            datasetName = datasetSpec.datasetName
            if not self.dataSiteMap.has_key(datasetName):
                # get the list of sites where data is available
                tmpSt,tmpRet = AtlasBrokerUtils.getSitesWithData(self.siteMapper,
                                                                 self.ddmIF,datasetName)
                if tmpSt == self.SC_FAILED:
                    tmpLog.error('failed to get the list of sites where data is available, since %s' % tmpRet)
                    return retTmpError
                if tmpSt == self.SC_FATAL:
                    tmpLog.error('fatal error when getting the list of sites where data is available, since %s' % tmpRet)
                    return retFatal
                # append
                self.dataSiteMap[datasetName] = tmpRet
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
            # data is missing at T1         
            if not t1hasData:
                # make subscription to T1
                # FIXME
                pass
                # use T2 until data is complete at T1
                newScanSiteList = []
                for tmpSiteName in scanSiteList:                    
                    if cloudHasData and tmpSiteName in self.dataSiteMap[datasetName][cloudName]['t2']:
                        newScanSiteList.append(tmpSiteName)
                    else:
                        tmpLog.debug('  skip %s due to T2 data' % tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.debug('selected %s candidates for %s which is available only at T2' % \
                             (len(scanSiteList),datasetName))
                if scanSiteList == []:
                    tmpLog.error('no candidates')
                    return retTmpError
        ######################################
        # selection for fairshare
        # FIXME
        pass
        ######################################
        # selection due to I/O intensive tasks
        # FIXME
        pass
        ######################################
        # selection for MP
        if taskSpec.coreCount != None and taskSpec.coreCount > 1:
            useMP = True
        else:
            useMP = False
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if (useMP and tmpSiteSpec.coreCount > 1) or \
               (not useMP and tmpSiteSpec.coreCount in [0,1,None]):
                    newScanSiteList.append(tmpSiteName)
            else:
                tmpLog.debug('  skip %s due to core mismatch site:%s != task:%s' % \
                             (tmpSiteName,tmpSiteSpec.coreCount,taskSpec.coreCount))
        scanSiteList = newScanSiteList        
        tmpLog.debug('selected %s candidates for useMP=%s' % (len(scanSiteList),useMP))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            return retTmpError
        ######################################
        # selection for release
        if taskSpec.transHome != None:
            if re.search('rel_\d+(\n|$)',taskSpec.transHome) == None:
                # only cache is checked for normal tasks
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                         caches=taskSpec.transHome,
                                                                         cmtConfig=taskSpec.architecture)
                
            else:
                # both release and cache are checked for nightlies
                # FIXME
                pass
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # release check is disabled or release is available
                if tmpSiteSpec.releases == ['ANY'] or \
                   tmpSiteSpec.cloud in ['ND'] or \
                   tmpSiteName in ['CERN-RELEASE']:
                    newScanSiteList.append(tmpSiteName)
                elif tmpSiteName in siteListWithSW:
                    newScanSiteList.append(tmpSiteName)
                else:
                    # release is unavailable
                    tmpLog.debug('  skip %s due to missing rel/cache %s:%s' % \
                                 (tmpSiteName,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = newScanSiteList        
            tmpLog.debug('selected %s candidates for ATLAS release %s:%s' % \
                         (len(scanSiteList),taskSpec.transHome,taskSpec.architecture))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                return retTmpError
        ######################################
        # selection for memory
        minRamCount  = taskSpec.ramCount
        if not minRamCount in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.memory != 0 and minRamCount > tmpSiteSpec.memory:
                    tmpLog.debug('  skip %s due to memory shortage=%s < %s' % \
                                 (tmpSiteName,tmpSiteSpec.memory,minRamCount))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('selected %s candidates for memory=%s%s' % \
                         (len(scanSiteList),minRamCount,taskSpec.ramCountUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                return retTmpError
        ######################################
        # selection for scratch disk
        #minDiskCount = taskSpec.outDiskCount + taskSpec.workDiskCount + self.getMaxFsize(inFilesMap)
        # FIXME
        pass
        ######################################
        # selection for available space in SE
        # FIXME
        pass
        ######################################
        # selection for walltime
        minWalltime = taskSpec.walltime
        if not minWalltime in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxtime != 0 and minWalltime > tmpSiteSpec.maxtime:
                    tmpLog.debug('  skip %s due to short walltime=%s < %s' % \
                                 (tmpSiteName,tmpSiteSpec.maxtime,minWalltime))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('selected %s candidates for walltime=%s%s' % \
                         (len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                return retTmpError
        ######################################
        # selection for transferring
        # FIXME : doesn't work on voatlas294
        """
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # limit
            def_maxTransferring = 2000 
            if tmpSiteSpec.transferringlimit == 0:
                # use default value
                maxTransferring   = def_maxTransferring
            else:
                maxTransferring = tmpSiteSpec.transferringlimit
            # check at the site
            nTraJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'transferring',cloud=cloudName)
            nRunJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'running',cloud=cloudName)
            if max(maxTransferring,2*nRunJobs) < nTraJobs and not tmpSiteSpec.cloud in ['ND']:
                tmpLog.debug('  skip %s due to too many transferring %s > max(%s,2x%s' % \
                             (tmpSiteName,nTraJobs,def_maxTransferring,nRunJobs))
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList        
        tmpLog.debug('selected %s candidates due to transferring' % len(scanSiteList))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            return retTmpError
        """    
        ######################################
        # selection for nPilot
        nWNmap = self.taskBufferIF.getCurrentSiteData()
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            # check at the site
            nPilot = 0
            if nWNmap.has_key(tmpSiteName):
                nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
            if nPilot == 0:
                tmpLog.debug('  skip %s due to no pilot' % tmpSiteName)
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList        
        tmpLog.debug('selected %s candidates due to pilot' % len(scanSiteList))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            return retTmpError
        ######################################
        # get available files
        totalSize = 0
        normalizeFactors = {}        
        availableFileMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteStorageEndpointMap(scanSiteList,self.siteMapper)
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            ngGroup=[1])
                if tmpAvFileMap == None:
                    raise Interaction.JEDITemporaryError,'ddmIF.getAvailableFiles failed'
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                return retTmpError
            # get total size
            totalSize += datasetSpec.getSize()
            # loop over all sites to get the size of available files
            for tmpSiteName in scanSiteList:
                if not normalizeFactors.has_key(tmpSiteName):
                    normalizeFactors[tmpSiteName] = 0
                # get the total size of available files
                if availableFileMap[datasetSpec.datasetName].has_key(tmpSiteName):
                    availableFiles = availableFileMap[datasetSpec.datasetName][tmpSiteName]
                    for tmpFileSpec in \
                            availableFiles['localdisk']+availableFiles['localtape']+availableFiles['cache']:
                        normalizeFactors[tmpSiteName] += tmpFileSpec.fsize
        ######################################
        # calculate weight
        tmpSt,jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                    taskSpec.prodSourceLabel,
                                                                                    taskSpec.currentPriority)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            return retTmpError
        weightMap = {}
        for tmpSiteName in scanSiteList:
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'running',cloudName,taskSpec.workQueue_ID)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'assigned',cloudName,taskSpec.workQueue_ID)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',cloudName,taskSpec.workQueue_ID)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + 1) / float(nAssigned + 1)
            # noramize weights by taking data availability into account
            if totalSize != 0:
                weight = weight * float(normalizeFactors[tmpSiteName]+totalSize) / float(totalSize)
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # set weight
            siteCandidateSpec.weight = weight
            # set available files
            for tmpDatasetName,availableFiles in availableFileMap.iteritems():
                if availableFiles.has_key(tmpSiteName):
                    siteCandidateSpec.localDiskFiles  += availableFiles[tmpSiteName]['localdisk']
                    siteCandidateSpec.localTapeFiles  += availableFiles[tmpSiteName]['localtape']
                    siteCandidateSpec.cacheFiles  += availableFiles[tmpSiteName]['cache']
                    siteCandidateSpec.remoteFiles += availableFiles[tmpSiteName]['remote']
            # append        
            inputChunk.addSiteCandidate(siteCandidateSpec)
        # return
        tmpLog.debug('done')        
        return True,inputChunk
    
