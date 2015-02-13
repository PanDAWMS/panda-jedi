import re
import sys

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from pandajedi.jedicore import JediCoreUtils
from JobBrokerBase import JobBrokerBase
import AtlasBrokerUtils
from pandaserver.dataservice import DataServiceUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])


# brokerage for ATLAS production
class AtlasProdJobBroker (JobBrokerBase):

    # constructor
    def __init__(self,ddmIF,taskBufferIF):
        JobBrokerBase.__init__(self,ddmIF,taskBufferIF)
        self.hospitalQueueMap = AtlasBrokerUtils.getHospitalQueues(self.siteMapper)
        self.dataSiteMap = {}


    # main
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID))
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # get sites in the cloud
        sitePreAssigned = False
        siteListPreAssigned = False
        if not taskSpec.site in ['',None]:
            sitePreAssigned = True
            scanSiteList = [taskSpec.site]
            tmpLog.debug('site={0} is pre-assigned'.format(taskSpec.site))
        elif inputChunk.getPreassignedSite() != None:
            siteListPreAssigned = True
            scanSiteList = DataServiceUtils.getSitesShareDDM(self.siteMapper,inputChunk.getPreassignedSite())
            scanSiteList.append(inputChunk.getPreassignedSite())
            tmpLog.debug('use {0} since they share DDM endpoints with site={1} which is pre-assigned in masterDS'.format(str(scanSiteList),
                                                                                                                         inputChunk.getPreassignedSite()))
        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
            tmpLog.debug('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,taskSpec.prodSourceLabel)
        if not tmpSt:
            tmpLog.error('failed to get job statistics')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # T1 
        t1Sites = [self.siteMapper.getCloud(cloudName)['source']]
        # hospital sites
        if self.hospitalQueueMap.has_key(cloudName):
            t1Sites += self.hospitalQueueMap[cloudName]
        # sites sharing SE with T1
        sitesShareSeT1 = DataServiceUtils.getSitesShareDDM(self.siteMapper,self.siteMapper.getCloud(cloudName)['source'])
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
        ######################################
        # selection for status
        if not sitePreAssigned:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check site status
                skipFlag = False
                if tmpSiteSpec.status != 'online':
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
        # selection for reprocessing
        if taskSpec.processingType == 'reprocessing':
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check schedconfig.validatedreleases
                if tmpSiteSpec.validatedreleases == ['True']:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.debug('  skip %s due to validatedreleases != True' % tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for reprocessing'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for high priorities
        if (taskSpec.currentPriority >= 950 or inputChunk.useScout()) \
                and not sitePreAssigned and not siteListPreAssigned:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:            
                if tmpSiteName in t1Sites+sitesShareSeT1:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpLog.debug('  skip {0} due to highPrio/scouts which needs to run at sites associated with {1} T1 SE'.format(tmpSiteName,
                                                                                                                                  cloudName))
            scanSiteList = newScanSiteList
            tmpLog.debug('{0} candidates passed for highPrio/scouts'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for data availability
        if not sitePreAssigned and not siteListPreAssigned:
            for datasetSpec in inputChunk.getDatasets():
                datasetName = datasetSpec.datasetName
                # ignore DBR
                if DataServiceUtils.isDBR(datasetName):
                    continue
                if not self.dataSiteMap.has_key(datasetName):
                    # get the list of sites where data is available
                    tmpLog.debug('getting the list of sites where {0} is avalable'.format(datasetName))
                    tmpSt,tmpRet = AtlasBrokerUtils.getSitesWithData(self.siteMapper,
                                                                     self.ddmIF,datasetName,
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
                """
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
                    tmpLog.debug('{0} is unavailable at T1. scanning T2 sites in homeCloud={1}'.format(datasetName,cloudName))
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
                                tmpLog.debug('  skip %s due to foreign T2' % tmpSiteName)
                            else:
                                tmpLog.debug('  skip %s due to missing data at T2' % tmpSiteName)
                    scanSiteList = newScanSiteList
                    tmpLog.debug('{0} candidates passed T2 scan in the home cloud with input:{1}'.format(len(scanSiteList),datasetName))
                    if scanSiteList == []:
                        tmpLog.error('no candidates')
                        taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                        return retTmpError
                """        
        ######################################
        # selection for fairshare
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if AtlasBrokerUtils.hasZeroShare(tmpSiteSpec,taskSpec,tmpLog):
                tmpLog.debug('  skip {0} due to zero share'.format(tmpSiteName))
                continue
            newScanSiteList.append(tmpSiteName)                
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed zero share check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
                    tmpLog.debug('  skip %s due to core mismatch site:%s != task:%s' % \
                                 (tmpSiteName,tmpSiteSpec.coreCount,taskCoreCount))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for useMP={1}'.format(len(scanSiteList),useMP))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
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
                # nightlies
                siteListWithSW = self.taskBufferIF.checkSitesWithRelease(scanSiteList,
                                                                         releases='CVMFS')
                #                                                         releases='nightlies',
                #                                                         cmtConfig=taskSpec.architecture)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # release check is disabled or release is available
                if tmpSiteSpec.releases == ['ANY'] or \
                   tmpSiteName in ['CERN-RELEASE']:
                    newScanSiteList.append(tmpSiteName)
                elif tmpSiteName in siteListWithSW:
                    newScanSiteList.append(tmpSiteName)
                else:
                    # release is unavailable
                    tmpLog.debug('  skip %s due to missing rel/cache %s:%s' % \
                                 (tmpSiteName,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for ATLAS release {1}:{2}'.format(len(scanSiteList),
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
                                                                             minRamCount,taskSpec.ramUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for scratch disk
        minDiskCount = taskSpec.getOutDiskSize()*inputChunk.getMaxAtomSize(effectiveSize=True) \
            + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCount = minDiskCount / 1024 / 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir != 0 and minDiskCount > tmpSiteSpec.maxwdir:
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
            # don't check for T1
            if tmpSiteName in t1Sites:
                pass
            else:
                # check at the site
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # the number of jobs which will produce outputs
                nRemJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'assigned') + \
                           AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'activated') + \
                           AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'running')
                # the size of input files which will be copied to the site
                movingInputSize = self.taskBufferIF.getMovingInputSize_JEDI(tmpSiteName)
                if movingInputSize == None:
                    tmpLog.error('failed to get the size of input file moving to {0}'.format(tmpSiteName))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    return retTmpError
                # free space - inputs - outputs(250MB*nJobs) must be >= 200GB
                outSizePerJob = 0.250
                diskThreshold = 200
                tmpSpaceSize = tmpSiteSpec.space - movingInputSize - nRemJobs * outSizePerJob
                if tmpSiteSpec.space != 0 and tmpSpaceSize < diskThreshold:
                    tmpLog.debug('  skip {0} due to disk shortage in SE = {1}-{2}-{3}x{4} < {5}'.format(tmpSiteName,tmpSiteSpec.space,
                                                                                                        movingInputSize,outSizePerJob,
                                                                                                        nRemJobs,diskThreshold))
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
            return retTmpError
        ######################################
        # selection for walltime
        minWalltime = taskSpec.walltime * inputChunk.getMaxAtomSize(effectiveSize=True)
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
            tmpLog.debug('{0} candidates passed walltime check ={1}({2})'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # selection for transferring
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
                tmpLog.debug('  skip %s due to too many transferring %s > max(%s,2x%s)' % \
                             (tmpSiteName,nTraJobs,def_maxTransferring,nRunJobs))
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed transferring check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for T1 weight
        t1Weight = taskSpec.getT1Weight()
        if t1Weight == 0:
            # use T1 weight in cloudconfig
            t1Weight = self.siteMapper.getCloud(cloudName)['weight']
        tmpLog.debug('T1 weight {0}'.format(t1Weight))
        if t1Weight < 0:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if not tmpSiteName in t1Sites:
                    tmpLog.debug('  skip {0} due to negative T1 weight'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed T1 weight check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        ######################################
        # selection for nPilot
        if not sitePreAssigned:
            nWNmap = self.taskBufferIF.getCurrentSiteData()
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                # check at the site
                nPilot = 0
                if nWNmap.has_key(tmpSiteName):
                    nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
                if nPilot == 0 and not 'test' in taskSpec.prodSourceLabel:
                    tmpLog.debug('  skip %s due to no pilot' % tmpSiteName)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
        ######################################
        # get available files
        normalizeFactors = {}        
        availableFileMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteStorageEndpointMap(scanSiteList,self.siteMapper)
                # disable file lookup for merge jobs
                if inputChunk.isMerging or not datasetSpec.isMaster():
                    checkCompleteness = False
                else:
                    checkCompleteness = True
                # get available files per site/endpoint
                tmpAvFileMap = self.ddmIF.getAvailableFiles(datasetSpec,
                                                            siteStorageEP,
                                                            self.siteMapper,
                                                            ngGroup=[1],
                                                            checkCompleteness=checkCompleteness,
                                                            storageToken=datasetSpec.storageToken)
                if tmpAvFileMap == None:
                    raise Interaction.JEDITemporaryError,'ddmIF.getAvailableFiles failed'
                availableFileMap[datasetSpec.datasetName] = tmpAvFileMap
            except:
                errtype,errvalue = sys.exc_info()[:2]
                tmpLog.error('failed to get available files with %s %s' % (errtype.__name__,errvalue))
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                return retTmpError
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
        # get max total size
        tmpTotalSizes = normalizeFactors.values()
        tmpTotalSizes.sort()
        if tmpTotalSizes != []:
            totalSize = tmpTotalSizes.pop()
        else:
            totalSize = 0
        ######################################
        # calculate weight
        tmpSt,jobStatPrioMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,
                                                                                    taskSpec.prodSourceLabel)
        if not tmpSt:
            tmpLog.error('failed to get job statistics with priority')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        tmpLog.debug('final {0} candidates'.format(len(scanSiteList)))
        weightMap = {}
        for tmpSiteName in scanSiteList:
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'running',None,taskSpec.workQueue_ID)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'assigned',None,taskSpec.workQueue_ID)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',None,taskSpec.workQueue_ID)
            nStarting  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'starting',None,taskSpec.workQueue_ID)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + 1) / float(nAssigned + 1)
            weightStr = 'nRun={0} nAct={1} nAss={2} nStart={3} tSize={4} '.format(nRunning,nActivated,nAssigned,
                                                                                  nStarting,totalSize)
            # normalize weights by taking data availability into account
            if totalSize != 0:
                weight = weight * float(normalizeFactors[tmpSiteName]+totalSize) / float(totalSize)
                weightStr += 'norm={0} '.format(normalizeFactors[tmpSiteName])
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
            # T1 weight
            if tmpSiteName in t1Sites:
                weight *= t1Weight
                weightStr += 't1W={0} '.format(t1Weight)
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
            # append        
            inputChunk.addSiteCandidate(siteCandidateSpec)
            tmpLog.debug('  use {0} with weight={1} {2}'.format(tmpSiteName,weight,weightStr))
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
    
