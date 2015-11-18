import re
import sys
import datetime

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
        self.suppressLogSending = False


    # wrapper for return
    def sendLogMessage(self,tmpLog):
        # log suppression
        if self.suppressLogSending:
            return
        # send info to logger
        tmpLog.bulkSendMsg('prod_brokerage')
        tmpLog.debug('sent')


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
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap,hintForTB=False,glLog=None):
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
        tmpLog.debug('start')
        # return for failure
        retFatal    = self.SC_FATAL,inputChunk
        retTmpError = self.SC_FAILED,inputChunk
        # get sites in the cloud
        sitePreAssigned = False
        siteListPreAssigned = False
        if not taskSpec.site in ['',None]:
            if ',' in taskSpec.site:
                # site list
                siteListPreAssigned = True
                scanSiteList = taskSpec.site.split(',')
            else:
                # site
                sitePreAssigned = True
                scanSiteList = [taskSpec.site]
            tmpLog.debug('site={0} is pre-assigned criteria=+preassign'.format(taskSpec.site))
        elif inputChunk.getPreassignedSite() != None:
            siteListPreAssigned = True
            scanSiteList = DataServiceUtils.getSitesShareDDM(self.siteMapper,inputChunk.getPreassignedSite())
            scanSiteList.append(inputChunk.getPreassignedSite())
            tmpMsg = 'use site={0} since they share DDM endpoints with orinal_site={1} which is pre-assigned in masterDS '.format(str(scanSiteList),
                                                                                                                                  inputChunk.getPreassignedSite())
            tmpMsg += 'criteria=+premerge'
            tmpLog.debug(tmpMsg)
        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
            tmpLog.debug('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
        # get job statistics
        tmpSt,jobStatMap = self.taskBufferIF.getJobStatisticsWithWorkQueue_JEDI(taskSpec.vo,taskSpec.prodSourceLabel)
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
                t1Sites = []
                tmpStat,datasetSpecList = self.taskBufferIF.getDatasetsWithJediTaskID_JEDI(taskSpec.jediTaskID,datasetTypes=['log','output'])
                for datasetSpec in datasetSpecList:
                    if self.siteMapper.checkSite(datasetSpec.destination) and \
                            not datasetSpec.destination in t1Sites:
                        t1Sites.append(datasetSpec.destination)
                        tmpMap = AtlasBrokerUtils.getHospitalQueues(self.siteMapper,datasetSpec.destination,cloudName)
                        for tmpList in tmpMap.values():
                            for tmpHQ in tmpList:
                                if not tmpHQ in t1Sites:
                                    t1Sites.append(tmpHQ)
            else:
                # user all sites in nuclei for WORLD task brokerage
                t1Sites = []
                for tmpNucleus in self.siteMapper.nuclei.values():
                    t1Sites += tmpNucleus.allPandaSites
        # sites sharing SE with T1
        sitesShareSeT1 = DataServiceUtils.getSitesShareDDM(self.siteMapper,t1Sites[0])
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
        workQueue = self.taskBufferIF.getWorkQueueMap().getQueueWithID(taskSpec.workQueue_ID)
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
                    tmpLog.debug('  skip site=%s due to status=%s criteria=-status' % (tmpSiteName,tmpSiteSpec.status))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed site status check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
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
                    tmpLog.debug('  skip site=%s due to validatedreleases <> True criteria=-validated' % tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for reprocessing'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for high priorities
        t1WeightForHighPrio = 1
        if (taskSpec.currentPriority >= 900 or inputChunk.useScout()) \
                and not sitePreAssigned and not siteListPreAssigned \
                and not taskSpec.useEventService():
            t1WeightForHighPrio = 100
            newScanSiteList = []
            for tmpSiteName in scanSiteList:            
                if tmpSiteName in t1Sites+sitesShareSeT1+allT1Sites:
                    newScanSiteList.append(tmpSiteName)
                else:
                    tmpMsg = '  skip site={0} due to highPrio/scouts which needs to run at T1 or sites associated with {1} T1 SE '.format(tmpSiteName,
                                                                                                                                          cloudName)
                    tmpMsg += 'criteria=-scoutprio'
                    tmpLog.debug(tmpMsg)
            scanSiteList = newScanSiteList
            tmpLog.debug('{0} candidates passed for highPrio/scouts'.format(len(scanSiteList)))
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
            for tmpSiteName in scanSiteList:
                nToGetAll = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'activated') + \
                    AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'starting')
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
                scanSiteList = newScanSiteList
                for tmpMsg in tmpMsgList:
                    tmpLog.debug(tmpMsg)
            tmpLog.debug('{0} candidates passed for slowness/inactive check'.format(len(scanSiteList)))
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
                    tmpLog.debug('getting the list of sites where {0} is avalable'.format(datasetName))
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
        if taskSpec.prodSourceLabel in ['managed'] or not workQueue.queue_name in ['test','validation']:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if AtlasBrokerUtils.hasZeroShare(tmpSiteSpec,taskSpec,inputChunk.isMerging,tmpLog):
                    tmpLog.debug('  skip site={0} due to zero share criteria=-zeroshare'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)                
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed zero share check'.format(len(scanSiteList)))
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
                    tmpLog.debug('  skip site=%s due to core mismatch site:%s <> task:%s criteria=-cpucore' % \
                                 (tmpSiteName,tmpSiteSpec.coreCount,taskCoreCount))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for useMP={1}'.format(len(scanSiteList),useMP))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
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
                    tmpLog.debug('  skip site=%s due to missing cache=%s:%s criteria=-cache' % \
                                 (tmpSiteName,taskSpec.transHome,taskSpec.architecture))
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed for ATLAS release {1}:{2}'.format(len(scanSiteList),
                                                                                  taskSpec.transHome,
                                                                                  taskSpec.architecture))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        ######################################
        # selection for memory
        origMinRamCount  = max(taskSpec.ramCount, inputChunk.ramCount)
        if not origMinRamCount in [0,None]:
            strMinRamCount = '{0}({1})'.format(origMinRamCount,taskSpec.ramUnit)
            if not taskSpec.baseRamCount in [0,None]:
                strMinRamCount += '+{0}'.format(taskSpec.baseRamCount)
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # job memory requirement 
                minRamCount = origMinRamCount
                if taskSpec.ramUnit == 'MBPerCore':
                    if not tmpSiteSpec.coreCount in [None,0]:
                        minRamCount = origMinRamCount * tmpSiteSpec.coreCount
                    minRamCount += taskSpec.baseRamCount
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
                    tmpLog.debug(tmpMsg)
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
                    tmpLog.debug(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed memory check {1}'.format(len(scanSiteList),strMinRamCount))
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
        minDiskCount = minDiskCount + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCount = minDiskCount / 1024 / 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir != 0 and minDiskCount > tmpSiteSpec.maxwdir:
                tmpMsg = '  skip site={0} due to small scratch disk {1} less than {2} '.format(tmpSiteName,
                                                                                               tmpSiteSpec.maxwdir,
                                                                                               minDiskCount)
                tmpMsg += 'criteria=-disk'
                tmpLog.debug(tmpMsg)
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed scratch disk check minDiskCount>{1}MB'.format(len(scanSiteList),
                                                                                          minDiskCount))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
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
                """
                # the number of jobs which will produce outputs
                nRemJobs = AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'assigned') + \
                           AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'activated') + \
                           AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'throttled') + \
                           AtlasBrokerUtils.getNumJobs(jobStatMap,tmpSiteName,'running')
                # the size of input files which will be copied to the site
                movingInputSize = self.taskBufferIF.getMovingInputSize_JEDI(tmpSiteName)
                if movingInputSize == None:
                    tmpLog.error('failed to get the size of input file moving to {0}'.format(tmpSiteName))
                    taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                    self.sendLogMessage(tmpLog)
                    return retTmpError
                # free space - inputs - outputs(250MB*nJobs) must be >= 200GB
                outSizePerJob = 0.250
                diskThreshold = 200
                tmpSiteSpaceMap = self.ddmIF.getRseUsage(tmpSiteSpec.ddm)
                if tmpSiteSpaceMap != {}:
                    tmpSiteFreeSpace = tmpSiteSpaceMap['free']
                    tmpSpaceSize = tmpSiteFreeSpace - movingInputSize - nRemJobs * outSizePerJob
                    if tmpSiteSpec.space != 0 and tmpSpaceSize < diskThreshold:
                        tmpLog.debug('  skip {0} due to disk shortage in SE = {1}-{2}-{3}x{4} < {5}'.format(tmpSiteName,tmpSiteFreeSpace,
                                                                                                            movingInputSize,outSizePerJob,
                                                                                                            nRemJobs,diskThreshold))
                        continue
                """        
                # check if blacklisted
                if self.ddmIF.isBlackListedEP(tmpSiteSpec.ddm):
                    tmpLog.debug('  skip site={0} since endpoint={1} is blacklisted in DDM criteria=-blacklist'.format(tmpSiteName,tmpSiteSpec.ddm))
                    continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList
        tmpLog.debug('{0} candidates passed SE space check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for walltime
        if not taskSpec.useHS06():
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(effectiveSize=True)
            if taskSpec.walltime != None:
                minWalltime = taskSpec.walltime * tmpMaxAtomSize
            else:
                minWalltime = None
            strMinWalltime = 'walltime*inputSize={0}*{1}'.format(taskSpec.walltime,tmpMaxAtomSize)
        else:
            tmpMaxAtomSize = inputChunk.getMaxAtomSize(getNumEvents=True)
            if taskSpec.cpuTime != None:
                minWalltime = taskSpec.cpuTime * tmpMaxAtomSize
            else:
                minWalltime = None
            strMinWalltime = 'cpuTime*nEventsPerJob={0}*{1}'.format(taskSpec.cpuTime,tmpMaxAtomSize)
        if minWalltime != None or inputChunk.useScout():
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                siteMaxTime = tmpSiteSpec.maxtime
                origSiteMaxTime = siteMaxTime
                # sending scouts merge or wallime-undefined jobs to only sites where walltime is more than 1 day
                if inputChunk.useScout() or inputChunk.isMerging or \
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
                        tmpLog.debug(tmpMsg)
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
                    tmpLog.debug(tmpMsg)
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
                    tmpLog.debug(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            if not taskSpec.useHS06():
                tmpLog.debug('{0} candidates passed walltime check {1}({2})'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
            else:
                tmpLog.debug('{0} candidates passed walltime check {1}({2}*nEventsPerJob)'.format(len(scanSiteList),strMinWalltime,taskSpec.cpuTimeUnit))
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
                        tmpLog.debug(tmpMsg)
                        continue
                    newScanSiteList.append(tmpSiteName)
                scanSiteList = newScanSiteList
                tmpLog.debug('{0} candidates passed network check ({1})'.format(len(scanSiteList),
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
                if taskSpec.useEventService():
                    if tmpSiteSpec.getJobSeed() == 'std':
                        tmpMsg = '  skip site={0} since EventService is not allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-es'
                        tmpLog.debug(tmpMsg)
                        continue
                else:
                    if tmpSiteSpec.getJobSeed() == 'es':
                        tmpMsg = '  skip site={0} since only EventService is allowed '.format(tmpSiteName)
                        tmpMsg += 'criteria=-nones'
                        tmpLog.debug(tmpMsg)
                        continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            tmpLog.debug('{0} candidates passed EventService check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
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
                tmpLog.debug('  skip site=%s due to too many transferring=%s greater than max(%s,2x%s) criteria=-transferring' % \
                                 (tmpSiteName,nTraJobs,def_maxTransferring,nRunJobs))
                continue
            newScanSiteList.append(tmpSiteName)
        scanSiteList = newScanSiteList        
        tmpLog.debug('{0} candidates passed transferring check'.format(len(scanSiteList)))
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        ######################################
        # selection for T1 weight
        t1Weight = taskSpec.getT1Weight()
        if t1Weight == 0:
            # use T1 weight in cloudconfig
            t1Weight = self.siteMapper.getCloud(cloudName)['weight']
        if t1Weight < 0:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                if not tmpSiteName in t1Sites:
                    tmpLog.debug('  skip site={0} due to negative T1 weight criteria=-t1weight'.format(tmpSiteName))
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList
            t1Weight = 1
        t1Weight = max(t1Weight,t1WeightForHighPrio)
        tmpLog.debug('T1 weight {0}'.format(t1Weight))
        tmpLog.debug('{0} candidates passed T1 weight check'.format(len(scanSiteList)))
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
            for tmpSiteName in scanSiteList:
                # check at the site
                nPilot = 0
                if nWNmap.has_key(tmpSiteName):
                    nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
                if nPilot == 0 and not 'test' in taskSpec.prodSourceLabel:
                    tmpLog.debug('  skip site=%s due to no pilot criteria=-nopilot' % tmpSiteName)
                    continue
                newScanSiteList.append(tmpSiteName)
                nPilotMap[tmpSiteName] = nPilot
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed pilot activity check'.format(len(scanSiteList)))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
                return retTmpError
        # return if to give a hint for task brokerage
        if hintForTB:
            tmpLog.debug('done')
            return self.SC_SUCCEEDED,scanSiteList
        ######################################
        # get available files
        normalizeFactors = {}        
        availableFileMap = {}
        for datasetSpec in inputChunk.getDatasets():
            try:
                # mapping between sites and storage endpoints
                siteStorageEP = AtlasBrokerUtils.getSiteStorageEndpointMap(scanSiteList,self.siteMapper,
                                                                           ignoreCC=True)
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
                                                            ngGroup=[1],
                                                            checkCompleteness=checkCompleteness,
                                                            storageToken=datasetSpec.storageToken,
                                                            useCompleteOnly=useCompleteOnly)
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
            self.sendLogMessage(tmpLog)
            return retTmpError
        tmpLog.debug('calculate weight and check cap for {0} candidates'.format(len(scanSiteList)))
        weightMapPrimary = {}
        weightMapSecondary = {}
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            nRunning   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'running',None,taskSpec.workQueue_ID)
            nDefined   = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'definied',None,taskSpec.workQueue_ID) + self.getLiveCount(tmpSiteName)
            nAssigned  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'assigned',None,taskSpec.workQueue_ID)
            nActivated = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'activated',None,taskSpec.workQueue_ID) + \
                         AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'throttled',None,taskSpec.workQueue_ID)
            nStarting  = AtlasBrokerUtils.getNumJobs(jobStatPrioMap,tmpSiteName,'starting',None,taskSpec.workQueue_ID)
            if tmpSiteName in nPilotMap:
                nPilot = nPilotMap[tmpSiteName]
            else:
                nPilot = 0
            manyAssigned = float(nAssigned + 1) / float(nActivated + 1)
            manyAssigned = min(2.0,manyAssigned)
            manyAssigned = max(1.0,manyAssigned)
            weight = float(nRunning + 1) / float(nActivated + nAssigned + nStarting + nDefined + 1) / manyAssigned
            weightStr = 'nRun={0} nAct={1} nAss={2} nStart={3} nDef={4} totalSize={5} manyAss={6} nPilot={7} '.format(nRunning,nActivated,nAssigned,
                                                                                                                      nStarting,nDefined,
                                                                                                                      totalSize,manyAssigned,
                                                                                                                      nPilot)
            # normalize weights by taking data availability into account
            if totalSize != 0:
                weight = weight * float(normalizeFactors[tmpSiteName]+totalSize) / float(totalSize)
                weightStr += 'availableSize={0} '.format(normalizeFactors[tmpSiteName])
            # T1 weight
            if tmpSiteName in t1Sites+sitesShareSeT1:
                weight *= t1Weight
                weightStr += 't1W={0} '.format(t1Weight)
            # make candidate
            siteCandidateSpec = SiteCandidate(tmpSiteName)
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
            # check if site is locked for WORLD
            lockedByBrokerage = False
            if taskSpec.useWorldCloud():
                lockedByBrokerage = self.checkSiteLock(taskSpec.vo,taskSpec.prodSourceLabel,
                                                       tmpSiteName,taskSpec.workQueue_ID)
            # check cap with nRunning
            cutOffValue = 20
            cutOffFactor = 2 
            nRunningCap = max(cutOffValue,cutOffFactor*nRunning)
            nRunningCap = max(nRunningCap,nPilot)
            okMsg = '  use site={0} with weight={1} {2} criteria=+use'.format(tmpSiteName,weight,weightStr)
            okAsPrimay = False
            if lockedByBrokerage:
                ngMsg = '  skip site={0} due to locked by another brokerage '.format(tmpSiteName)
                ngMsg += 'criteria=-lock'
            elif (not tmpSiteName in normalizeFactors or normalizeFactors[tmpSiteName] >= totalSize) and \
                    (nActivated+nStarting) > nRunningCap:
                ngMsg = '  skip site={0} due to nActivated+nStarting={1} '.format(tmpSiteName,
                                                                                  nActivated+nStarting)
                ngMsg += 'greater than max({0},{1}*nRunning={1}*{2},nPilot={3}) '.format(cutOffValue,
                                                                                         cutOffFactor,                                  
                                                                                         nRunning,                                      
                                                                                         nPilot)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-cap'
            elif tmpSiteName in normalizeFactors and normalizeFactors[tmpSiteName] < totalSize and \
                    (nDefined+nActivated+nAssigned+nStarting) > nRunningCap:
                ngMsg = '  skip site={0} due to nDefined+nActivated+nAssigned+nStarting={1} '.format(tmpSiteName,
                                                                                                     nDefined+nActivated+nAssigned+nStarting)
                ngMsg += 'greater than max({0},{1}*nRunning={1}*{2},nPilot={3}) '.format(cutOffValue,
                                                                                         cutOffFactor,                                  
                                                                                         nRunning,                                      
                                                                                         nPilot)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-cap'
            else:
                ngMsg = '  skip site={0} due to low weight '.format(tmpSiteName)
                ngMsg += '{0} '.format(weightStr)
                ngMsg += 'criteria=-loweigh'
                okAsPrimay = True
            # use primay if cap/lock check is passed
            if okAsPrimay:
                weightMap = weightMapPrimary
            else:
                weightMap = weightMapSecondary
            # add weight
            if not weight in weightMap:
                weightMap[weight] = []
            weightMap[weight].append((siteCandidateSpec,okMsg,ngMsg))
        # use second candidates if no primary candidates passed cap/lock check
        if weightMapPrimary == {}:
            tmpLog.debug('use second candidates since no sites pass cap/lock check')
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
                    tmpLog.debug(tmpNgMsg)
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
                if (weightRank == None or weightIdx < weightRank) and \
                        (maxSiteCandidates == None or len(newScanSiteList) < maxSiteCandidates):
                    # use site
                    tmpLog.debug(tmpOkMsg)
                    newScanSiteList.append(siteCandidateSpec.siteName)
                    inputChunk.addSiteCandidate(siteCandidateSpec)
                else:
                    # dump NG message
                    tmpLog.debug(tmpNgMsg)
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
        tmpLog.debug('final {0} candidates'.format(len(scanSiteList)))
        # return
        self.sendLogMessage(tmpLog)
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
    
