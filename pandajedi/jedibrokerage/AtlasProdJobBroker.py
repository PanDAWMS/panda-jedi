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


    # wrapper for return
    def sendLogMessage(self,tmpLog):
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
    def doBrokerage(self,taskSpec,cloudName,inputChunk,taskParamMap):
        # make logger
        tmpLog = MsgWrapper(logger,'<jediTaskID={0}>'.format(taskSpec.jediTaskID),
                            monToken='<jediTaskID={0} {1}>'.format(taskSpec.jediTaskID,
                                                                   datetime.datetime.utcnow().isoformat('/')))
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
        t1Sites = [self.siteMapper.getCloud(cloudName)['source']]
        # hospital sites
        if self.hospitalQueueMap.has_key(cloudName):
            t1Sites += self.hospitalQueueMap[cloudName]
        # sites sharing SE with T1
        sitesShareSeT1 = DataServiceUtils.getSitesShareDDM(self.siteMapper,self.siteMapper.getCloud(cloudName)['source'])
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
        if (taskSpec.currentPriority > 900 or inputChunk.useScout()) \
                and not sitePreAssigned and not siteListPreAssigned:
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
        minRamCount  = taskSpec.ramCount
        if not minRamCount in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check at the site
                if tmpSiteSpec.maxmemory != 0 and minRamCount != 0 and minRamCount > tmpSiteSpec.maxmemory:
                    tmpMsg = '  skip site={0} due to site RAM shortage {1}(site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                    tmpSiteSpec.maxmemory,
                                                                                                                    minRamCount)
                    tmpMsg += 'criteria=-lowmemory'
                    tmpLog.debug(tmpMsg)
                    continue
                if tmpSiteSpec.minmemory != 0 and minRamCount != 0 and minRamCount < tmpSiteSpec.minmemory:
                    tmpMsg = '  skip site={0} due to job RAM shortage {1}(site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                      tmpSiteSpec.minmemory,
                                                                                                                      minRamCount)
                    tmpMsg += 'criteria=-highmemory'
                    tmpLog.debug(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed memory check {1}({2})'.format(len(scanSiteList),
                                                                              minRamCount,taskSpec.ramUnit))
            if scanSiteList == []:
                tmpLog.error('no candidates')
                taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
                self.sendLogMessage(tmpLog)
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
            minWalltime = taskSpec.walltime * inputChunk.getMaxAtomSize(effectiveSize=True)
        else:
            minWalltime = taskSpec.cpuTime * inputChunk.getMaxAtomSize(getNumEvents=True)
        if not minWalltime in [0,None]:
            newScanSiteList = []
            for tmpSiteName in scanSiteList:
                tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
                # check max walltime at the site
                siteMaxTime = tmpSiteSpec.maxtime
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
                if siteMaxTime != 0 and minWalltime > siteMaxTime:
                    tmpMsg = '  skip site={0} due to short site walltime {1} (site upper limit) less than {2} '.format(tmpSiteName,
                                                                                                                       tmpSiteStr,
                                                                                                                       minWalltime)
                    tmpMsg += 'criteria=-shortwalltime'
                    tmpLog.debug(tmpMsg)
                    continue
                # check min walltime at the site
                siteMinTime = tmpSiteSpec.mintime
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
                if siteMinTime != 0 and minWalltime < siteMinTime:
                    tmpMsg = '  skip site {0} due to short job walltime {1} (site lower limit) greater than {2} '.format(tmpSiteName,
                                                                                                                         tmpSiteStr,
                                                                                                                         minWalltime)
                    tmpMsg += 'criteria=-longwalltime'
                    tmpLog.debug(tmpMsg)
                    continue
                newScanSiteList.append(tmpSiteName)
            scanSiteList = newScanSiteList        
            tmpLog.debug('{0} candidates passed walltime check {1}({2})'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
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
            jobseed = taskSpec.useEventService()
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
        weightMap = {}
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
            weightStr = 'nRun={0} nAct={1} nAss={2} nStart={3} nDef={4} tSize={5} manyAss={6} nPilot={7} '.format(nRunning,nActivated,nAssigned,
                                                                                                                  nStarting,nDefined,
                                                                                                                  totalSize,manyAssigned,
                                                                                                                  nPilot)
            # normalize weights by taking data availability into account
            if totalSize != 0:
                weight = weight * float(normalizeFactors[tmpSiteName]+totalSize) / float(totalSize)
                weightStr += 'norm={0} '.format(normalizeFactors[tmpSiteName])
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
            # check cap with nRunning
            cutOffValue = 20
            cutOffFactor = 2 
            nRunningCap = max(cutOffValue,cutOffFactor*nRunning)
            nRunningCap = max(nRunningCap,nPilot)
            okMsg = '  use site={0} with weight={1} {2} criteria=+use'.format(tmpSiteName,weight,weightStr)
            if (nDefined+nActivated+nAssigned+nStarting) > nRunningCap:
                ngMsg = '  skip site={0} due to nDefined+nActivated+nAssigned+nStarting={1} '.format(tmpSiteName,
                                                                                                     nDefined+nActivated+nAssigned+nStarting)
                ngMsg += 'greater than max({0},{1}*nRunning={1}*{2},nPilot={3}) '.format(cutOffValue,
                                                                                         cutOffFactor,                                  
                                                                                         nRunning,                                      
                                                                                         nPilot)
                ngMsg += 'criteria=-cap'
                # add weight
                if not weight in weightMap:
                    weightMap[weight] = []
                weightMap[weight].append((siteCandidateSpec,okMsg,ngMsg))
                # use hightest 3 weights
                weightRank = 3
                weightList = weightMap.keys()
                if weightList > weightRank:
                    weightList.sort()
                    weightList.reverse()
                    newWeightMap = {}
                    for tmpWeight in weightList[:weightRank]:
                        newWeightMap[tmpWeight] = weightMap[tmpWeight]
                    # dump NG message
                    for tmpWeight in weightList[weightRank:]:
                        for tmpSiteCandidateSpec,tmpOkMsg,tmpNgMsg in weightMap[weight]:
                            tmpLog.debug(tmpNgMsg)
                    weightMap = newWeightMap
            else:
                # append        
                newScanSiteList.append(tmpSiteName)
                inputChunk.addSiteCandidate(siteCandidateSpec)
                tmpLog.debug(okMsg)
        scanSiteList = newScanSiteList
        newScanSiteList = []
        if scanSiteList == []:
            tmpLog.debug('use second candidates since no sites pass cap check')
        for tmpWeight,weightList in weightMap.iteritems():
            for siteCandidateSpec,tmpOkMsg,tmpNgMsg in weightList:
                if scanSiteList == []:
                    tmpLog.debug(tmpOkMsg)
                    newScanSiteList.append(siteCandidateSpec.siteName)
                    inputChunk.addSiteCandidate(siteCandidateSpec)
                else:
                    tmpLog.debug(tmpNgMsg)
        # second candidates
        if scanSiteList == []:
            scanSiteList = newScanSiteList
        # final check
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            self.sendLogMessage(tmpLog)
            return retTmpError
        tmpLog.debug('final {0} candidates'.format(len(scanSiteList)))
        # return
        self.sendLogMessage(tmpLog)
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
    
