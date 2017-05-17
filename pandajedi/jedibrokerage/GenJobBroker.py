import re
import sys
import random

from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.SiteCandidate import SiteCandidate
from pandajedi.jedicore import Interaction
from JobBrokerBase import JobBrokerBase
from pandaserver.taskbuffer import PrioUtil
import AtlasBrokerUtils

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
        # get sites in the cloud
        if not taskSpec.site in ['',None]:
            scanSiteList = [taskSpec.site]
            tmpLog.debug('site={0} is pre-assigned'.format(taskSpec.site))
        elif inputChunk.getPreassignedSite() != None:
            scanSiteList = [inputChunk.getPreassignedSite()]
            tmpLog.debug('site={0} is pre-assigned in masterDS'.format(inputChunk.getPreassignedSite()))
        else:
            scanSiteList = self.siteMapper.getCloud(cloudName)['sites']
            tmpLog.debug('cloud=%s has %s candidates' % (cloudName,len(scanSiteList)))
        tmpLog.debug('initial {0} candidates'.format(len(scanSiteList)))
        ######################################
        # selection for status
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
        # selection for memory
        minRamCount  = max(taskSpec.ramCount, inputChunk.ramCount)
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
        minDiskCountS = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize() + inputChunk.getMaxAtomSize()
        minDiskCountS = minDiskCountS / 1024 / 1024
        # size for direct IO sites
        if taskSpec.useLocalIO():
            minDiskCountR = minDiskCountS
        else:
            minDiskCountR = taskSpec.getOutDiskSize() + taskSpec.getWorkDiskSize()
            minDiskCountR = minDiskCountR / 1024 / 1024
        newScanSiteList = []
        for tmpSiteName in scanSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            # check at the site
            if tmpSiteSpec.maxwdir != 0:
                if tmpSiteSpec.isDirectIO():
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
            if tmpSiteSpec.space != 0 and tmpSpaceSize < diskThreshold:
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
            tmpLog.debug('{0} candidates passed walltime check ={1}{2}'.format(len(scanSiteList),minWalltime,taskSpec.walltimeUnit))
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
            if nWNmap.has_key(tmpSiteName):
                nPilot = nWNmap[tmpSiteName]['getJob'] + nWNmap[tmpSiteName]['updateJob']
            if nPilot == 0 and not taskSpec.prodSourceLabel in ['test']:
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
            # append
            if tmpSiteName in sitesUsedByTask:
                candidateSpecList.append(siteCandidateSpec)
            else:
                if not weightMap.has_key(weight):
                    weightMap[weight] = []
                weightMap[weight].append(siteCandidateSpec)    
        # limit the number of sites
        maxNumSites = 5
        weightList = weightMap.keys()
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
            tmpSiteName = siteCandidateSpec.siteName
            # append
            inputChunk.addSiteCandidate(siteCandidateSpec)
            newScanSiteList.append(siteCandidateSpec.siteName)
            tmpLog.debug('  use {0} with weight={1}'.format(siteCandidateSpec.siteName,
                                                            siteCandidateSpec.weight))
        scanSiteList = newScanSiteList
        if scanSiteList == []:
            tmpLog.error('no candidates')
            taskSpec.setErrDiag(tmpLog.uploadLog(taskSpec.jediTaskID))
            return retTmpError
        # return
        tmpLog.debug('done')        
        return self.SC_SUCCEEDED,inputChunk
