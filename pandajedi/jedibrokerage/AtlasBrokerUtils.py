import re
import sys

from pandajedi.jedicore import Interaction
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import ProcessGroups


# get hospital queues
def getHospitalQueues(siteMapper):
    retMap = {}
    # hospital words
    goodWordList = ['CORE$','VL$','MEM$','MP\d+$','LONG$','_HIMEM','_\d+$']
    # loop over all clouds
    for tmpCloudName in siteMapper.getCloudList():
        # get cloud
        tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
        # get T1
        tmpT1Name = tmpCloudSpec['source']
        tmpT1Spec = siteMapper.getSite(tmpT1Name)
        # skip if DDM is undefined
        if tmpT1Spec.ddm == []:
            continue
        # loop over all sites
        for tmpSiteName in tmpCloudSpec['sites']:
            # skip T1 defined in cloudconfig
            if tmpSiteName == tmpT1Name:
                continue
            # check hospital words
            checkHospWord = False
            for tmpGoodWord in goodWordList:
                if re.search(tmpGoodWord,tmpSiteName) != None:
                    checkHospWord = True
                    break
            if not checkHospWord:
                continue
            # check site
            if not siteMapper.checkSite(tmpSiteName):
                continue
            tmpSiteSpec = siteMapper.getSite(tmpSiteName)
            # check DDM
            if tmpT1Spec.ddm == tmpSiteSpec.ddm:
                # append
                if not retMap.has_key(tmpCloudName):
                    retMap[tmpCloudName] = []
                if not tmpSiteName in retMap[tmpCloudName]:
                    retMap[tmpCloudName].append(tmpSiteName)
    # return
    return retMap

    

# get sites where data is available
def getSitesWithData(siteMapper,ddmIF,datasetName):
    # get replicas
    try:
        replicaMap= {}
        replicaMap[datasetName] = ddmIF.listDatasetReplicas(datasetName)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listDatasetReplicas failed with %s' % errvalue
    # loop over all clouds
    retMap = {}
    for tmpCloudName in siteMapper.cloudSpec.keys():
        retMap[tmpCloudName] = {'t1':{},'t2':[]}
        # get T1 DDM endpoints
        tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
        # FIXME until CERN-PROD_TZERO is added to cloudconfig.tier1SE
        if tmpCloudName == 'CERN':
            if not 'CERN-PROD_TZERO' in tmpCloudSpec['tier1SE']:
                tmpCloudSpec['tier1SE'].append('CERN-PROD_TZERO')
        for tmpSePat in tmpCloudSpec['tier1SE']:
            if '*' in tmpSePat:
                tmpSePat = tmpSePat.replace('*','.*')
            tmpSePat = '^' + tmpSePat +'$'
            for tmpSE in replicaMap[datasetName].keys():
                # check name with regexp pattern
                if re.search(tmpSePat,tmpSE) == None:
                    continue
                # check archived metadata
                # FIXME 
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'tape')
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1] 
                if tmpStatistics['found'] == None:
                    tmpDatasetStatus = 'unknown'
                    # refresh request
                    try:
                        ddmIF.checkDatasetConsistency(tmpSE,datasetName)
                    except:
                        pass
                elif tmpStatistics['total'] == tmpStatistics['found']:
                    tmpDatasetStatus = 'complete'
                else:
                    tmpDatasetStatus = 'incomplete'
                # append
                retMap[tmpCloudName]['t1'][tmpSE] = {'tape':tmpOnTape,'state':tmpDatasetStatus}
        # get T2 list
        tmpSiteList = DataServiceUtils.getSitesWithDataset(datasetName,siteMapper,replicaMap,
                                                           tmpCloudName,useHomeCloud=True,
                                                           useOnlineSite=True,includeT1=False)
        # append
        retMap[tmpCloudName]['t2'] = tmpSiteList
        # remove if empty
        if len(retMap[tmpCloudName]['t1']) == 0 and len(retMap[tmpCloudName]['t2']) == 0:
            del retMap[tmpCloudName]
    # return
    return Interaction.SC_SUCCEEDED,retMap



# get analysis sites where data is available
def getAnalSitesWithData(siteList,siteMapper,ddmIF,datasetName):
    # get replicas
    try:
        replicaMap= {}
        replicaMap[datasetName] = ddmIF.listDatasetReplicas(datasetName)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listDatasetReplicas failed with %s' % errvalue
    # loop over all clouds
    retMap = {}
    for tmpSiteName in siteList:
        tmpSiteSpec = siteMapper.getSite(tmpSiteName)
        # loop over all DDM endpoints
        checkedEndPoints = []
        for tmpDDM in [tmpSiteSpec.ddm] + tmpSiteSpec.setokens.values():
            # skip empty
            if tmpDDM == '':
                continue
            # get prefix
            tmpPrefix = re.sub('_[^_]+$','',tmpDDM) 
            # already checked 
            if tmpPrefix in checkedEndPoints:
                continue
            # DBR
            if DataServiceUtils.isCachedFile(datasetName,tmpSiteSpec):
                # no replica check since it is cached 
                if not retMap.has_key(tmpSiteName):
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpDDM] = {'tape':False,'state':'complete'}
                checkedEndPoints.append(tmpPrefix)
                continue
            checkedEndPoints.append(tmpPrefix)
            tmpSePat = '^' + tmpPrefix
            for tmpSE in replicaMap[datasetName].keys():
                # check name with regexp pattern
                if re.search(tmpSePat,tmpSE) == None:
                    continue
                # check archived metadata
                # FIXME 
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'tape')
                except:
                    errtype,errvalue = sys.exc_info()[:2]
                    return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1] 
                if tmpStatistics['found'] == None:
                    tmpDatasetStatus = 'unknown'
                    # refresh request
                    try:
                        ddmIF.checkDatasetConsistency(tmpSE,datasetName)
                    except:
                        pass
                elif tmpStatistics['total'] == tmpStatistics['found']:
                    tmpDatasetStatus = 'complete'
                else:
                    tmpDatasetStatus = 'incomplete'
                # append
                if not retMap.has_key(tmpSiteName):
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpSE] = {'tape':tmpOnTape,'state':tmpDatasetStatus}
    # return
    return Interaction.SC_SUCCEEDED,retMap



# get analysis sites where data is available at disk
def getAnalSitesWithDataDisk(dataSiteMap):
    siteList = []
    siteWithIncomp = []
    for tmpSiteName,tmpSeValMap in dataSiteMap.iteritems():
        for tmpSE,tmpValMap in tmpSeValMap.iteritems():
            # on disk 
            if not tmpValMap['tape']:
                if tmpValMap['state'] == 'complete':
                    # complete replica at disk
                    if not tmpSiteName in siteList:
                        siteList.append(tmpSiteName)
                    break
                else:
                    # incomplete replica at disk
                    if not tmpSiteName in siteWithIncomp:
                        siteWithIncomp.append(tmpSiteName)
    # return sites with complete
    if siteList != []:
        return siteList
    # return sites with incomplete if complete is unavailable
    return siteWithIncomp



# get sites which can remotely access source sites
def getSatelliteSites(siteList,taskBufferIF,siteMapper,protocol='xrd',nSites=5,threshold=0,
                      cutoff=25,maxWeight=0.5):
    # loop over all sites
    retVal = {}
    for siteName in siteList:
        # check if the site can be used as source
        tmpSiteSpec = siteMapper.getSite(siteName)
        if tmpSiteSpec.wansourcelimit <= 0:
            continue
        # get sites with better network connections to sources
        tmpStat,tmpVal = taskBufferIF.getBestNNetworkSites_JEDI(siteName,protocol,nSites,
                                                                threshold,cutoff,maxWeight,
                                                                useResultCache=3600)
        # DB failure
        if tmpStat == False:
            return {}
        # loop over all destinations 
        for tmpD,tmpW in tmpVal.iteritems():
            # use first or larger value
            tmpSiteSpec = siteMapper.getSite(tmpD)
            if not retVal.has_key(tmpD) or retVal[tmpD]['weight'] < tmpW:
                retVal[tmpD] = {'weight':tmpW,'source':[siteName]}
            elif retVal.has_key(tmpD) and retVal[tmpD]['weight'] == tmpW:
                retVal[tmpD]['source'].append(siteName)
    return retVal
                        


# get the number of jobs in a status
def getNumJobs(jobStatMap,computingSite,jobStatus,cloud=None,workQueue_ID=None):
    if not jobStatMap.has_key(computingSite):
        return 0
    nJobs = 0
    # loop over all clouds
    for tmpCloud,tmpCloudVal in jobStatMap[computingSite].iteritems():
        # cloud is specified
        if cloud != None and cloud != tmpCloud:
            continue
        # loop over all workQueues
        for tmpWorkQueue,tmpWorkQueueVal in tmpCloudVal.iteritems():
            # workQueue is defined
            if workQueue_ID != None and workQueue_ID != tmpWorkQueue:
                continue
            # loop over all job status
            for tmpJobStatus,tmpCount in tmpWorkQueueVal.iteritems():
                if tmpJobStatus == jobStatus:
                    nJobs += tmpCount
    # return
    return nJobs



# get mapping between sites and storage endpoints 
def getSiteStorageEndpointMap(siteList,siteMapper):
    # get T1s
    t1Map = {}
    for tmpCloudName in siteMapper.getCloudList():
        # get cloud
        tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
        # get T1
        tmpT1Name = tmpCloudSpec['source']
        # append
        t1Map[tmpT1Name] = tmpCloudName
    # loop over all sites
    retMap = {}
    for siteName in siteList:
        tmpSiteSpec = siteMapper.getSite(siteName)
        # use schedconfig.ddm
        retMap[siteName] = [tmpSiteSpec.ddm]
        for tmpEP in tmpSiteSpec.setokens.values():
            if tmpEP != '' and not tmpEP in retMap[siteName]:
                retMap[siteName].append(tmpEP)
        # use cloudconfig.tier1SE for T1       
        if t1Map.has_key(siteName):
            tmpCloudName = t1Map[siteName]
            # get cloud
            tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
            for tmpEP in tmpCloudSpec['tier1SE']:
                if tmpEP != '' and not tmpEP in retMap[siteName]:
                    retMap[siteName].append(tmpEP)
    # return
    return retMap



# check if the queue is suppressed
def hasZeroShare(siteSpec,taskSpec,tmpLog):
    # per-site share is undefined
    if siteSpec.fairsharePolicy in ['',None]:
        return False
    try:
        # get process group
        tmpProGroup = ProcessGroups.getProcessGroup(taskSpec.processingType)
        # loop over all policies
        for tmpItem in siteSpec.fairsharePolicy.split(','):
            if re.search('(^|,|:)id=',tmpItem) != None: 
                # new format
                tmpMatch = re.search('(^|,|:)id={0}:'.format(taskSpec.workQueue_ID),tmpItem)
                if tmpMatch != None:
                    tmpShare = tmpItem.split(':')[-1]
                    tmpSahre = tmpShare.replace('%','')
                    if tmpSahre == '0':
                        return True
                    else:
                        return False
            else:
                # old format
                tmpType  = None
                tmpGroup = None
                tmpShare = tmpItem.split(':')[-1]
                for tmpStr in tmpItem.split(':'):
                    if tmpStr.startswith('type='):
                        tmpType = tmpStr.split('=')[-1]
                    elif tmpStr.startswith('group='):
                        tmpGroup = tmpStr.split('=')[-1]
                # check matching for type
                if not tmpType in ['any',None]:
                    if '*' in tmpType:
                        tmpType = tmpType.replace('*','.*')
                    # type mismatch
                    if re.search('^'+tmpType+'$',tmpProGroup) == None:
                        continue
                # check matching for group
                if not tmpGroup in ['any',None] and taskSpec.workingGroup != None:
                    if '*' in tmpGroup:
                        tmpGroup = tmpGroup.replace('*','.*')
                    # group mismatch
                    if re.search('^'+tmpGroup+'$',taskSpec.workingGroup) == None:
                        continue
                # check share
                tmpShare = tmpItem.split(':')[-1]
                if tmpShare in ['0','0%']:
                    return True
                else:
                    return False
    except:
        errtype,errvalue = sys.exc_info()[:2]
        tmpLog.error('hasZeroShare failed with {0}:{1}'.format(errtype,errvalue))
    # return
    return False



# check if site name is matched with one of list items
def isMatched(siteName,nameList):
    for tmpName in nameList:
        # ignore empty
        if tmpName == '':
            continue
        # wild card
        if '*' in tmpName:
            tmpName = tmpName.replace('*','.*')
            if re.search(tmpName,siteName) != None:
                return True
        else:
            # normal pattern
            if tmpName == siteName:
                return True
    # return
    return False
