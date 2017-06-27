import re
import sys

from pandajedi.jedicore import Interaction
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import ProcessGroups


# get hospital queues
def getHospitalQueues(siteMapper,siteInNucleus=None,cloudForNucleus=None):
    retMap = {}
    # hospital words
    goodWordList = ['CORE$','VL$','MEM$','MP\d+$','LONG$','_HIMEM','_\d+$','SHORT$']
    # loop over all clouds
    if siteInNucleus == None:
        cloudList = siteMapper.getCloudList()
    else:
        # WORLD
        cloudList = [cloudForNucleus]
    for tmpCloudName in cloudList:
        # get cloud
        tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
        if siteInNucleus == None:
            # get T1
            tmpT1Name = tmpCloudSpec['source']
        else:
            tmpT1Name = siteInNucleus
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
def getSitesWithData(siteMapper,ddmIF,datasetName,storageToken=None):
    # get num of files
    try:
        if not datasetName.endswith('/'):
            totalNumDatasets = 1
        else:
            tmpDsMap = ddmIF.listDatasetsInContainer(datasetName)
            totalNumDatasets = len(tmpDsMap)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.ddmIF.getFilesInDataset failed with %s' % errvalue
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
                # check space token
                if not storageToken in ['',None,'NULL']:
                    seStr = ddmIF.getSiteProperty(tmpSE,'se')
                    try:
                        if seStr.split(':')[1] != storageToken:
                            continue
                    except:
                        pass
                # check archived metadata
                # FIXME 
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'is_tape')
                except:
                    continue
                    # errtype,errvalue = sys.exc_info()[:2]
                    # return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1] 
                if tmpStatistics['found'] == None:
                    tmpDatasetStatus = 'unknown'
                    pass
                elif tmpStatistics['total'] == tmpStatistics['found'] and tmpStatistics['total'] >= totalNumDatasets:
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



# get nuclei where data is available
def getNucleiWithData(siteMapper,ddmIF,datasetName,candidateNuclei=[],deepScan=False):
    # get replicas
    try:
        replicaMap = ddmIF.listReplicasPerDataset(datasetName,deepScan)
    except:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listReplicasPerDataset failed with %s' % errvalue
    # loop over all clouds
    retMap = {}
    for tmpNucleus,tmpNucleusSpec in siteMapper.nuclei.iteritems():
        if candidateNuclei != [] and not tmpNucleus in candidateNuclei:
            continue
        # loop over all datasets
        totalNum = 0
        totalSize = 0
        avaNumDisk = 0
        avaNumAny = 0
        avaSizeDisk = 0
        avaSizeAny = 0
        for tmpDataset,tmpRepMap in replicaMap.iteritems():
            tmpTotalNum = 0
            tmpTotalSize = 0
            tmpAvaNumDisk = 0
            tmpAvaNumAny = 0
            tmpAvaSizeDisk = 0
            tmpAvaSizeAny = 0
            # loop over all endpoints
            for tmpLoc,locData in tmpRepMap.iteritems():
                # get total
                if tmpTotalNum == 0:
                    tmpTotalNum = locData[0]['total']
                    tmpTotalSize = locData[0]['tsize']
                # check if the endpoint is associated
                if tmpNucleusSpec.isAssociatedEndpoint(tmpLoc):
                    tmpEndpoint = tmpNucleusSpec.getEndpoint(tmpLoc)
                    tmpAvaNum   = locData[0]['found']
                    tmpAvaSize  = locData[0]['asize']
                    # disk
                    if tmpEndpoint['is_tape'] != 'Y':
                        # complete replica is available at DISK
                        if tmpTotalNum == tmpAvaNum and tmpTotalNum > 0:
                            tmpAvaNumDisk  = tmpAvaNum
                            tmpAvaNumAny   = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                            tmpAvaSizeAny  = tmpAvaSize
                            break
                        if tmpAvaNum > tmpAvaNumDisk:
                            tmpAvaNumDisk  = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                    # tape
                    if tmpAvaNumAny < tmpAvaNum:
                        tmpAvaNumAny  = tmpAvaNum
                        tmpAvaSizeAny = tmpAvaSize
            # total
            totalNum     = tmpTotalNum 
            totalSize    = tmpTotalSize
            avaNumDisk  += tmpAvaNumDisk
            avaNumAny   += tmpAvaNumAny
            avaSizeDisk += tmpAvaSizeDisk
            avaSizeAny  += tmpAvaSizeAny
        # append
        if tmpNucleus in candidateNuclei or avaNumAny > 0:
            retMap[tmpNucleus] = {'tot_num'       : totalNum,
                                  'tot_size'      : totalSize,
                                  'ava_num_disk'  : avaNumDisk,
                                  'ava_num_any'   : avaNumAny,
                                  'ava_size_disk' : avaSizeDisk,
                                  'ava_size_any'  : avaSizeAny,
                                  }
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
            tmpPrefix = re.sub('_[^_]+$','_',tmpDDM) 
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
                # skip staging
                if re.search('STAGING$',tmpSE) != None:
                    continue
                # check archived metadata
                # FIXME 
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'is_tape')
                except:
                    continue
                    # errtype,errvalue = sys.exc_info()[:2]
                    # return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1] 
                if tmpStatistics['found'] == None:
                    tmpDatasetStatus = 'unknown'
                    # refresh request
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
def getAnalSitesWithDataDisk(dataSiteMap,includeTape=False):
    siteList = []
    siteWithIncomp = []
    for tmpSiteName,tmpSeValMap in dataSiteMap.iteritems():
        for tmpSE,tmpValMap in tmpSeValMap.iteritems():
            # on disk or tape
            if includeTape or not tmpValMap['tape']:
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
def getSatelliteSites(siteList,taskBufferIF,siteMapper,protocol='xrd',nSites=5,threshold=0.5,
                      cutoff=15,maxWeight=0.5):
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
            # skip source sites
            if tmpD in siteList:
                continue
            # use first or larger value
            tmpSiteSpec = siteMapper.getSite(tmpD)
            if not retVal.has_key(tmpD) or retVal[tmpD]['weight'] < tmpW:
                retVal[tmpD] = {'weight':tmpW,'source':[siteName]}
            elif retVal.has_key(tmpD) and retVal[tmpD]['weight'] == tmpW:
                retVal[tmpD]['source'].append(siteName)
    return retVal
                        


# get the number of jobs in a status
def getNumJobs(jobStatMap, computingSite, jobStatus, cloud=None, workQueue_tag=None):
    if not jobStatMap.has_key(computingSite):
        return 0
    nJobs = 0
    # loop over all workQueues
    for tmpWorkQueue, tmpWorkQueueVal in jobStatMap[computingSite].iteritems():
        # workQueue is defined
        if workQueue_tag is not None and workQueue_tag != tmpWorkQueue:
            continue
        # loop over all job status
        for tmpJobStatus, tmpCount in tmpWorkQueueVal.iteritems():
            if tmpJobStatus == jobStatus:
                nJobs += tmpCount
    # return
    return nJobs



# get mapping between sites and storage endpoints
def getSiteStorageEndpointMap(siteList,siteMapper,ignoreCC=False):
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
        if not ignoreCC and t1Map.has_key(siteName):
            tmpCloudName = t1Map[siteName]
            # get cloud
            tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
            for tmpEP in tmpCloudSpec['tier1SE']:
                if tmpEP != '' and not tmpEP in retMap[siteName]:
                    retMap[siteName].append(tmpEP)
    # return
    return retMap



# check if the queue is suppressed
def hasZeroShare(siteSpec, taskSpec, ignorePrio, tmpLog):
    # per-site share is undefined
    if siteSpec.fairsharePolicy in ['',None]:
        return False
    try:
        # get process group
        tmpProGroup = ProcessGroups.getProcessGroup(taskSpec.processingType)
        # no suppress for test queues
        if tmpProGroup in ['test']:
            return False
        # loop over all policies
        for tmpItem in siteSpec.fairsharePolicy.split(','):
            if re.search('(^|,|:)id=',tmpItem) != None: 
                # new format
                tmpMatch = re.search('(^|,|:)id={0}:'.format(taskSpec.workQueue_ID),tmpItem)
                if tmpMatch != None:
                    # check priority if any
                    tmpPrio = None
                    for tmpStr in tmpItem.split(':'):
                        if tmpStr.startswith('priority'):
                            tmpPrio = re.sub('priority','',tmpStr)
                            break
                    if tmpPrio != None:
                        try:
                            exec "tmpStat = {0}{1}".format(taskSpec.currentPriority,tmpPrio)
                            if not tmpStat:
                                continue
                        except:
                            pass
                    # check share
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
                tmpPrio  = None
                tmpShare = tmpItem.split(':')[-1]
                for tmpStr in tmpItem.split(':'):
                    if tmpStr.startswith('type='):
                        tmpType = tmpStr.split('=')[-1]
                    elif tmpStr.startswith('group='):
                        tmpGroup = tmpStr.split('=')[-1]
                    elif tmpStr.startswith('priority'):
                        tmpPrio = re.sub('priority','',tmpStr)
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
                # check priority
                if tmpPrio != None and not ignorePrio:
                    try:
                        exec "tmpStat = {0}{1}".format(taskSpec.currentPriority,tmpPrio)
                        if not tmpStat:
                            continue
                    except:
                        pass
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



# get dict to set nucleus
def getDictToSetNucleus(nucleusSpec,tmpDatasetSpecs):
    # get destinations
    retMap = {'datasets':[],'nucleus':nucleusSpec.name}
    for datasetSpec in tmpDatasetSpecs:
        # skip distributed datasets
        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) != None:
            continue
        # get token
        endPoint = nucleusSpec.getAssoicatedEndpoint(datasetSpec.storageToken)
        if endPoint == None:
            continue
        token = endPoint['ddm_endpoint_name']
        # add origianl token
        if not datasetSpec.storageToken in ['',None]:
            token += '/{0}'.format(datasetSpec.storageToken.split('/')[-1])
        retMap['datasets'].append({'datasetID':datasetSpec.datasetID,
                                   'token':'dst:{0}'.format(token),
                                   'destination':'nucleus:{0}'.format(nucleusSpec.name)})
    return retMap



# remove problematic sites
def skipProblematicSites(candidateSpecList,ngSites,sitesUsedByTask,preSetSiteSpec,maxNumSites,timeWindow,tmpLog):
    newcandidateSpecList = []
    skippedSites = set()
    usedSitesAll = []
    usedSitesGood = []
    newSitesAll = []
    newSitesGood = []
    # collect sites already used by the task
    for candidateSpec in candidateSpecList:
        # check if problematic
        isGood = True
        if candidateSpec.siteName in ngSites and \
                (preSetSiteSpec == None or candidateSpec.siteName != preSetSiteSpec.siteName):
            isGood = False
            skippedSites.add(candidateSpec.siteName)
        # check if used
        if candidateSpec.siteName in sitesUsedByTask:
            usedSitesAll.append(candidateSpec)
            if isGood:
                usedSitesGood.append(candidateSpec)
        else:
            newSitesAll.append(candidateSpec)
            if isGood:
                newSitesGood.append(candidateSpec)
    # unlimit number of sites if undefined
    if maxNumSites in [0,None]:
        maxNumSites = len(candidateSpec)
    # only used sites are enough
    if len(usedSitesAll) >= maxNumSites:
        # no good used sites
        if len(usedSitesGood) == 0:
            newcandidateSpecList = usedSitesAll
            # disable dump
            skippedSites = []
        else:
            newcandidateSpecList = usedSitesGood
    else:
        # no good used sites
        if len(usedSitesGood) == 0:
            # some good new sites are there
            if len(newSitesGood) > 0:
                newcandidateSpecList = newSitesGood[:maxNumSites-len(usedSitesAll)]
            else:
                # no good used and new sites
                newcandidateSpecList = usedSitesAll+newSitesAll[:maxNumSites-len(usedSitesAll)]
                # disable dump
                skippedSites = []
        else:
            # some good new sites are there
            if len(newSitesGood) > 0:
                newcandidateSpecList = usedSitesGood+newSitesGood[:maxNumSites-len(usedSitesAll)]
            else:
                # only good used sites
                newcandidateSpecList = usedSitesGood 
    # dump
    for skippedSite in skippedSites:
        tmpLog.debug('  skip {0} too many closed or failed for last {1}hr'.format(skippedSite,timeWindow))
    return newcandidateSpecList
