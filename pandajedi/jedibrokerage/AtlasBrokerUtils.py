import re
import sys
import json
import datetime
import time
import os
import socket

from six import iteritems

from pandajedi.jedicore import Interaction
from pandaserver.dataservice import DataServiceUtils
from dataservice.DataServiceUtils import select_scope
from pandaserver.taskbuffer import ProcessGroups, JobUtils

# get hospital queues
def getHospitalQueues(siteMapper, prodSourceLabel, job_label, siteInNucleus=None, cloudForNucleus=None):
    retMap = {}
    # hospital words
    goodWordList = ['CORE$', 'VL$', 'MEM$', 'MP\d+$', 'LONG$', '_HIMEM', '_\d+$','SHORT$']
    # loop over all clouds
    if siteInNucleus is None:
        cloudList = siteMapper.getCloudList()
    else:
        # WORLD
        cloudList = [cloudForNucleus]
    for tmpCloudName in cloudList:
        # get cloud
        tmpCloudSpec = siteMapper.getCloud(tmpCloudName)
        if siteInNucleus is None:
            # get T1
            tmpT1Name = tmpCloudSpec['source']
        else:
            tmpT1Name = siteInNucleus
        tmpT1Spec = siteMapper.getSite(tmpT1Name)
        scope_t1_input, scope_t1_output = select_scope(tmpT1Spec, prodSourceLabel, job_label)
        # skip if DDM is undefined
        if not tmpT1Spec.ddm_output[scope_t1_output]:
            continue
        # loop over all sites
        for tmpSiteName in tmpCloudSpec['sites']:
            # skip T1 defined in cloudconfig
            if tmpSiteName == tmpT1Name:
                continue
            # check hospital words
            checkHospWord = False
            for tmpGoodWord in goodWordList:
                if re.search(tmpGoodWord,tmpSiteName) is not None:
                    checkHospWord = True
                    break
            if not checkHospWord:
                continue
            # check site
            if not siteMapper.checkSite(tmpSiteName):
                continue
            tmpSiteSpec = siteMapper.getSite(tmpSiteName)
            scope_tmpSite_input, scope_tmpSite_output = select_scope(tmpSiteSpec, prodSourceLabel, job_label)
            # check DDM
            if scope_t1_output in tmpT1Spec.ddm_output and scope_tmpSite_output in tmpSiteSpec.ddm_output and tmpT1Spec.ddm_output[scope_t1_output] == tmpSiteSpec.ddm_output[scope_tmpSite_output]:
                # append
                if tmpCloudName not in retMap:
                    retMap[tmpCloudName] = []
                if tmpSiteName not in retMap[tmpCloudName]:
                    retMap[tmpCloudName].append(tmpSiteName)
    # return
    return retMap



# get sites where data is available
def getSitesWithData(siteMapper, ddmIF, datasetName, prodsourcelabel, job_label, storageToken=None):
    # get num of files
    try:
        if not datasetName.endswith('/'):
            totalNumDatasets = 1
        else:
            tmpDsMap = ddmIF.listDatasetsInContainer(datasetName)
            totalNumDatasets = len(tmpDsMap)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listDatasetsInContainer failed with %s' % errvalue
    # get replicas
    try:
        replicaMap= {}
        replicaMap[datasetName] = ddmIF.listDatasetReplicas(datasetName)
    except Exception:
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
            if 'CERN-PROD_TZERO' not in tmpCloudSpec['tier1SE']:
                tmpCloudSpec['tier1SE'].append('CERN-PROD_TZERO')
        for tmpSePat in tmpCloudSpec['tier1SE']:
            if '*' in tmpSePat:
                tmpSePat = tmpSePat.replace('*','.*')
            tmpSePat = '^' + tmpSePat +'$'
            for tmpSE in replicaMap[datasetName].keys():
                # check name with regexp pattern
                if re.search(tmpSePat,tmpSE) is None:
                    continue
                # check space token
                if storageToken not in ['',None,'NULL']:
                    seStr = ddmIF.getSiteProperty(tmpSE,'se')
                    try:
                        if seStr.split(':')[1] != storageToken:
                            continue
                    except Exception:
                        pass
                # check archived metadata
                # FIXME
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'is_tape')
                except Exception:
                    continue
                    # errtype,errvalue = sys.exc_info()[:2]
                    # return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1]
                if tmpStatistics['found'] is None:
                    tmpDatasetStatus = 'unknown'
                    pass
                elif tmpStatistics['total'] == tmpStatistics['found'] and tmpStatistics['total'] >= totalNumDatasets:
                    tmpDatasetStatus = 'complete'
                else:
                    tmpDatasetStatus = 'incomplete'
                # append
                retMap[tmpCloudName]['t1'][tmpSE] = {'tape':tmpOnTape,'state':tmpDatasetStatus}
        # get T2 list
        tmpSiteList = DataServiceUtils.getSitesWithDataset(datasetName, siteMapper, replicaMap,
                                                           tmpCloudName, prodsourcelabel, job_label,
                                                           useHomeCloud=True, useOnlineSite=True, includeT1=False)
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
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listReplicasPerDataset failed with %s' % errvalue
    # loop over all clouds
    retMap = {}
    for tmpNucleus,tmpNucleusSpec in iteritems(siteMapper.nuclei):
        if candidateNuclei != [] and tmpNucleus not in candidateNuclei:
            continue
        # loop over all datasets
        totalNum = 0
        totalSize = 0
        avaNumDisk = 0
        avaNumAny = 0
        avaSizeDisk = 0
        avaSizeAny = 0
        for tmpDataset,tmpRepMap in iteritems(replicaMap):
            tmpTotalNum = 0
            tmpTotalSize = 0
            tmpAvaNumDisk = 0
            tmpAvaNumAny = 0
            tmpAvaSizeDisk = 0
            tmpAvaSizeAny = 0
            # loop over all endpoints
            for tmpLoc,locData in iteritems(tmpRepMap):
                # get total
                if tmpTotalNum == 0:
                    tmpTotalNum = locData[0]['total']
                    tmpTotalSize = locData[0]['tsize']
                # check if the endpoint is associated
                if tmpNucleusSpec.is_associated_for_input(tmpLoc):
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
        replicaMap[datasetName] = ddmIF.listDatasetReplicas(datasetName, use_vp=True)
    except Exception:
        errtype,errvalue = sys.exc_info()[:2]
        return errtype,'ddmIF.listDatasetReplicas failed with %s' % errvalue
    # loop over all clouds
    retMap = {}
    for tmpSiteName in siteList:
        if not siteMapper.checkSite(tmpSiteName):
            continue
        tmpSiteSpec = siteMapper.getSite(tmpSiteName)
        scope_input, scope_output = select_scope(tmpSiteSpec, JobUtils.ANALY_PS, JobUtils.ANALY_PS)

        # loop over all DDM endpoints
        checkedEndPoints = []
        try:
            input_endpoints = tmpSiteSpec.ddm_endpoints_input[scope_input].all.keys()
        except Exception:
            input_endpoints = {}
        for tmpDDM in input_endpoints:
            # skip empty
            if tmpDDM == '':
                continue
            # get prefix
            # tmpPrefix = re.sub('_[^_]+$','_',tmpDDM)
            tmpPrefix = tmpDDM

            # already checked
            if tmpPrefix in checkedEndPoints:
                continue
            # DBR
            if DataServiceUtils.isCachedFile(datasetName,tmpSiteSpec):
                # no replica check since it is cached
                if tmpSiteName not in retMap:
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpDDM] = {'tape': False, 'state': 'complete'}
                checkedEndPoints.append(tmpPrefix)
                continue
            checkedEndPoints.append(tmpPrefix)
            tmpSePat = '^' + tmpPrefix
            for tmpSE in replicaMap[datasetName].keys():
                # check name with regexp pattern
                if re.search(tmpSePat,tmpSE) is None:
                    continue
                # skip staging
                if re.search('STAGING$',tmpSE) is not None:
                    continue
                # check archived metadata
                # FIXME
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE,'is_tape')
                except Exception:
                    continue
                    # errtype,errvalue = sys.exc_info()[:2]
                    # return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1]
                if tmpStatistics['found'] is None:
                    tmpDatasetStatus = 'unknown'
                    # refresh request
                    pass
                elif tmpStatistics['total'] == tmpStatistics['found']:
                    tmpDatasetStatus = 'complete'
                else:
                    tmpDatasetStatus = 'incomplete'
                # append
                if tmpSiteName not in retMap:
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpSE] = {'tape':tmpOnTape,'state':tmpDatasetStatus}
                if 'vp' in tmpStatistics:
                    retMap[tmpSiteName][tmpSE]['vp'] = tmpStatistics['vp']
    # return
    return Interaction.SC_SUCCEEDED,retMap



# get analysis sites where data is available at disk
def getAnalSitesWithDataDisk(dataSiteMap, includeTape=False, use_vp=True, use_incomplete=False):
    siteList = []
    siteWithIncomp = []
    for tmpSiteName,tmpSeValMap in iteritems(dataSiteMap):
        for tmpSE,tmpValMap in iteritems(tmpSeValMap):
            # VP
            if not use_vp and 'vp' in tmpValMap and tmpValMap['vp'] is True:
                continue
            # on disk or tape
            if includeTape or not tmpValMap['tape']:
                if tmpValMap['state'] == 'complete':
                    # complete replica at disk
                    if tmpSiteName not in siteList:
                        siteList.append(tmpSiteName)
                    break
                else:
                    # incomplete replica at disk
                    if tmpSiteName not in siteWithIncomp:
                        siteWithIncomp.append(tmpSiteName)
    # return sites with complete
    if siteList != [] or not use_incomplete:
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
        if tmpStat is False:
            return {}
        # loop over all destinations
        for tmpD,tmpW in iteritems(tmpVal):
            # skip source sites
            if tmpD in siteList:
                continue
            # use first or larger value
            tmpSiteSpec = siteMapper.getSite(tmpD)
            if tmpD not in retVal or retVal[tmpD]['weight'] < tmpW:
                retVal[tmpD] = {'weight':tmpW,'source':[siteName]}
            elif tmpD in retVal and retVal[tmpD]['weight'] == tmpW:
                retVal[tmpD]['source'].append(siteName)
    return retVal



# get the number of jobs in a status
def getNumJobs(jobStatMap, computingSite, jobStatus, cloud=None, workQueue_tag=None):
    if computingSite not in jobStatMap:
        return 0
    nJobs = 0
    # loop over all workQueues
    for tmpWorkQueue, tmpWorkQueueVal in iteritems(jobStatMap[computingSite]):
        # workQueue is defined
        if workQueue_tag is not None and workQueue_tag != tmpWorkQueue:
            continue
        # loop over all job status
        for tmpJobStatus, tmpCount in iteritems(tmpWorkQueueVal):
            if tmpJobStatus == jobStatus:
                nJobs += tmpCount
    # return
    return nJobs


# get the total number of jobs in a status
def get_total_nq_nr_ratio(job_stat_map, work_queue_tag=None):
    nRunning = 0
    nQueue = 0
    # loop over all workQueues
    for siteVal in job_stat_map.values():
        for tmpWorkQueue in siteVal:
            # workQueue is defined
            if work_queue_tag is not None and work_queue_tag != tmpWorkQueue:
                continue
            tmpWorkQueueVal = siteVal[tmpWorkQueue]
            # loop over all job status
            for tmpJobStatus in ['defined', 'assigned', 'activated', 'starting']:
                if tmpJobStatus in tmpWorkQueueVal:
                    nQueue += tmpWorkQueueVal[tmpJobStatus]
            if 'running' in tmpWorkQueueVal:
                nRunning += tmpWorkQueueVal['running']
    try:
        ratio = float(nQueue) / float(nRunning)
    except Exception:
        ratio = None
    # return
    return ratio

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
            if re.search('(^|,|:)id=',tmpItem) is not None:
                # new format
                tmpMatch = re.search('(^|,|:)id={0}:'.format(taskSpec.workQueue_ID),tmpItem)
                if tmpMatch is not None:
                    # check priority if any
                    tmpPrio = None
                    for tmpStr in tmpItem.split(':'):
                        if tmpStr.startswith('priority'):
                            tmpPrio = re.sub('priority','',tmpStr)
                            break
                    if tmpPrio is not None:
                        try:
                            exec("tmpStat = {0}{1}".format(taskSpec.currentPriority,tmpPrio), globals())
                            if not tmpStat:
                                continue
                        except Exception:
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
                if tmpType not in ['any',None]:
                    if '*' in tmpType:
                        tmpType = tmpType.replace('*','.*')
                    # type mismatch
                    if re.search('^'+tmpType+'$',tmpProGroup) is None:
                        continue
                # check matching for group
                if tmpGroup not in ['any',None] and taskSpec.workingGroup is not None:
                    if '*' in tmpGroup:
                        tmpGroup = tmpGroup.replace('*','.*')
                    # group mismatch
                    if re.search('^'+tmpGroup+'$',taskSpec.workingGroup) is None:
                        continue
                # check priority
                if tmpPrio is not None and not ignorePrio:
                    try:
                        exec("tmpStat = {0}{1}".format(taskSpec.currentPriority,tmpPrio), globals())
                        if not tmpStat:
                            continue
                    except Exception:
                        pass
                # check share
                tmpShare = tmpItem.split(':')[-1]
                if tmpShare in ['0','0%']:
                    return True
                else:
                    return False
    except Exception:
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
            if re.search(tmpName,siteName) is not None:
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
        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
            continue
        # get token
        endPoint = nucleusSpec.getAssociatedEndpoint(datasetSpec.storageToken)
        if endPoint is None:
            continue
        token = endPoint['ddm_endpoint_name']
        # add original token
        if datasetSpec.storageToken not in ['',None]:
            token += '/{0}'.format(datasetSpec.storageToken.split('/')[-1])
        retMap['datasets'].append({'datasetID':datasetSpec.datasetID,
                                   'token':'dst:{0}'.format(token),
                                   'destination':'nucleus:{0}'.format(nucleusSpec.name)})
    return retMap


# remove problematic sites
def skipProblematicSites(candidateSpecList,ngSites,sitesUsedByTask,preSetSiteSpec,maxNumSites,timeWindow,tmpLog):
    skippedSites = set()
    usedSitesGood = []
    newSitesGood = []
    # collect sites already used by the task
    for candidateSpec in candidateSpecList:
        # check if problematic
        if (candidateSpec.siteName in ngSites or candidateSpec.unifiedName in ngSites) and \
                (preSetSiteSpec is None or candidateSpec.siteName != preSetSiteSpec.siteName):
            skippedSites.add(candidateSpec.siteName)
        else:
            if (candidateSpec.siteName in sitesUsedByTask or candidateSpec.unifiedName in sitesUsedByTask):
                usedSitesGood.append(candidateSpec)
            else:
                newSitesGood.append(candidateSpec)
    # set number of sites if undefined
    if maxNumSites in [0,None]:
        maxNumSites = len(candidateSpecList)
    newcandidateSpecList = usedSitesGood + newSitesGood
    newcandidateSpecList = newcandidateSpecList[:maxNumSites]
    # dump
    for skippedSite in skippedSites:
        tmpLog.debug('getting rid of problematic site {0}'.format(skippedSite))
    return newcandidateSpecList




# get mapping between sites and input storage endpoints
def getSiteInputStorageEndpointMap(site_list, site_mapper, prod_source_label, job_label, ignore_cc=False):

    # make a map of the t1 to its respective cloud
    t1_map = {}
    for tmp_cloud_name in site_mapper.getCloudList():
        # get cloud
        tmp_cloud_spec = site_mapper.getCloud(tmp_cloud_name)
        # get T1
        tmp_t1_name = tmp_cloud_spec['source']
        # append
        t1_map[tmp_t1_name] = tmp_cloud_name

    # make a map of panda sites to ddm endpoints
    ret_map = {}
    for site_name in site_list:
        tmp_site_spec = site_mapper.getSite(site_name)
        scope_input, scope_output = select_scope(tmp_site_spec, prod_source_label, job_label)

        # skip if scope not available
        if scope_input not in tmp_site_spec.ddm_endpoints_input:
            continue

        # add the schedconfig.ddm endpoints
        ret_map[site_name] = list(tmp_site_spec.ddm_endpoints_input[scope_input].all.keys())

        # add the cloudconfig.tier1SE for T1s
        if not ignore_cc and site_name in t1_map:
            tmp_cloud_name = t1_map[site_name]
            tmp_cloud_spec = site_mapper.getCloud(tmp_cloud_name)
            for tmp_endpoint in tmp_cloud_spec['tier1SE']:
                if tmp_endpoint and tmp_endpoint not in ret_map[site_name]:
                    ret_map[site_name].append(tmp_endpoint)
    # return
    return ret_map


# get lists of mandatory or inconvertible architectures
def getOkNgArchList(task_spec):
    if task_spec.termCondition:
        if '.el6.' in task_spec.termCondition:
            return (None, ['x86_64-centos7-%'])
        if '.el7.' in task_spec.termCondition:
            return (['x86_64-centos7-%'], None)
    return (None, None)


# get to-running rate of sites from various resources
CACHE_SiteToRunRateStats = {}
def getSiteToRunRateStats(tbIF, vo, time_window=21600, cutoff=300, cache_lifetime=600):
    # initialize
    ret_val = False
    ret_map = {}
    # DB cache keys
    dc_main_key = 'AtlasSites'
    dc_sub_key = 'SiteToRunRate'
    # arguments for process lock
    this_prodsourcelabel = 'user'
    this_pid = '{0}-{1}_{2}-broker'.format(socket.getfqdn().split('.')[0], os.getpid(), os.getpgrp())
    this_component = 'Cache.SiteToRunRate'
    # timestamps
    current_time = datetime.datetime.utcnow()
    starttime_max = current_time - datetime.timedelta(seconds=cutoff)
    starttime_min = current_time - datetime.timedelta(seconds=time_window)
    # rounded with 10 minutes
    starttime_max_rounded = starttime_max.replace(minute=starttime_max.minute//10*10, second=0, microsecond=0)
    starttime_min_rounded = starttime_min.replace(minute=starttime_min.minute//10*10, second=0, microsecond=0)
    real_interval_hours = (starttime_max_rounded - starttime_min_rounded).total_seconds()/3600
    # local cache key
    local_cache_key = (starttime_min_rounded, starttime_max_rounded)
    # condition of query
    if local_cache_key in CACHE_SiteToRunRateStats \
        and current_time <= CACHE_SiteToRunRateStats[local_cache_key]['exp']:
        # query from local cache
        ret_val = True
        ret_map = CACHE_SiteToRunRateStats[local_cache_key]['data']
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if datetime.datetime.utcnow() - current_time > datetime.timedelta(seconds=min(10, cache_lifetime/4)):
                # break trying
                break
            try:
                # query from DB cache
                cache_spec = tbIF.getCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key)
                if cache_spec is not None:
                    expired_time = cache_spec.last_update + datetime.timedelta(seconds=cache_lifetime)
                    if current_time <= expired_time:
                        # valid DB cache
                        ret_val = True
                        ret_map = json.loads(cache_spec.data)
                        # fill local cache
                        CACHE_SiteToRunRateStats[local_cache_key] = {   'exp': expired_time,
                                                                        'data': ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(   vo=vo, prodSourceLabel=this_prodsourcelabel,
                                                    cloud=None, workqueue_id=None,
                                                    resource_name=None,
                                                    component=this_component,
                                                    pid=this_pid, timeLimit=5)
                if not got_lock:
                    # not getting lock, sleep and query cache again
                    time.sleep(1)
                    continue
                # query from PanDA DB directly
                ret_val, ret_map = tbIF.getSiteToRunRateStats(  vo=vo, exclude_rwq=False,
                                                                starttime_min=starttime_min,
                                                                starttime_max=starttime_max)
                if ret_val:
                    # expired time
                    expired_time = current_time + datetime.timedelta(seconds=cache_lifetime)
                    # fill local cache
                    CACHE_SiteToRunRateStats[local_cache_key] = {   'exp': expired_time,
                                                                    'data': ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                # unlock process
                tbIF.unlockProcess_JEDI(vo=vo, prodSourceLabel=this_prodsourcelabel,
                                        cloud=None, workqueue_id=None,
                                        resource_name=None,
                                        component=this_component, pid=this_pid)
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = 'AtlasBrokerUtils.getSiteToRunRateStats got {0}: {1} \n'.format(e.__class__.__name__, e)
                sys.stderr.write(err_str)
                # break trying
                break
        # delete outdated entries in local cache
        for lc_key in list(CACHE_SiteToRunRateStats.keys()):
            lc_time_min, lc_time_max = lc_key
            if lc_time_max < starttime_max_rounded - datetime.timedelta(seconds=cache_lifetime):
                try:
                    del CACHE_SiteToRunRateStats[lc_key]
                except Exception as e:
                    err_str = 'AtlasBrokerUtils.getSiteToRunRateStats when deleting outdated entries got {0}: {1} \n'.format(e.__class__.__name__, e)
                    sys.stderr.write(err_str)
    # return
    return ret_val, ret_map


# get users jobs stats from various resources
CACHE_UsersJobsStats = {}
def getUsersJobsStats(tbIF, vo, prod_source_label, cache_lifetime=60):
    # initialize
    ret_val = False
    ret_map = {}
    # DB cache keys
    dc_main_key = 'AtlasSites'
    dc_sub_key = 'UsersJobsStats'
    # arguments for process lock
    this_prodsourcelabel = prod_source_label
    this_pid = this_pid = '{0}-{1}_{2}-broker'.format(socket.getfqdn().split('.')[0], os.getpid(), os.getpgrp())
    this_component = 'Cache.UsersJobsStats'
    # local cache key; a must if not using global variable
    local_cache_key = '_main'
    # timestamps
    current_time = datetime.datetime.utcnow()
    # condition of query
    if local_cache_key in CACHE_UsersJobsStats \
        and current_time <= CACHE_UsersJobsStats[local_cache_key]['exp']:
        # query from local cache
        ret_val = True
        ret_map = CACHE_UsersJobsStats[local_cache_key]['data']
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if datetime.datetime.utcnow() - current_time > datetime.timedelta(seconds=min(15, cache_lifetime/4)):
                # break trying
                break
            try:
                # query from DB cache
                cache_spec = tbIF.getCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key)
                if cache_spec is not None:
                    expired_time = cache_spec.last_update + datetime.timedelta(seconds=cache_lifetime)
                    if current_time <= expired_time:
                        # valid DB cache
                        ret_val = True
                        ret_map = json.loads(cache_spec.data)
                        # fill local cache
                        CACHE_UsersJobsStats[local_cache_key] = {'exp': expired_time, 'data': ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(   vo=vo, prodSourceLabel=this_prodsourcelabel,
                                                    cloud=None, workqueue_id=None,
                                                    resource_name=None,
                                                    component=this_component,
                                                    pid=this_pid, timeLimit=(cache_lifetime*0.75/60))
                if not got_lock:
                    # not getting lock, sleep and query cache again
                    time.sleep(1)
                    continue
                # query from PanDA DB directly
                ret_map = tbIF.getUsersJobsStats_JEDI(prod_source_label=this_prodsourcelabel)
                if ret_map is not None:
                    # expired time
                    expired_time = current_time + datetime.timedelta(seconds=cache_lifetime)
                    # fill local cache
                    CACHE_UsersJobsStats[local_cache_key] = {'exp': expired_time, 'data': ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                    # make True return
                    ret_val = True
                # unlock process
                tbIF.unlockProcess_JEDI(vo=vo, prodSourceLabel=this_prodsourcelabel,
                                        cloud=None, workqueue_id=None,
                                        resource_name=None,
                                        component=this_component, pid=this_pid)
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = 'AtlasBrokerUtils.getUsersJobsStats got {0}: {1} \n'.format(e.__class__.__name__, e)
                sys.stderr.write(err_str)
                # break trying
                break
    # return
    return ret_val, ret_map


# check SW with json
class JsonSoftwareCheck:
    # constructor
    def __init__(self, site_mapper, json_name=None):
        if json_name is None:
            json_name = '/cvmfs/atlas.cern.ch/repo/sw/local/etc/cric_pandaqueue_tags.json'
        self.siteMapper = site_mapper
        try:
            self.swDict = json.load(open(json_name))
        except Exception:
            self.swDict = dict()

    # get lists
    def check(self, site_list, cvmfs_tag, sw_project, sw_version, cmt_config, need_cvmfs, cmt_config_only,
              need_container=False, container_name=None, only_tags_fc=False):
        okSite = []
        noAutoSite = []
        for tmpSiteName in site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.releases == ['AUTO'] and tmpSiteName in self.swDict:
                # check for fat container
                if container_name:
                    # check architecture
                    if cmt_config and 'architecture' in self.swDict[tmpSiteName] and \
                            cmt_config not in self.swDict[tmpSiteName]:
                        continue
                    # check for container
                    if not only_tags_fc and ('any' in self.swDict[tmpSiteName]["containers"] or
                            '/cvmfs' in self.swDict[tmpSiteName]["containers"]):
                        # any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([t['container_name'] for t in self.swDict[tmpSiteName]['tags']
                                                if t['container_name']]):
                        # logical name in tags or any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([s for t in self.swDict[tmpSiteName]['tags'] for s in t['sources']
                                               if t['sources']]):
                        # full path in sources
                        okSite.append(tmpSiteName)
                    elif not only_tags_fc:
                        # get sources in all tag list
                        if 'ALL' in self.swDict:
                            source_list_in_all_tag = [s for t in self.swDict['ALL']['tags']
                                                      for s in t['sources'] if t['container_name']==container_name]
                        else:
                            source_list_in_all_tag = []
                        # prefix with full path
                        for tmp_prefix in self.swDict[tmpSiteName]["containers"]:
                            if container_name.startswith(tmp_prefix):
                                okSite.append(tmpSiteName)
                                break
                            toBreak = False
                            for source_in_all_tag in source_list_in_all_tag:
                                if source_in_all_tag.startswith(tmp_prefix):
                                    okSite.append(tmpSiteName)
                                    toBreak = True
                                    break
                            if toBreak:
                                break
                    continue
                # only cmt config check
                if cmt_config_only:
                    if cmt_config in self.swDict[tmpSiteName]['cmtconfigs']:
                        okSite.append(tmpSiteName)
                    continue
                # check if CVMFS is available
                if 'any' in self.swDict[tmpSiteName]["cvmfs"] or cvmfs_tag in self.swDict[tmpSiteName]["cvmfs"]:
                    # check if container is available
                    if 'any' in self.swDict[tmpSiteName]["containers"] or \
                            '/cvmfs' in self.swDict[tmpSiteName]["containers"]:
                        okSite.append(tmpSiteName)
                    # check cmt config
                    elif not need_container and cmt_config in self.swDict[tmpSiteName]['cmtconfigs']:
                        okSite.append(tmpSiteName)
                elif not need_cvmfs:
                    if not need_container or 'any' in self.swDict[tmpSiteName]["containers"]:
                        # check tags
                        for tag in self.swDict[tmpSiteName]["tags"]:
                            if tag['cmtconfig'] == cmt_config and tag['project'] == sw_project \
                               and tag['release'] == sw_version:
                                okSite.append(tmpSiteName)
                                break
                # don't pass to subsequent check if AUTO is enabled
                continue
            # use only AUTO for container
            if container_name is not None:
                continue
            noAutoSite.append(tmpSiteName)
        return (okSite, noAutoSite)
