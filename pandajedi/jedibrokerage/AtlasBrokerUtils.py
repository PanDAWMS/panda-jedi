import datetime
import json
import os
import re
import socket
import sys
import time
import traceback

from dataservice.DataServiceUtils import select_scope
from pandacommon.pandautils.PandaUtils import naive_utcnow
from pandaserver.dataservice import DataServiceUtils
from pandaserver.taskbuffer import JobUtils, ProcessGroups

from pandajedi.jedicore import Interaction


# get nuclei where data is available
def getNucleiWithData(siteMapper, ddmIF, datasetName, candidateNuclei, deepScan=False):
    # get replicas
    try:
        replicaMap = ddmIF.listReplicasPerDataset(datasetName, deepScan)
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        return errtype, f"ddmIF.listReplicasPerDataset failed with {errvalue}"
    # loop over all clouds
    retMap = {}
    for tmpNucleus in candidateNuclei:
        tmpNucleusSpec = siteMapper.getNucleus(tmpNucleus)
        # loop over all datasets
        totalNum = 0
        totalSize = 0
        avaNumDisk = 0
        avaNumAny = 0
        avaSizeDisk = 0
        avaSizeAny = 0
        for tmpDataset, tmpRepMap in replicaMap.items():
            tmpTotalNum = 0
            tmpTotalSize = 0
            tmpAvaNumDisk = 0
            tmpAvaNumAny = 0
            tmpAvaSizeDisk = 0
            tmpAvaSizeAny = 0
            # loop over all endpoints
            for tmpLoc, locData in tmpRepMap.items():
                # get total
                if tmpTotalNum == 0:
                    tmpTotalNum = locData[0]["total"]
                    tmpTotalSize = locData[0]["tsize"]
                # check if the endpoint is associated
                if tmpNucleusSpec.is_associated_for_input(tmpLoc):
                    tmpEndpoint = tmpNucleusSpec.getEndpoint(tmpLoc)
                    tmpAvaNum = locData[0]["found"]
                    tmpAvaSize = locData[0]["asize"]
                    # disk
                    if tmpEndpoint["is_tape"] != "Y":
                        # complete replica is available at DISK
                        if tmpTotalNum == tmpAvaNum and tmpTotalNum > 0:
                            tmpAvaNumDisk = tmpAvaNum
                            tmpAvaNumAny = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                            tmpAvaSizeAny = tmpAvaSize
                            break
                        if tmpAvaNum > tmpAvaNumDisk:
                            tmpAvaNumDisk = tmpAvaNum
                            tmpAvaSizeDisk = tmpAvaSize
                    # tape
                    if tmpAvaNumAny < tmpAvaNum:
                        tmpAvaNumAny = tmpAvaNum
                        tmpAvaSizeAny = tmpAvaSize
            # total
            totalNum = tmpTotalNum
            totalSize = tmpTotalSize
            avaNumDisk += tmpAvaNumDisk
            avaNumAny += tmpAvaNumAny
            avaSizeDisk += tmpAvaSizeDisk
            avaSizeAny += tmpAvaSizeAny
        # append
        if tmpNucleus in candidateNuclei or avaNumAny > 0:
            retMap[tmpNucleus] = {
                "tot_num": totalNum,
                "tot_size": totalSize,
                "ava_num_disk": avaNumDisk,
                "ava_num_any": avaNumAny,
                "ava_size_disk": avaSizeDisk,
                "ava_size_any": avaSizeAny,
            }
    # return
    return Interaction.SC_SUCCEEDED, retMap


# get sites where data is available and check if complete replica is available at online RSE
def get_sites_with_data(siteList, siteMapper, ddmIF, datasetName, element_list, max_missing_input_files, min_input_completeness):
    # get replicas
    try:
        replicaMap = {}
        replicaMap[datasetName] = ddmIF.listDatasetReplicas(datasetName, use_vp=True, skip_incomplete_element=True, element_list=element_list)
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        return errtype, f"ddmIF.listDatasetReplicas failed with {errvalue}", None, None

    # check if complete replica is available at online RSE
    complete_disk = False
    complete_tape = False
    for tmp_rse, tmp_data_list in replicaMap[datasetName].items():
        # look for complete replicas
        for tmp_data in tmp_data_list:
            # blacklisted
            if tmp_data.get("read_blacklisted") is True:
                continue
            if not tmp_data.get("vp"):
                if tmp_data["found"] == tmp_data["total"]:
                    pass
                elif (
                    tmp_data["total"]
                    and tmp_data["found"]
                    and (
                        tmp_data["total"] - tmp_data["found"] <= max_missing_input_files
                        or tmp_data["found"] / tmp_data["total"] * 100 >= min_input_completeness
                    )
                ):
                    pass
                else:
                    continue
                if tmp_data.get("is_tape") == "Y":
                    complete_tape = True
                else:
                    complete_disk = True

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
            if tmpDDM == "":
                continue
            # get prefix
            # tmpPrefix = re.sub('_[^_]+$','_',tmpDDM)
            tmpPrefix = tmpDDM

            # already checked
            if tmpPrefix in checkedEndPoints:
                continue
            # DBR
            if DataServiceUtils.isCachedFile(datasetName, tmpSiteSpec):
                # no replica check since it is cached
                if tmpSiteName not in retMap:
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpDDM] = {"tape": False, "state": "complete"}
                checkedEndPoints.append(tmpPrefix)
                continue
            checkedEndPoints.append(tmpPrefix)
            tmpSePat = "^" + tmpPrefix
            for tmpSE in replicaMap[datasetName].keys():
                # check name with regexp pattern
                if re.search(tmpSePat, tmpSE) is None:
                    continue
                # skip staging
                if re.search("STAGING$", tmpSE) is not None:
                    continue
                # check archived metadata
                # FIXME
                pass
                # check tape attribute
                try:
                    tmpOnTape = ddmIF.getSiteProperty(tmpSE, "is_tape")
                except Exception:
                    continue
                    # errtype,errvalue = sys.exc_info()[:2]
                    # return errtype,'ddmIF.getSiteProperty for %s:tape failed with %s' % (tmpSE,errvalue)
                # check completeness
                tmpStatistics = replicaMap[datasetName][tmpSE][-1]
                if tmpStatistics["found"] is None:
                    tmpDatasetStatus = "unknown"
                    # refresh request
                    pass
                elif tmpStatistics["total"] == tmpStatistics["found"]:
                    tmpDatasetStatus = "complete"
                else:
                    tmpDatasetStatus = "incomplete"
                # append
                if tmpSiteName not in retMap:
                    retMap[tmpSiteName] = {}
                retMap[tmpSiteName][tmpSE] = {"tape": tmpOnTape, "state": tmpDatasetStatus}
                if "vp" in tmpStatistics:
                    retMap[tmpSiteName][tmpSE]["vp"] = tmpStatistics["vp"]
    # return
    return Interaction.SC_SUCCEEDED, retMap, complete_disk, complete_tape


# get analysis sites where data is available at disk
def getAnalSitesWithDataDisk(dataSiteMap, includeTape=False, use_vp=True, use_incomplete=False):
    siteList = []
    siteWithIncomp = []
    siteListNonVP = set()
    siteListVP = set()
    for tmpSiteName, tmpSeValMap in dataSiteMap.items():
        for tmpSE, tmpValMap in tmpSeValMap.items():
            # VP
            if tmpValMap.get("vp"):
                siteListVP.add(tmpSiteName)
                if not use_vp:
                    continue
            # on disk or tape
            if includeTape or not tmpValMap["tape"]:
                if tmpValMap["state"] == "complete":
                    # complete replica at disk
                    if tmpSiteName not in siteList:
                        siteList.append(tmpSiteName)
                    if not tmpValMap["tape"] and not tmpValMap.get("vp"):
                        siteListNonVP.add(tmpSiteName)
                else:
                    # incomplete replica at disk
                    if tmpSiteName not in siteWithIncomp:
                        siteWithIncomp.append(tmpSiteName)
    # remove VP if complete disk replica is unavailable
    if not siteListNonVP:
        for tmpSiteNameVP in siteListVP:
            if tmpSiteNameVP in siteList:
                siteList.remove(tmpSiteNameVP)
            if tmpSiteNameVP in siteWithIncomp:
                siteWithIncomp.remove(tmpSiteNameVP)
    # return sites with complete
    if siteList != [] or not use_incomplete:
        return siteList
    # return sites with incomplete if complete is unavailable
    return siteWithIncomp


# get the number of jobs in a status
def getNumJobs(jobStatMap, computingSite, jobStatus, cloud=None, workQueue_tag=None):
    if computingSite not in jobStatMap:
        return 0
    nJobs = 0
    # loop over all workQueues
    for tmpWorkQueue, tmpWorkQueueVal in jobStatMap[computingSite].items():
        # workQueue is defined
        if workQueue_tag is not None and workQueue_tag != tmpWorkQueue:
            continue
        # loop over all job status
        for tmpJobStatus, tmpCount in tmpWorkQueueVal.items():
            if tmpJobStatus == jobStatus:
                nJobs += tmpCount
    # return
    return nJobs


def get_total_nq_nr_ratio(job_stat_map, work_queue_tag=None):
    """
    Get the ratio of number of queued jobs to number of running jobs
    """
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
            for tmpJobStatus in ["defined", "assigned", "activated", "starting"]:
                if tmpJobStatus in tmpWorkQueueVal:
                    nQueue += tmpWorkQueueVal[tmpJobStatus]
            if "running" in tmpWorkQueueVal:
                nRunning += tmpWorkQueueVal["running"]
    try:
        ratio = float(nQueue) / float(nRunning)
    except Exception:
        ratio = None

    return ratio


def hasZeroShare(site_spec, task_spec, ignore_priority, tmp_log):
    """
    Check if the site has a zero share for the given task. Zero share means there is a policy preventing the site to be used for the task.

    :param site_spec: SiteSpec object describing the site being checked in brokerage
    :param task_spec: TaskSpec object describing the task being brokered
    :param ignore_priority: Merging job chunks will skip the priority check
    :param tmp_log: Logger object

    :return: False means there is no policy defined and the site can be used.
             True means the site has a fair share policy that prevents the site to be used for the task
    """

    # there is no per-site share defined (CRIC "fairsharePolicy" field), the site can be used
    if site_spec.fairsharePolicy in ["", None]:
        return False

    try:
        # get the group of processing types from a pre-defined mapping
        processing_group = ProcessGroups.getProcessGroup(task_spec.processingType)

        # don't suppress test tasks - the site can be used
        if processing_group in ["test"]:
            return False

        # loop over all policies
        for policy in site_spec.fairsharePolicy.split(","):
            # Examples of policies are:
            # type=evgen:100%,type=simul:100%,type=any:0%
            # type=evgen:100%,type=simul:100%,type=any:0%,group=(AP_Higgs|AP_Susy|AP_Exotics|Higgs):0%
            # gshare=Express:100%,gshare=any:0%
            # priority>400:0
            tmp_processing_type = None
            tmp_working_group = None
            tmp_priority = None
            tmp_gshare = None
            tmp_fair_share = policy.split(":")[-1]

            # break down each fair share policy into its fields
            for tmp_field in policy.split(":"):
                if tmp_field.startswith("type="):
                    tmp_processing_type = tmp_field.split("=")[-1]
                elif tmp_field.startswith("group="):
                    tmp_working_group = tmp_field.split("=")[-1]
                elif tmp_field.startswith("gshare="):
                    tmp_gshare = tmp_field.split("=")[-1]
                elif tmp_field.startswith("priority"):
                    tmp_priority = re.sub("priority", "", tmp_field)

            # check for a matching processing type
            if tmp_processing_type not in ["any", None]:
                if "*" in tmp_processing_type:
                    tmp_processing_type = tmp_processing_type.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's processing type,
                # so continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_processing_type}$", processing_group) is None:
                    continue

            # check for matching working group
            if tmp_working_group not in ["any", None] and task_spec.workingGroup is not None:
                if "*" in tmp_working_group:
                    tmp_working_group = tmp_working_group.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's working group
                # continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_working_group}$", task_spec.workingGroup) is None:
                    continue

            # check for matching gshare. Note that this only works for "leave gshares" in the fairsharePolicy,
            # i.e. the ones that have no sub-gshares, since the task only gets "leave gshares" assigned
            if tmp_gshare not in ["any", None] and task_spec.gshare is not None:
                if "*" in tmp_gshare:
                    tmp_gshare = tmp_gshare.replace("*", ".*")
                # if there is no match between the site's fair share policy and the task's gshare
                # continue looking for other policies that trigger the zero share condition
                if re.search(f"^{tmp_gshare}$", task_spec.gshare) is None:
                    continue

            # check priority
            if tmp_priority is not None and not ignore_priority:
                try:
                    exec(f"tmpStat = {task_spec.currentPriority}{tmp_priority}", globals())
                    tmp_log.debug(
                        f"Priority check for {site_spec.sitename}, {task_spec.currentPriority}): " f"{task_spec.currentPriority}{tmp_priority} = {tmpStat}"
                    )
                    if not tmpStat:
                        continue
                except Exception:
                    error_type, error_value = sys.exc_info()[:2]
                    tmp_log.error(f"Priority check for {site_spec.sitename} failed with {error_type}:{error_value}")

            # check fair share value
            # if 0, we need to skip the site
            # if different than 0, the site can be used
            if tmp_fair_share in ["0", "0%"]:
                return True
            else:
                return False

    except Exception:
        error_type, error_value = sys.exc_info()[:2]
        tmp_log.error(f"hasZeroShare failed with {error_type}:{error_value}")

    # if we reach this point, it means there is no policy preventing the site to be used
    return False


# check if site name is matched with one of list items
def isMatched(siteName, nameList):
    for tmpName in nameList:
        # ignore empty
        if tmpName == "":
            continue
        # wild card
        if "*" in tmpName:
            tmpName = tmpName.replace("*", ".*")
            if re.search(tmpName, siteName) is not None:
                return True
        else:
            # normal pattern
            if tmpName == siteName:
                return True
    # return
    return False


# get dict to set nucleus
def getDictToSetNucleus(nucleusSpec, tmpDatasetSpecs):
    # get destinations
    retMap = {"datasets": [], "nucleus": nucleusSpec.name}
    for datasetSpec in tmpDatasetSpecs:
        # skip distributed datasets
        if DataServiceUtils.getDistributedDestination(datasetSpec.storageToken) is not None:
            continue
        # get endpoint relevant to token
        endPoint = nucleusSpec.getAssociatedEndpoint(datasetSpec.storageToken)
        if endPoint is None and not nucleusSpec.is_nucleus() and nucleusSpec.get_default_endpoint_out():
            # use default endpoint for satellite that doesn't have relevant endpoint
            endPoint = nucleusSpec.get_default_endpoint_out()
        if endPoint is None:
            continue
        token = endPoint["ddm_endpoint_name"]
        # add original token
        if datasetSpec.storageToken not in ["", None]:
            token += f"/{datasetSpec.storageToken.split('/')[-1]}"
        retMap["datasets"].append({"datasetID": datasetSpec.datasetID, "token": f"dst:{token}", "destination": f"nucleus:{nucleusSpec.name}"})
    return retMap


# remove problematic sites
def skipProblematicSites(candidateSpecList, ngSites, sitesUsedByTask, preSetSiteSpec, maxNumSites, timeWindow, tmpLog):
    skippedSites = set()
    usedSitesGood = []
    newSitesGood = []
    # collect sites already used by the task
    for candidateSpec in candidateSpecList:
        # check if problematic
        if (candidateSpec.siteName in ngSites or candidateSpec.unifiedName in ngSites) and (
            preSetSiteSpec is None or candidateSpec.siteName != preSetSiteSpec.siteName
        ):
            skippedSites.add(candidateSpec.siteName)
        else:
            if candidateSpec.siteName in sitesUsedByTask or candidateSpec.unifiedName in sitesUsedByTask:
                usedSitesGood.append(candidateSpec)
            else:
                newSitesGood.append(candidateSpec)
    # set number of sites if undefined
    if maxNumSites in [0, None]:
        maxNumSites = len(candidateSpecList)
    newcandidateSpecList = usedSitesGood + newSitesGood
    newcandidateSpecList = newcandidateSpecList[:maxNumSites]
    # dump
    for skippedSite in skippedSites:
        tmpLog.debug(f"getting rid of problematic site {skippedSite}")
    return newcandidateSpecList


# get mapping between sites and input storage endpoints
def getSiteInputStorageEndpointMap(site_list, site_mapper, prod_source_label, job_label):
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

    return ret_map


# get to-running rate of sites from various resources
CACHE_SiteToRunRateStats = {}


def getSiteToRunRateStats(tbIF, vo, time_window=21600, cutoff=300, cache_lifetime=600):
    # initialize
    ret_val = False
    ret_map = {}
    # DB cache keys
    dc_main_key = "AtlasSites"
    dc_sub_key = "SiteToRunRate"
    # arguments for process lock
    this_prodsourcelabel = "user"
    this_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-broker"
    this_component = "Cache.SiteToRunRate"
    # timestamps
    current_time = naive_utcnow()
    starttime_max = current_time - datetime.timedelta(seconds=cutoff)
    starttime_min = current_time - datetime.timedelta(seconds=time_window)
    # rounded with 10 minutes
    starttime_max_rounded = starttime_max.replace(minute=starttime_max.minute // 10 * 10, second=0, microsecond=0)
    starttime_min_rounded = starttime_min.replace(minute=starttime_min.minute // 10 * 10, second=0, microsecond=0)
    real_interval_hours = (starttime_max_rounded - starttime_min_rounded).total_seconds() / 3600
    # local cache key
    local_cache_key = (starttime_min_rounded, starttime_max_rounded)
    # condition of query
    if local_cache_key in CACHE_SiteToRunRateStats and current_time <= CACHE_SiteToRunRateStats[local_cache_key]["exp"]:
        # query from local cache
        ret_val = True
        ret_map = CACHE_SiteToRunRateStats[local_cache_key]["data"]
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if naive_utcnow() - current_time > datetime.timedelta(seconds=min(10, cache_lifetime / 4)):
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
                        CACHE_SiteToRunRateStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(
                    vo=vo,
                    prodSourceLabel=this_prodsourcelabel,
                    cloud=None,
                    workqueue_id=None,
                    resource_name=None,
                    component=this_component,
                    pid=this_pid,
                    timeLimit=5,
                )
                if not got_lock:
                    # not getting lock, sleep and query cache again
                    time.sleep(1)
                    continue
                # query from PanDA DB directly
                ret_val, ret_map = tbIF.getSiteToRunRateStats(vo=vo, exclude_rwq=False, starttime_min=starttime_min, starttime_max=starttime_max)
                if ret_val:
                    # expired time
                    expired_time = current_time + datetime.timedelta(seconds=cache_lifetime)
                    # fill local cache
                    CACHE_SiteToRunRateStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                # unlock process
                tbIF.unlockProcess_JEDI(
                    vo=vo, prodSourceLabel=this_prodsourcelabel, cloud=None, workqueue_id=None, resource_name=None, component=this_component, pid=this_pid
                )
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = f"AtlasBrokerUtils.getSiteToRunRateStats got {e.__class__.__name__}: {e} \n"
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
                    err_str = f"AtlasBrokerUtils.getSiteToRunRateStats when deleting outdated entries got {e.__class__.__name__}: {e} \n"
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
    dc_main_key = "AtlasSites"
    dc_sub_key = "UsersJobsStats"
    # arguments for process lock
    this_prodsourcelabel = prod_source_label
    this_pid = this_pid = f"{socket.getfqdn().split('.')[0]}-{os.getpid()}_{os.getpgrp()}-broker"
    this_component = "Cache.UsersJobsStats"
    # local cache key; a must if not using global variable
    local_cache_key = "_main"
    # timestamps
    current_time = naive_utcnow()
    # condition of query
    if local_cache_key in CACHE_UsersJobsStats and current_time <= CACHE_UsersJobsStats[local_cache_key]["exp"]:
        # query from local cache
        ret_val = True
        ret_map = CACHE_UsersJobsStats[local_cache_key]["data"]
    else:
        # try some times
        for _ in range(99):
            # skip if too long after original current time
            if naive_utcnow() - current_time > datetime.timedelta(seconds=min(15, cache_lifetime / 4)):
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
                        CACHE_UsersJobsStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                        # break trying
                        break
                # got process lock
                got_lock = tbIF.lockProcess_JEDI(
                    vo=vo,
                    prodSourceLabel=this_prodsourcelabel,
                    cloud=None,
                    workqueue_id=None,
                    resource_name=None,
                    component=this_component,
                    pid=this_pid,
                    timeLimit=(cache_lifetime * 0.75 / 60),
                )
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
                    CACHE_UsersJobsStats[local_cache_key] = {"exp": expired_time, "data": ret_map}
                    # json of data
                    data_json = json.dumps(ret_map)
                    # fill DB cache
                    tbIF.updateCache_JEDI(main_key=dc_main_key, sub_key=dc_sub_key, data=data_json)
                    # make True return
                    ret_val = True
                # unlock process
                tbIF.unlockProcess_JEDI(
                    vo=vo, prodSourceLabel=this_prodsourcelabel, cloud=None, workqueue_id=None, resource_name=None, component=this_component, pid=this_pid
                )
                # break trying
                break
            except Exception as e:
                # dump error message
                err_str = f"AtlasBrokerUtils.getUsersJobsStats got {e.__class__.__name__}: {e} \n"
                sys.stderr.write(err_str)
                # break trying
                break
    # return
    return ret_val, ret_map


# get gshare usage
def getGShareUsage(tbIF, gshare, fresher_than_minutes_ago=15):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_gshare = (
                """SELECT m.value_json """
                """FROM ATLAS_PANDA.Metrics m """
                """WHERE m.metric=:metric """
                """AND m.gshare=:gshare """
                """AND m.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "gshare_preference",
                ":gshare": gshare,
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_gshare, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json)
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getGShareUsage got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get user evaluation
def getUserEval(tbIF, user, fresher_than_minutes_ago=20):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_user_eval = f'SELECT m.value_json."{user}" FROM ATLAS_PANDA.Metrics m WHERE m.metric=:metric AND m.timestamp>=:min_timestamp '
            # varMap
            varMap = {
                ":metric": "analy_user_eval",
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_user_eval, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json) if value_json else None
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getUserEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get user task evaluation
def getUserTaskEval(tbIF, taskID, fresher_than_minutes_ago=15):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_task_eval = (
                """SELECT tev.value_json """
                """FROM ATLAS_PANDA.Task_Evaluation tev """
                """WHERE tev.metric=:metric """
                """AND tev.jediTaskID=:taskID """
                """AND tev.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "analy_task_eval",
                ":taskID": taskID,
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_task_eval, varMap)
            if res:
                value_json = res[0][0]
                # json of data
                ret_map = json.loads(value_json) if value_json else None
                # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getUserTaskEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# get analysis sites class
def getAnalySitesClass(tbIF, fresher_than_minutes_ago=60):
    # initialize
    ret_val = False
    ret_map = {}
    # timestamps
    current_time = naive_utcnow()
    # try some times
    for _ in range(99):
        now_time = naive_utcnow()
        # skip if too long after original current time
        if now_time - current_time > datetime.timedelta(seconds=max(3, fresher_than_minutes_ago / 3)):
            # break trying
            break
        try:
            # query from PanDA DB directly
            sql_get_task_eval = (
                """SELECT m.computingSite, m.value_json.class """
                """FROM ATLAS_PANDA.Metrics m """
                """WHERE m.metric=:metric """
                """AND m.timestamp>=:min_timestamp """
            )
            # varMap
            varMap = {
                ":metric": "analy_site_eval",
                ":min_timestamp": now_time - datetime.timedelta(minutes=fresher_than_minutes_ago),
            }
            # result
            res = tbIF.querySQL(sql_get_task_eval, varMap)
            if res:
                for site, class_value in res:
                    ret_map[site] = int(class_value)
                    # make True return
                ret_val = True
            # break trying
            break
        except Exception as e:
            # dump error message
            err_str = f"AtlasBrokerUtils.getAnalySitesEval got {e.__class__.__name__}: {e} \n"
            sys.stderr.write(err_str)
            # break trying
            break
    # return
    return ret_val, ret_map


# check SW with json
class JsonSoftwareCheck:
    # constructor
    def __init__(self, site_mapper, sw_map):
        self.siteMapper = site_mapper
        self.sw_map = sw_map

    # get lists
    def check(
        self,
        site_list,
        cvmfs_tag,
        sw_project,
        sw_version,
        cmt_config,
        need_cvmfs,
        cmt_config_only,
        need_container=False,
        container_name=None,
        only_tags_fc=False,
        host_cpu_specs=None,
        host_gpu_spec=None,
        log_stream=None,
    ):
        okSite = []
        noAutoSite = []
        for tmpSiteName in site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.releases == ["AUTO"] and tmpSiteName in self.sw_map:
                try:
                    go_ahead = False
                    # convert to a dict
                    architecture_map = {}
                    if "architectures" in self.sw_map[tmpSiteName]:
                        for arch_spec in self.sw_map[tmpSiteName]["architectures"]:
                            if "type" in arch_spec:
                                architecture_map[arch_spec["type"]] = arch_spec
                    # check if need CPU
                    if "cpu" in architecture_map:
                        need_cpu = False
                        for k in architecture_map["cpu"]:
                            if isinstance(architecture_map["cpu"][k], list):
                                if "excl" in architecture_map["cpu"][k]:
                                    need_cpu = True
                                    break
                        if need_cpu and host_cpu_specs is None:
                            continue
                    # check if need GPU
                    if "gpu" in architecture_map:
                        need_gpu = False
                        for k in architecture_map["gpu"]:
                            if isinstance(architecture_map["gpu"][k], list):
                                if "excl" in architecture_map["gpu"][k]:
                                    need_gpu = True
                                    break
                        if need_gpu and host_gpu_spec is None:
                            continue
                    if host_cpu_specs or host_gpu_spec:
                        # skip since the PQ doesn't describe HW spec
                        if not architecture_map:
                            continue
                        # check CPU
                        if host_cpu_specs:
                            host_ok = False
                            for host_cpu_spec in host_cpu_specs:
                                # CPU not specified
                                if "cpu" not in architecture_map:
                                    continue
                                # check architecture
                                if host_cpu_spec["arch"] == "*":
                                    if "excl" in architecture_map["cpu"]["arch"]:
                                        continue
                                else:
                                    if "any" not in architecture_map["cpu"]["arch"]:
                                        if host_cpu_spec["arch"] not in architecture_map["cpu"]["arch"]:
                                            # check with regex
                                            if not [True for iii in architecture_map["cpu"]["arch"] if re.search("^" + host_cpu_spec["arch"] + "$", iii)]:
                                                continue
                                # check vendor
                                if host_cpu_spec["vendor"] == "*":
                                    # task doesn't specify a vendor and PQ explicitly requests a specific vendor
                                    if "vendor" in architecture_map["cpu"] and "excl" in architecture_map["cpu"]["vendor"]:
                                        continue
                                else:
                                    # task specifies a vendor and PQ doesn't request any specific vendor
                                    if "vendor" not in architecture_map["cpu"]:
                                        continue
                                    # task specifies a vendor and PQ doesn't accept any vendor or the specific vendor
                                    if "any" not in architecture_map["cpu"]["vendor"] and host_cpu_spec["vendor"] not in architecture_map["cpu"]["vendor"]:
                                        continue
                                # check instruction set
                                if host_cpu_spec["instr"] == "*":
                                    if "instr" in architecture_map["cpu"] and "excl" in architecture_map["cpu"]["instr"]:
                                        continue
                                else:
                                    if "instr" not in architecture_map["cpu"]:
                                        continue
                                    if "any" not in architecture_map["cpu"]["instr"] and host_cpu_spec["instr"] not in architecture_map["cpu"]["instr"]:
                                        continue
                                host_ok = True
                                break
                            if not host_ok:
                                continue
                        # check GPU
                        if host_gpu_spec:
                            # GPU not specified
                            if "gpu" not in architecture_map:
                                continue
                            # check vendor
                            if host_gpu_spec["vendor"] == "*":
                                # task doesn't specify CPU vendor and PQ explicitly requests a specific vendor
                                if "vendor" in architecture_map["gpu"] and "excl" in architecture_map["gpu"]["vendor"]:
                                    continue
                            else:
                                # task specifies a vendor and PQ doesn't request any specific vendor
                                if "vendor" not in architecture_map["gpu"]:
                                    continue
                                # task specifies a vendor and PQ doesn't accept any vendor or the specific vendor
                                if "any" not in architecture_map["gpu"]["vendor"] and host_gpu_spec["vendor"] not in architecture_map["gpu"]["vendor"]:
                                    continue
                            # check model
                            if host_gpu_spec["model"] == "*":
                                if "model" in architecture_map["gpu"] and "excl" in architecture_map["gpu"]["model"]:
                                    continue
                            else:
                                if "model" not in architecture_map["gpu"] or (
                                    "any" not in architecture_map["gpu"]["model"] and host_gpu_spec["model"] not in architecture_map["gpu"]["model"]
                                ):
                                    continue
                    go_ahead = True
                except Exception as e:
                    if log_stream:
                        log_stream.error(f"json check {str(architecture_map)} failed for {tmpSiteName} {str(e)} {traceback.format_exc()} ")
                if not go_ahead:
                    continue
                # only HW check
                if not (cvmfs_tag or cmt_config or sw_project or sw_version or container_name) and (host_cpu_specs or host_gpu_spec):
                    okSite.append(tmpSiteName)
                    continue
                # check for fat container
                if container_name:
                    # check for container
                    if not only_tags_fc and ("any" in self.sw_map[tmpSiteName]["containers"] or "/cvmfs" in self.sw_map[tmpSiteName]["containers"]):
                        # any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([t["container_name"] for t in self.sw_map[tmpSiteName]["tags"] if t["container_name"]]):
                        # logical name in tags or any in containers
                        okSite.append(tmpSiteName)
                    elif container_name in set([s for t in self.sw_map[tmpSiteName]["tags"] for s in t["sources"] if t["sources"]]):
                        # full path in sources
                        okSite.append(tmpSiteName)
                    elif not only_tags_fc:
                        # get sources in all tag list
                        if "ALL" in self.sw_map:
                            source_list_in_all_tag = [s for t in self.sw_map["ALL"]["tags"] for s in t["sources"] if t["container_name"] == container_name]
                        else:
                            source_list_in_all_tag = []
                        # prefix with full path
                        for tmp_prefix in self.sw_map[tmpSiteName]["containers"]:
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
                    if not cmt_config or cmt_config in self.sw_map[tmpSiteName]["cmtconfigs"]:
                        okSite.append(tmpSiteName)
                    continue
                # check if CVMFS is available
                if "any" in self.sw_map[tmpSiteName]["cvmfs"] or cvmfs_tag in self.sw_map[tmpSiteName]["cvmfs"]:
                    # check if container is available
                    if "any" in self.sw_map[tmpSiteName]["containers"] or "/cvmfs" in self.sw_map[tmpSiteName]["containers"]:
                        okSite.append(tmpSiteName)
                    # check cmt config
                    elif not need_container and cmt_config in self.sw_map[tmpSiteName]["cmtconfigs"]:
                        okSite.append(tmpSiteName)
                elif not need_cvmfs:
                    if not need_container or "any" in self.sw_map[tmpSiteName]["containers"]:
                        # check tags
                        for tag in self.sw_map[tmpSiteName]["tags"]:
                            if tag["cmtconfig"] == cmt_config and tag["project"] == sw_project and tag["release"] == sw_version:
                                okSite.append(tmpSiteName)
                                break
                # don't pass to subsequent check if AUTO is enabled
                continue
            # use only AUTO for container or HW
            if container_name is not None or host_cpu_specs is not None or host_gpu_spec is not None:
                continue
            noAutoSite.append(tmpSiteName)
        return (okSite, noAutoSite)


# resolve cmt_config
def resolve_cmt_config(queue_name: str, cmt_config: str, base_platform, sw_map: dict) -> str | None:
    """
    resolve cmt config at a given queue_name
    :param queue_name: queue name
    :param cmt_config: cmt confing to resolve
    :param base_platform: base platform
    :param sw_map: software map
    :return: resolved cmt config or None if unavailable or valid
    """
    # return None if queue_name is unavailable
    if queue_name not in sw_map:
        return None
    # return None if cmt_config is valid
    if cmt_config in sw_map[queue_name]["cmtconfigs"]:
        return None
    # check if cmt_config matches with any of the queue's cmt_configs
    for tmp_cmt_config in sw_map[queue_name]["cmtconfigs"]:
        if re.search("^" + cmt_config + "$", tmp_cmt_config):
            if base_platform:
                # add base_platform if necessary
                tmp_cmt_config = tmp_cmt_config + "@" + base_platform
            return tmp_cmt_config
    # return None if cmt_config is unavailable
    return None
