import os
import sys
import copy
import re
import json
import socket
import datetime
import traceback

from six import iteritems

from .WatchDogBase import WatchDogBase
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock
from pandajedi.jedicore import Interaction
from pandajedi.jedibrokerage import AtlasBrokerUtils

from pandaserver.dataservice import DataServiceUtils
# from pandaserver.dataservice.Activator import Activator
from pandaserver.taskbuffer import JobUtils


# logger
from pandacommon.pandalogger.PandaLogger import PandaLogger
logger = PandaLogger().getLogger(__name__.split('.')[-1])

# dry run or not
DRY_RUN = False

# boosted task priority when preassigned
magic_priority = 1023

# queue filler watchdog for ATLAS
class AtlasQueueFillerWatchDog(WatchDogBase):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        # self.cronActions = {'forPrestage': 'atlas_prs'}
        self.vo = 'atlas'
        self.prodSourceLabelList = ['managed']
        # keys for cache
        self.dc_main_key = 'AtlasQueueFillerWatchDog'
        self.dc_sub_key_pt = 'PreassignedTasks'
        self.dc_sub_key_bt = 'BlacklistedTasks'
        self.dc_sub_key_prio = 'OriginalTaskPriority'
        # call refresh
        self.refresh()

    # refresh information stored in the instance
    def refresh(self):
        # work queue mapper
        self.workQueueMapper = self.taskBufferIF.getWorkQueueMap()
        # site mapper
        self.siteMapper = self.taskBufferIF.getSiteMapper()
        # all sites
        allSiteList = []
        for siteName, tmpSiteSpec in iteritems(self.siteMapper.siteSpecList):
            # if tmpSiteSpec.type == 'analysis' or tmpSiteSpec.is_grandly_unified():
            allSiteList.append(siteName)
        self.allSiteList = allSiteList

    # update preassigned task map to cache
    def _update_to_pt_cache(self, ptmap):
        data_json = json.dumps(ptmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_pt, data=data_json)

    # get preassigned task map from cache
    def _get_from_pt_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_pt)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # update blacklisted task map to cache
    def _update_to_bt_cache(self, btmap):
        data_json = json.dumps(btmap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_bt, data=data_json)

    # get blacklisted task map from cache
    def _get_from_bt_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_bt)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # update task prioirty map to cache
    def _update_to_prio_cache(self, priomap):
        data_json = json.dumps(priomap)
        self.taskBufferIF.updateCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_prio, data=data_json)

    # get task prioirty map from cache
    def _get_from_prio_cache(self):
        cache_spec = self.taskBufferIF.getCache_JEDI(main_key=self.dc_main_key, sub_key=self.dc_sub_key_prio)
        if cache_spec is not None:
            ret_map = json.loads(cache_spec.data)
            return ret_map
        else:
            return dict()

    # get process lock to preassign
    def _get_lock(self):
        return self.taskBufferIF.lockProcess_JEDI(
                vo=self.vo, prodSourceLabel='managed',
                cloud=None, workqueue_id=None, resource_name=None,
                component='AtlasQueueFillerWatchDog.preassign',
                pid=self.pid, timeLimit=5)

    # release lock
    def _release_lock(self):
        return self.taskBufferIF.unlockProcess_JEDI(
                vo=self.vo, prodSourceLabel='managed',
                cloud=None, workqueue_id=None, resource_name=None,
                component='AtlasQueueFillerWatchDog.preassign',
                pid=self.pid)

    # get map of site to list of RSEs
    def get_site_rse_map(self, prod_source_label):
        site_rse_map = {}
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            scope_input, scope_output = DataServiceUtils.select_scope(tmpSiteSpec, prod_source_label, prod_source_label)
            try:
                endpoint_token_map = tmpSiteSpec.ddm_endpoints_input[scope_input].getTokenMap('input')
            except KeyError:
                continue
            else:
                # fill
                site_rse_map[tmpSiteName] = list(endpoint_token_map.values())
        # return
        return site_rse_map

    # get to-running rate of sites between 24 hours ago ~ 6 hours ago
    def get_site_ttr_map(self):
        ret_val, ret_map = AtlasBrokerUtils.getSiteToRunRateStats(
                            self.taskBufferIF, self.vo, time_window=86400, cutoff=21600, cache_lifetime=600)
        if ret_val:
            return ret_map
        else:
            return None

    # get available sites
    def get_available_sites(self):
        available_sites_dict = {}
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return available_sites_dict
        # get to-running rate of sites
        site_ttr_map = self.get_site_ttr_map()
        if site_ttr_map is None:
            return available_sites_dict
        # loop over sites
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # skip site already added
            if tmpSiteName in available_sites_dict:
                continue
            # skip site is not online
            if tmpSiteSpec.status not in ('online'):
                continue
            # skip site if not for production
            if not tmpSiteSpec.runs_production():
                continue
            # skip if site has memory limitations
            if tmpSiteSpec.minrss not in (0, None):
                continue
            # skip if site has not enough activity in the past 24 hours
            site_ttr = site_ttr_map.get(tmpSiteName)
            if site_ttr is None or site_ttr < 0.8:
                continue
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running')
            nQueue = 0
            for jobStatus in ['activated', 'starting']:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus)
            # available sites: must be idle now
            if nQueue < max(20, nRunning*2)*0.25:
                available_sites_dict[tmpSiteName] = tmpSiteSpec
        # return
        return available_sites_dict

    # get busy sites
    def get_busy_sites(self):
        busy_sites_dict = {}
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return busy_sites_dict
        # get to-running rate of sites
        site_ttr_map = self.get_site_ttr_map()
        if site_ttr_map is None:
            return busy_sites_dict
        # loop over sites
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # skip site already added
            if tmpSiteName in busy_sites_dict:
                continue
            # initialize
            is_busy = False
            # site is not online viewed as busy
            if tmpSiteSpec.status not in ('online'):
                is_busy = True
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running')
            nQueue = 0
            for jobStatus in ['activated', 'starting']:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus)
            # busy sites
            if nQueue > max(20, nRunning*2)*0.375:
                busy_sites_dict[tmpSiteName] = tmpSiteSpec
        # return
        return busy_sites_dict

    # preassign tasks to site
    def do_preassign(self):
        tmp_log = MsgWrapper(logger, 'do_preassign')
        # refresh
        self.refresh()
        # list of resource type
        resource_type_list = [ rt.resource_name for rt in self.taskBufferIF.load_resource_types() ]
        # loop
        for prod_source_label in self.prodSourceLabelList:
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label)
            # parameters from GDP config
            max_preassigned_tasks = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MAX_PREASSIGNED_TASKS_{0}'.format(prod_source_label), 'jedi', self.vo)
            if max_preassigned_tasks is None:
                max_preassigned_tasks = 3
            min_files_ready = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MIN_FILES_READY_{0}'.format(prod_source_label), 'jedi', self.vo)
            if min_files_ready is None:
                min_files_ready = 50
            min_files_remaining = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MIN_FILES_REMAINING_{0}'.format(prod_source_label), 'jedi', self.vo)
            if min_files_remaining is None:
                min_files_remaining = 100
            # available sites
            available_sites_dict = self.get_available_sites()
            # get blacklisted_tasks_map from cache
            blacklisted_tasks_map = self._get_from_bt_cache()
            blacklisted_tasks_set = set()
            for bt_list in blacklisted_tasks_map.values():
                blacklisted_tasks_set |= set(bt_list)
            # loop over available sites to preassign
            for site, tmpSiteSpec in available_sites_dict.items():
                # rses of the available site
                available_rses = set()
                try:
                    available_rses.update(set(site_rse_map[site]))
                except KeyError:
                    continue
                # do not consider TAPE rses
                for rse in set(available_rses):
                    if 'TAPE' in str(rse):
                        available_rses.remove(rse)
                # skip if no rse for available site
                if not available_rses:
                    continue
                # skip if no coreCount set
                if not tmpSiteSpec.coreCount or not tmpSiteSpec.coreCount > 0:
                    continue
                # site attributes
                site_maxrss =  tmpSiteSpec.maxrss if tmpSiteSpec.maxrss not in (0, None) else 999999
                site_corecount = tmpSiteSpec.coreCount
                site_capability = str(tmpSiteSpec.capability).lower()
                # make sql parameters of rses
                available_rses = list(available_rses)
                rse_params_list = []
                rse_params_map = {}
                for j, rse in enumerate(available_rses):
                    rse_param = ':rse_{0}'.format(j + 1)
                    rse_params_list.append(rse_param)
                    rse_params_map[rse_param] = rse
                rse_params_str = ','.join(rse_params_list)
                # only simul tasks if site has fairsharePolicy setup
                processing_type_constraint = ''
                if tmpSiteSpec.fairsharePolicy not in ('NULL', None):
                    processing_type_constraint = "AND t.processingType='simul' "
                # sql
                sql_query = (
                    "SELECT t.jediTaskID, t.currentPriority "
                    "FROM {jedi_schema}.JEDI_Tasks t "
                    "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                        "AND t.prodSourceLabel=:prodSourceLabel "
                        "AND t.resource_type=:resource_type "
                        "AND site IS NULL "
                        "AND (COALESCE(t.baseRamCount, 0) + (CASE WHEN t.ramUnit IN ('MBPerCore','MBPerCoreFixed') THEN t.ramCount*:site_corecount ELSE t.ramCount END))*0.95 < :site_maxrss "
                        "AND t.eventService=0 "
                        "AND EXISTS ( "
                            "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                            "WHERE dl.jediTaskID=t.jediTaskID "
                                "AND dl.rse IN ({rse_params_str}) "
                            ") "
                        "{processing_type_constraint} "
                        "AND EXISTS ( "
                            "SELECT d.datasetID FROM {jedi_schema}.JEDI_Datasets d "
                            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                                "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFilesToBeUsed>=:min_files_remaining "
                            ") "
                        "AND t.currentPriority<:magic_priority "
                        "AND t.container_name IS NULL "
                    "ORDER BY t.currentPriority DESC "
                    "FOR UPDATE "
                ).format(jedi_schema=jedi_config.db.schemaJEDI,
                            rse_params_str=rse_params_str,
                            processing_type_constraint=processing_type_constraint)
                # loop over resource type
                for resource_type in resource_type_list:
                    # key name for preassigned_tasks_map = site + resource_type
                    key_name = '{0}|{1}'.format(site, resource_type)
                    # skip if not match with site capability
                    if site_capability == 'score' and not resource_type.startswith('SCORE'):
                        continue
                    elif site_capability == 'mcore' and not resource_type.startswith('MCORE'):
                        continue
                    # params map
                    params_map = {
                            ':prodSourceLabel': prod_source_label,
                            ':resource_type': resource_type,
                            ':site_maxrss': site_maxrss,
                            ':site_corecount': site_corecount,
                            ':min_files_ready': min_files_ready,
                            ':min_files_remaining': min_files_remaining,
                            ':magic_priority': magic_priority,
                        }
                    params_map.update(rse_params_map)
                    # get preassigned_tasks_map from cache
                    preassigned_tasks_map = self._get_from_pt_cache()
                    preassigned_tasks_cached = preassigned_tasks_map.get(key_name, [])
                    # get task_orig_priority_map from cache
                    task_orig_priority_map = self._get_from_prio_cache()
                    # number of tasks already preassigned
                    n_preassigned_tasks = len(preassigned_tasks_cached)
                    # nuber of tasks to preassign
                    n_tasks_to_preassign = max(max_preassigned_tasks - n_preassigned_tasks, 0)
                    # preassign
                    if n_tasks_to_preassign <= 0:
                        tmp_log.debug('{key_name:<64} already has enough preassigned tasks ({n_tasks:>3}) ; skipped '.format(
                                        key_name=key_name, n_tasks=n_preassigned_tasks))
                    elif DRY_RUN:
                        dry_sql_query = (
                            "SELECT t.jediTaskID, t.currentPriority "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                                "AND t.prodSourceLabel=:prodSourceLabel "
                                "AND t.resource_type=:resource_type "
                                "AND site IS NULL "
                                "AND (COALESCE(t.baseRamCount, 0) + (CASE WHEN t.ramUnit IN ('MBPerCore','MBPerCoreFixed') THEN t.ramCount*:site_corecount ELSE t.ramCount END))*0.95 < :site_maxrss "
                                "AND t.eventService=0 "
                                "AND EXISTS ( "
                                    "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                                    "WHERE dl.jediTaskID=t.jediTaskID "
                                        "AND dl.rse IN ({rse_params_str}) "
                                    ") "
                                "{processing_type_constraint} "
                                "AND EXISTS ( "
                                    "SELECT d.datasetID FROM {jedi_schema}.JEDI_Datasets d "
                                    "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                                        "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFilesToBeUsed>=:min_files_remaining "
                                    ") "
                                "AND t.currentPriority<:magic_priority "
                                "AND t.container_name IS NULL "
                            "ORDER BY t.currentPriority DESC "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI,
                                    rse_params_str=rse_params_str,
                                    processing_type_constraint=processing_type_constraint)
                        # tmp_log.debug('[dry run] {} {}'.format(dry_sql_query, params_map))
                        res = self.taskBufferIF.querySQL(dry_sql_query, params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            result = [ x[0] for x in res if x[0] not in preassigned_tasks_cached ]
                            updated_tasks = result[:n_tasks_to_preassign]
                            tmp_log.debug('[dry run] {key_name:<64} {n_tasks:>3} tasks would be preassigned '.format(
                                            key_name=key_name, n_tasks=n_tasks_to_preassign))
                            # update preassigned_tasks_map into cache
                            preassigned_tasks_map[key_name] = list(set(updated_tasks) | set(preassigned_tasks_cached))
                            tmp_log.debug('{} ; {}'.format(str(updated_tasks), str(preassigned_tasks_map[key_name])))
                            self._update_to_pt_cache(preassigned_tasks_map)
                    else:
                        updated_tasks_prio = self.taskBufferIF.queryTasksToPreassign_JEDI(sql_query, params_map, site,
                                                                                        blacklist=blacklisted_tasks_set,
                                                                                        priority=magic_priority,
                                                                                        limit=n_tasks_to_preassign)
                        if updated_tasks_prio is None:
                            # dbproxy method failed
                            tmp_log.error('{key_name:<64} failed to preassign tasks '.format(
                                            key_name=key_name))
                        else:
                            n_tasks = len(updated_tasks_prio)
                            if n_tasks > 0:
                                updated_tasks = [ x[0] for x in updated_tasks_prio ]
                                tmp_log.info('{key_name:<64} {n_tasks:>3} tasks preassigned : {updated_tasks}'.format(
                                                key_name=key_name, n_tasks=str(n_tasks), updated_tasks=updated_tasks))
                                # update preassigned_tasks_map into cache
                                preassigned_tasks_map[key_name] = list(set(updated_tasks) | set(preassigned_tasks_cached))
                                self._update_to_pt_cache(preassigned_tasks_map)
                                # update task_orig_priority_map into cache
                                for taskid, orig_priority in updated_tasks_prio:
                                    taskid_str = str(taskid)
                                    task_orig_priority_map[taskid_str] = orig_priority
                                self._update_to_prio_cache(task_orig_priority_map)
                                # Kibana log
                                for taskid in updated_tasks:
                                    tmp_log.debug('#ATM #KV jediTaskID={taskid} action=do_preassign site={site} rtype={rtype} preassigned '.format(
                                                    taskid=taskid, site=site, rtype=resource_type))
        # total preassigned tasks
        n_pt_tot = sum([ len(pt_list) for pt_list in preassigned_tasks_map.values() ])
        tmp_log.debug('now {n_pt_tot} tasks preassigned in total'.format(n_pt_tot=n_pt_tot))

    # undo preassign tasks
    def undo_preassign(self):
        tmp_log = MsgWrapper(logger, 'undo_preassign')
        # refresh
        self.refresh()
        # busy sites
        busy_sites_dict = self.get_busy_sites()
        # loop to undo preassignment
        for prod_source_label in self.prodSourceLabelList:
            # parameter from GDP config
            max_preassigned_tasks = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MAX_PREASSIGNED_TASKS_{0}'.format(prod_source_label), 'jedi', self.vo)
            if max_preassigned_tasks is None:
                max_preassigned_tasks = 3
            min_files_ready = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MIN_FILES_READY_{0}'.format(prod_source_label), 'jedi', self.vo)
            if min_files_ready is None:
                min_files_ready = 50
            min_files_remaining = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MIN_FILES_REMAINING_{0}'.format(prod_source_label), 'jedi', self.vo)
            if min_files_remaining is None:
                min_files_remaining = 100
            # clean up outdated blacklist
            blacklist_duration_hours = 12
            blacklisted_tasks_map_orig = self._get_from_bt_cache()
            blacklisted_tasks_map = copy.deepcopy(blacklisted_tasks_map_orig)
            now_time = datetime.datetime.utcnow()
            min_allowed_time = now_time - datetime.timedelta(hours=blacklist_duration_hours)
            min_allowed_ts = int(min_allowed_time.timestamp())
            for ts_str in blacklisted_tasks_map_orig:
                ts = int(ts_str)
                if ts < min_allowed_ts:
                    del blacklisted_tasks_map[ts_str]
            self._update_to_bt_cache(blacklisted_tasks_map)
            n_bt_old = sum([ len(bt_list) for bt_list in blacklisted_tasks_map_orig.values() ])
            n_bt = sum([ len(bt_list) for bt_list in blacklisted_tasks_map.values() ])
            tmp_log.debug('done cleanup blacklist; before {n_bt_old} , now {n_bt} tasks in blacklist'.format(n_bt_old=n_bt_old, n_bt=n_bt))
            # get a copy of preassigned_tasks_map from cache
            preassigned_tasks_map_orig = self._get_from_pt_cache()
            preassigned_tasks_map = copy.deepcopy(preassigned_tasks_map_orig)
            # clean up task_orig_priority_map in cache
            task_orig_priority_map_orig = self._get_from_prio_cache()
            task_orig_priority_map = copy.deepcopy(task_orig_priority_map_orig)
            all_preassiged_taskids = set()
            for taskid_list in preassigned_tasks_map_orig.values():
                all_preassiged_taskids |= set(taskid_list)
            for taskid_str in task_orig_priority_map_orig:
                taskid = int(taskid_str)
                if taskid not in all_preassiged_taskids:
                    del task_orig_priority_map[taskid_str]
            self._update_to_prio_cache(task_orig_priority_map)
            # loop on preassigned tasks in cache
            for key_name in preassigned_tasks_map_orig:
                # parse key name = site + resource_type
                site, resource_type = key_name.split('|')
                # preassigned tasks in cache
                preassigned_tasks_cached = preassigned_tasks_map.get(key_name, [])
                # force_undo=True for all tasks in busy sites, and force_undo=False for tasks not in status to generate jobs
                force_undo = False
                if site in busy_sites_dict or len(preassigned_tasks_cached) > max_preassigned_tasks:
                    force_undo = True
                reason_str = 'site busy or offline or with too many preassigned tasks' if force_undo \
                                else 'task paused/terminated or without enough files to process'
                # parameters for undo, kinda ugly
                params_map = {
                        ":magic_priority": magic_priority,
                        ':min_files_ready': min_files_ready,
                        ':min_files_remaining': min_files_remaining,
                    }
                # undo preassign
                had_undo = False
                updated_tasks = []
                if DRY_RUN:
                    if force_undo:
                        updated_tasks = list(preassigned_tasks_cached)
                        n_tasks = len(updated_tasks)
                    else:
                        preassigned_tasks_list = []
                        preassigned_tasks_params_map = {}
                        for j, taskid in enumerate(preassigned_tasks_cached):
                            pt_param = ':pt_{0}'.format(j + 1)
                            preassigned_tasks_list.append(pt_param)
                            preassigned_tasks_params_map[pt_param] = taskid
                        if not preassigned_tasks_list:
                            continue
                        preassigned_tasks_params_str = ','.join(preassigned_tasks_list)
                        dry_sql_query = (
                            "SELECT t.jediTaskID "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.jediTaskID IN ({preassigned_tasks_params_str}) "
                                "AND t.site IS NOT NULL "
                                "AND NOT ( "
                                        "t.status IN ('ready','running','scouting') "
                                        "AND EXISTS ( "
                                            "SELECT d.datasetID FROM {0}.JEDI_Datasets d "
                                            "WHERE t.jediTaskID=d.jediTaskID AND d.type='input' "
                                                "AND d.nFilesToBeUsed-d.nFilesUsed>=:min_files_ready AND d.nFilesToBeUsed>=:min_files_remaining "
                                            ") "
                                    ") "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI, preassigned_tasks_params_str=preassigned_tasks_params_str)
                        res = self.taskBufferIF.querySQL(dry_sql_query, preassigned_tasks_params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            updated_tasks = [ x[0] for x in res ]
                    # tmp_log.debug('[dry run] {} {} force={}'.format(key_name, str(updated_tasks), force_undo))
                    had_undo = True
                    if n_tasks > 0:
                        tmp_log.debug('[dry run] {key_name:<64} {n_tasks:>3} preassigned tasks would be undone ({reason_str}) '.format(
                                        key_name=key_name, n_tasks=n_tasks, reason_str=reason_str))
                else:
                    updated_tasks = self.taskBufferIF.undoPreassignedTasks_JEDI(preassigned_tasks_cached,
                                                                                task_orig_priority_map=task_orig_priority_map,
                                                                                params_map=params_map,
                                                                                force=force_undo)
                    if updated_tasks is None:
                        # dbproxy method failed
                        tmp_log.error('{key_name:<64} failed to undo preassigned tasks (force={force_undo})'.format(
                                        key_name=key_name, force_undo=force_undo))
                    else:
                        had_undo = True
                        n_tasks = len(updated_tasks)
                        if n_tasks > 0:
                            tmp_log.info('{key_name:<64} {n_tasks:>3} preassigned tasks undone ({reason_str}) : {updated_tasks} '.format(
                                            key_name=key_name, n_tasks=str(n_tasks), reason_str=reason_str, updated_tasks=updated_tasks))
                            # Kibana log
                            for taskid in updated_tasks:
                                tmp_log.debug('#ATM #KV jediTaskID={taskid} action=undo_preassign site={site} rtype={rtype} un-preassinged since {reason_str}'.format(
                                                taskid=taskid, site=site, rtype=resource_type, reason_str=reason_str))
                # update preassigned_tasks_map into cache
                if had_undo:
                    if force_undo:
                        del preassigned_tasks_map[key_name]
                    else:
                        tmp_tasks_set = set(preassigned_tasks_cached) - set(updated_tasks)
                        if not tmp_tasks_set:
                            del preassigned_tasks_map[key_name]
                        else:
                            preassigned_tasks_map[key_name] = list(tmp_tasks_set)
                    self._update_to_pt_cache(preassigned_tasks_map)
                # update blacklisted_tasks_map into cache
                if had_undo and not force_undo:
                    blacklisted_tasks_map_orig = self._get_from_bt_cache()
                    blacklisted_tasks_map = copy.deepcopy(blacklisted_tasks_map_orig)
                    now_time = datetime.datetime.utcnow()
                    now_rounded_ts = int(now_time.replace(minute=0, second=0, microsecond=0).timestamp())
                    ts_str = str(now_rounded_ts)
                    if ts_str in blacklisted_tasks_map_orig:
                        tmp_bt_list = blacklisted_tasks_map[ts_str]
                        blacklisted_tasks_map[ts_str] = list(set(tmp_bt_list)|set(updated_tasks))
                    else:
                        blacklisted_tasks_map[ts_str] = list(updated_tasks)
                    self._update_to_bt_cache(blacklisted_tasks_map)

    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # lock
            got_lock = self._get_lock()
            if not got_lock:
                origTmpLog.debug('locked by another process. Skipped')
                return self.SC_SUCCEEDED
            origTmpLog.debug('got lock')
            # undo preassigned tasks
            self.undo_preassign()
            # preassign tasks to sites
            self.do_preassign()
            # unlock
            # self._release_lock()
            # origTmpLog.debug('released lock')
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            err_str = traceback.format_exc()
            origTmpLog.error('failed with {0} {1} ; {2}'.format(errtype, errvalue, err_str))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED
