import os
import sys
import re
import socket
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


# queue filler watchdog for ATLAS
class AtlasQueueFillerWatchDog(WatchDogBase):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        # self.cronActions = {'forPrestage': 'atlas_prs'}
        self.vo = 'atlas'
        self.prodSourceLabelList = ['managed']
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

    # preassign tasks to site
    def do_preassign_to_sites(self):
        tmp_log = MsgWrapper(logger)
        # refresh
        self.refresh()
        # list of resource type
        resource_type_list = [ rt.resource_name for rt in self.taskBufferIF.load_resource_types() ]
        # loop
        for prod_source_label in self.prodSourceLabelList:
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label)
            # parameter from GDP config
            max_preassigned_tasks = self.taskBufferIF.getConfigValue(
                                        'queue_filler', 'MAX_PREASSIGNED_TASKS_{0}'.format(prod_source_label), 'jedi', self.vo)
            if max_preassigned_tasks is None:
                max_preassigned_tasks = 3
            # available sites
            available_sites_dict = self.get_available_sites()
            # loop or available sites
            for site, tmpSiteSpec in available_sites_dict.items():
                # rses of the available site
                available_rses = set()
                try:
                    available_rses.update(set(site_rse_map[site]))
                except KeyError:
                    continue
                # skip if no rse for available site
                if not available_rses:
                    continue
                # site attributes
                site_maxrss =  tmpSiteSpec.maxrss if tmpSiteSpec.maxrss not in (0, None) else 999999
                site_corecount_allowed = []
                if tmpSiteSpec.is_unified or tmpSiteSpec.capability == 'ucore':
                    site_corecount_allowed = [1, tmpSiteSpec.coreCount]
                else:
                    if tmpSiteSpec.capability == 'mcore':
                        site_corecount_allowed = [tmpSiteSpec.coreCount]
                    else:
                        site_corecount_allowed = [1]
                # make sql parameters of site_corecount_allowed
                site_corecount_allowed_params_list = []
                site_corecount_allowed_params_map = {}
                for j, cc in enumerate(site_corecount_allowed):
                    sca_param = ':site_corecount_{0}'.format(j + 1)
                    site_corecount_allowed_params_list.append(sca_param)
                    site_corecount_allowed_params_map[sca_param] = cc
                site_corecount_allowed_params_str = ','.join(site_corecount_allowed_params_list)
                # make sql parameters of rses
                available_rses = list(available_rses)
                rse_params_list = []
                rse_params_map = {}
                for j, rse in enumerate(available_rses):
                    rse_param = ':rse_{0}'.format(j + 1)
                    rse_params_list.append(rse_param)
                    rse_params_map[rse_param] = rse
                rse_params_str = ','.join(rse_params_list)
                # sql
                sql_query = (
                    "SELECT t.jediTaskID "
                    "FROM {jedi_schema}.JEDI_Tasks t "
                    "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                        "AND t.resource_type=:resource_type "
                        "AND t.ramCount<:site_maxrss "
                        "AND t.coreCount IN ({site_corecount_allowed_params_str}) "
                        "AND EXISTS ( "
                            "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                            "WHERE dl.jediTaskID=t.jediTaskID "
                                "AND dl.rse IN ({rse_params_str}) "
                            ") "
                    "ORDER BY t.currentPriority DESC "
                    "FOR UPDATE "
                ).format(jedi_schema=jedi_config.db.schemaJEDI,
                            site_corecount_allowed_params_str=site_corecount_allowed_params_str,
                            rse_params_str=rse_params_str)
                # loop over resource type
                for resource_type in resource_type_list:
                    # params map
                    params_map = {
                            ':resource_type': resource_type,
                            ':site_maxrss': tmpSiteSpec.maxrss if tmpSiteSpec.maxrss not in (0, None) else 999999,
                        }
                    params_map.update(rse_params_map)
                    params_map.update(site_corecount_allowed_params_map)
                    # set pending
                    dry_run = True
                    if dry_run:
                        dry_sql_query = (
                            "SELECT t.jediTaskID "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                                "AND t.resource_type=:resource_type "
                                "AND t.ramCount<:site_maxrss "
                                "AND t.coreCount IN ({site_corecount_allowed_params_str}) "
                                "AND EXISTS ( "
                                    "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                                    "WHERE dl.jediTaskID=t.jediTaskID "
                                        "AND dl.rse IN ({rse_params_str}) "
                                    ") "
                            "ORDER BY t.currentPriority DESC "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI,
                                    site_corecount_allowed_params_str=site_corecount_allowed_params_str,
                                    rse_params_str=rse_params_str)
                        res = self.taskBufferIF.querySQL(dry_sql_query, params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            result = [ x[0] for x in res ]
                            tmp_log.debug('[dry run] rtype={resource_type:<11} {n_tasks:>3} tasks would be preassigned to {site} '.format(
                                            resource_type=resource_type, n_tasks=min(n_tasks, max_preassigned_tasks), site=site))
                    else:
                        updated_tasks = self.taskBufferIF.queryTasksToPreassign_JEDI(sql_query, params_map, site, limit=max_preassigned_tasks)
                        if updated_tasks is None:
                            # dbproxy method failed
                            tmp_log.error('rtype={resource_type:<11} failed to preassign tasks to {site} '.format(
                                            resource_type=resource_type, site=site))
                            return
                        n_tasks = len(updated_tasks)
                        if n_tasks > 0:
                            tmp_log.info('rtype={resource_type:<11} {n_tasks:>3} tasks preassigned to {site} '.format(
                                            resource_type=resource_type, n_tasks=str(n_tasks), site=site))


    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # preassign tasks to sites
            self.do_preassign_to_sites()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            err_str = traceback.format_exc()
            origTmpLog.error('failed with {0} {1} ; {2}'.format(errtype, errvalue, err_str))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED
