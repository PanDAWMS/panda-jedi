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

    # get available sites
    def get_available_sites(self):
        available_sites_list = []
        available_sites_set = set()
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return available_sites_list
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running')
            nQueue = 0
            for jobStatus in ['activated', 'starting']:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus)
            # available sites
            if nQueue < max(20, nRunning*2)*0.25:
                available_sites_set.add(tmpSiteName)
        available_sites_list = list(available_sites_set)
        # return
        return available_sites_list


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
            available_sites_list = self.get_available_sites()
            # loop or available sites
            for site in available_sites_list:
                # rses of the available site
                available_rses = set()
                try:
                    available_rses.update(set(site_rse_map[site]))
                except KeyError:
                    continue
                # skip if no rse for available site
                if not available_rses:
                    continue
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
                        "AND EXISTS ( "
                            "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                            "WHERE dl.jediTaskID=t.jediTaskID "
                                "AND dl.rse IN ({rse_params_str}) "
                            ") "
                    "ORDER BY t.currentPriority DESC "
                    "FOR UPDATE "
                ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
                # loop over resource type
                for resource_type in resource_type_list:
                    # params map
                    params_map = {
                            ':resource_type': resource_type,
                        }
                    params_map.update(rse_params_map)
                    # set pending
                    dry_run = True
                    if dry_run:
                        dry_sql_query = (
                            "SELECT t.jediTaskID "
                            "FROM {jedi_schema}.JEDI_Tasks t "
                            "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                                "AND t.resource_type=:resource_type "
                                "AND EXISTS ( "
                                    "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                                    "WHERE dl.jediTaskID=t.jediTaskID "
                                        "AND dl.rse IN ({rse_params_str}) "
                                    ") "
                            "ORDER BY t.currentPriority DESC "
                        ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
                        res = self.taskBufferIF.querySQL(dry_sql_query, params_map)
                        n_tasks = 0 if res is None else len(res)
                        if n_tasks > 0:
                            result = [ x[0] for x in res ]
                            tmp_log.debug('[dry run] rtype={resource_type:<11} {n_tasks:>3} tasks would be preassigned to {site} '.format(
                                            resource_type=resource_type, n_tasks=max(n_tasks, max_preassigned_tasks), site=site))
                    else:
                        n_tasks = self.taskBufferIF.queryTasksToPreassign_JEDI(sql_query, params_map, site, limit=max_preassigned_tasks)
                        if n_tasks is not None and n_tasks > 0:
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
