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


# task withholder watchdog for ATLAS
class AtlasTaskWithholderWatchDog(WatchDogBase):

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

    # get busy sites
    def get_busy_sites(self, gshare):
        busy_sites_list = []
        # get global share
        tmpSt, jobStatPrioMap = self.taskBufferIF.getJobStatisticsByGlobalShare(self.vo)
        if not tmpSt:
            # got nothing...
            return busy_sites_list
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            # get nQueue and nRunning
            nRunning = AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, 'running', gshare)
            nQueue = 0
            for jobStatus in ['defined', 'assigned', 'activated', 'starting']:
                nQueue += AtlasBrokerUtils.getNumJobs(jobStatPrioMap, tmpSiteName, jobStatus, gshare)
            # busy sites
            if (nQueue + 1)/(nRunning + 1) > 2:
                busy_sites_list.append(tmpSiteName)
        # return
        return busy_sites_list

    # # handle waiting jobs
    # def do_make_tasks_pending(self, task_list):
    #     tmpLog = MsgWrapper(logger, 'do_make_tasks_pending')
    #     tmpLog.debug('start')
    #     # check every x min
    #     checkInterval = 20
    #     # make task pending
    #     for taskID, pending_reason in task_list:
    #         tmpLog = MsgWrapper(logger, '< #ATM #KV do_make_tasks_pending jediTaskID={0}>'.format(taskID))
    #         retVal = self.taskBufferIF.makeTaskPending_JEDI(taskID, reason=pending_reason)
    #         tmpLog.debug('done with {0}'.format(retVal))


    # set tasks to be pending due to condition of data locality
    def do_for_data_locality(self):
        tmp_log = MsgWrapper(logger)
        # refresh
        self.refresh()
        # list of gshare resource type
        gshare_list = [ res['name'] for res in self.taskBufferIF.getGShareStatus() ]
        resource_type_list = [ rt.resource_name for rt in self.taskBufferIF.load_resource_types() ]
        # combination of prodSourceLabel and gShare
        psl_gs_sql = (
                'SELECT PRODSOURCELABEL, NAME '
                'FROM ATLAS_PANDA.GLOBAL_SHARES '
            )
        psl_gs_list = self.taskBufferIF.querySQL(psl_gs_sql, {})
        # loop
        for prod_source_label, gshare in psl_gs_list:
            if prod_source_label not in self.prodSourceLabelList:
                continue
            if gshare not in gshare_list:
                # skip non-leaf gshare
                continue
            # parameter from GDP config
            if prod_source_label == 'managed':
                upplimit_ioIntensity = self.taskBufferIF.getConfigValue('task_withholder', 'PROD_LIMIT_IOINTENSITY', 'jedi', self.vo)
                lowlimit_currentPriority = self.taskBufferIF.getConfigValue('task_withholder', 'PROD_LIMIT_PRIORITY', 'jedi', self.vo)
            else:
                upplimit_ioIntensity = 999999
                lowlimit_currentPriority = -999999
            # busy sites
            busy_sites_list = self.get_busy_sites(gshare)
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label)
            # rses of busy sites
            busy_rses = set()
            for site in busy_sites_list:
                busy_rses.update(set(site_rse_map[site]))
            busy_rses = list(busy_rses)
            # make sql parameters of rses
            rse_params_list = []
            rse_params_map = {}
            for j, rse in enumerate(busy_rses):
                rse_param = ':rse_{0}'.format(j + 1)
                rse_params_list.append(rse_param)
                rse_params_map[rse_param] = rse
            rse_params_str = ','.join(rse_params_list)
            # sql
            sql_query = (
                "SELECT t.jediTaskID "
                "FROM {jedi_schema}.JEDI_Tasks t "
                "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                    "AND t.gshare=:gshare AND t.resource_type=:resource_type "
                    "AND t.ioIntensity>=:ioIntensity AND t.currentPriority<:currentPriority "
                    "AND NOT EXISTS ( "
                        "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                        "WHERE dl.jediTaskID=t.jediTaskID "
                            "AND dl.rse NOT IN ({rse_params_str}) "
                        ") "
                "FOR UPDATE "
            ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
            # loop over resource type
            for resource_type in resource_type_list:
                # params map
                params_map = {
                        ':gshare': gshare,
                        ':resource_type': resource_type,
                        ':ioIntensity': upplimit_ioIntensity,
                        ':currentPriority': lowlimit_currentPriority,
                    }
                params_map.update(rse_params_map)
                # pending reason
                reason = 'no local input data'
                # set pending
                self.taskBufferIF.queryTasksToBePending_JEDI(sql_query, params_map, reason)


    # main
    def doAction(self):
        try:
            # get logger
            origTmpLog = MsgWrapper(logger)
            origTmpLog.debug('start')
            # make tasks pending under certain conditions
            self.do_for_data_locality()
        except Exception:
            errtype, errvalue = sys.exc_info()[:2]
            err_str = traceback.format_exc()
            origTmpLog.error('failed with {0} {1} ; {2}'.format(errtype, errvalue, err_str))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED
