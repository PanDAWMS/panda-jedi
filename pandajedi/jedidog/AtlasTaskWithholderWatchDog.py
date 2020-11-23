import os
import sys
import re
import itertools
import socket
import traceback

from six import iteritems

from .WatchDogBase import WatchDogBase
from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper
from pandajedi.jedicore.ThreadUtils import ListWithLock
from pandajedi.jedicore import Interaction
from pandajei.jedibrokerage import AtlasBrokerUtils

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
        self.prodSourceLabelList = ['managed', 'user']
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
    def get_site_rse_map(self, prod_source_label, job_label):
        site_rse_map = {}
        for tmpPseudoSiteName in self.allSiteList:
            tmpSiteSpec = self.siteMapper.getSite(tmpPseudoSiteName)
            tmpSiteName = tmpSiteSpec.get_unified_name()
            scope_input, scope_output = DataServiceUtils.select_scope(tmpSiteSpec, prod_source_label, job_label)
            endpoint_token_map = tmpSiteSpec.ddm_endpoints_input[scope_input].getTokenMap('input')
            # fill
            site_rse_map[tmpSiteName] = list(endpoint_token_map.values())
        # return
        return site_rse_map

    # get busy sites
    def get_busy_sites(self, gshare):
        busy_sites_list = []
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
        # refresh
        self.refresh()
        # list of job label, gshare, resource type
        job_label_list = JobUtils.job_labels
        gshare_list = [ res['name'] for res in self.taskBufferIF.getGShareStatus() ]
        resource_type_list = self.taskBufferIF.load_resource_types()
        # parameter from GDP config
        upplimit_ioIntensity = self.taskBufferIF.getConfigValue('????', '??????', 'jedi', self.vo)
        lowlimit_currentPriority = self.taskBufferIF.getConfigValue('????', '??????', 'jedi', self.vo)
        # loop
        grand_iter = itertools.product(self.prodSourceLabelList, job_label_list, gshare_list, resource_type_list)
        for prod_source_label, job_label, gshare, resource_type in grand_iter:
            # busy sites
            busy_sites_list = self.get_busy_sites(gshare)
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label, job_label)
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
                "SELECT t.jediTaskID FOR UPDATE "
                "FROM {jedi_schema}.JEDI_Tasks t "
                "WHERE t.status IN ('ready','running','scouting') AND t.lockedBy IS NULL "
                    "AND t.gshare=:gshare AND t.resource_type=:resource_type "
                    "AND t.ioIntensity>=:ioIntensity AND t.currentPriority<:currentPriority "
                    "AND NOT EXISTS ( "
                        "SELECT * FROM {jedi_schema}.JEDI_Dataset_Locality dl "
                        "WHERE dl.jediTaskID=t.jediTaskID "
                            "AND dl.rse NOT IN ({rse_params_str}) "
                        ") "
            ).format(jedi_schema=jedi_config.db.schemaJEDI, rse_params_str=rse_params_str)
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
            origTmpLog.error('failed with {0} {1}'.format(errtype, errvalue))
        # return
        origTmpLog.debug('done')
        return self.SC_SUCCEEDED
