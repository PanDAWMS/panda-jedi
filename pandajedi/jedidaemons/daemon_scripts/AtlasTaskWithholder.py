import os
import sys
import re
import socket
import traceback

from six import iteritems

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
class AtlasTaskWithholder(object):

    # constructor
    def __init__(self, taskBufferIF, ddmIF):
        # WatchDogBase.__init__(self, taskBufferIF, ddmIF)
        self.pid = '{0}-{1}-dog'.format(socket.getfqdn().split('.')[0], os.getpid())
        # self.cronActions = {'forPrestage': 'atlas_prs'}
        self.vo = 'atlas'
        self.taskBufferIF = taskBufferIF
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
        # list of resource type
        resource_type_list = [ rt.resource_name for rt in self.taskBufferIF.load_resource_types() ]
        # loop
        for prod_source_label in self.prodSourceLabelList:
            # site-rse map
            site_rse_map = self.get_site_rse_map(prod_source_label)
            # parameter from GDP config
            upplimit_ioIntensity = self.taskBufferIF.getConfigValue(
                                        'task_withholder', 'LIMIT_IOINTENSITY_{0}'.format(prod_source_label), 'jedi', self.vo)
            lowlimit_currentPriority = self.taskBufferIF.getConfigValue(
                                        'task_withholder', 'LIMIT_PRIORITY_{0}'.format(prod_source_label), 'jedi', self.vo)
            if upplimit_ioIntensity is None:
                upplimit_ioIntensity = 999999
            if lowlimit_currentPriority is None:
                lowlimit_currentPriority = -999999
            # get work queue for gshare
            work_queue_list = self.workQueueMapper.getAlignedQueueList(self.vo, prod_source_label)
            # loop over work queue
            for work_queue in work_queue_list:
                gshare = work_queue.queue_name
                # busy sites
                busy_sites_list = self.get_busy_sites(gshare)
                # rses of busy sites
                busy_rses = set()
                for site in busy_sites_list:
                    try:
                        busy_rses.update(set(site_rse_map[site]))
                    except KeyError:
                        continue
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
def main(tbuf, ddmif):
    # get logger
    main_log = MsgWrapper(logger)
    main_log.debug('start')
    try:
        # instantiate
        daemon_obj = AtlasTaskWithholder(taskBufferIF=tbuf, ddmIF=ddmif)
        # make tasks pending under certain conditions
        daemon_obj.do_for_data_locality()
    except Exception:
        errtype, errvalue = sys.exc_info()[:2]
        err_str = traceback.format_exc()
        main_log.error('failed with {0} {1} ; {2}'.format(errtype, errvalue, err_str))
    # return
    main_log.debug('done')
    return True
