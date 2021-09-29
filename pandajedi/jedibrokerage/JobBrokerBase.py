from pandajedi.jedicore import Interaction

# base class for job brokerage
class JobBrokerBase (object):

    def __init__(self,ddmIF,taskBufferIF):
        self.ddmIF = ddmIF
        self.taskBufferIF = taskBufferIF
        self.liveCounter = None
        self.lockID = None
        self.baseLockID = None
        self.useLock = False
        self.testMode = False
        self.refresh()
        self.task_common = None

    # set task common dictionary
    def set_task_common_dict(self, task_common):
        self.task_common = task_common

    # get task common attribute
    def get_task_common(self, attr_name):
        if self.task_common:
            return self.task_common.get(attr_name)

    # set task common attribute
    def set_task_common(self, attr_name, attr_value):
        self.task_common[attr_name] = attr_value

    def refresh(self):
        self.siteMapper = self.taskBufferIF.getSiteMapper()



    def setLiveCounter(self,liveCounter):
        self.liveCounter = liveCounter



    def getLiveCount(self,siteName):
        if self.liveCounter is None:
            return 0
        return self.liveCounter.get(siteName)



    def setLockID(self,pid,tid):
        self.baseLockID = '{0}-jbr'.format(pid)
        self.lockID = '{0}-{1}'.format(self.baseLockID,tid)



    def getBaseLockID(self):
        if self.useLock:
            return self.baseLockID
        return None

    def releaseSiteLock(self, vo, prodSourceLabel, queue_id):
        # FIXME: releaseSiteLock method is unused elswhere
        if self.useLock:
            self.taskBufferIF.unlockProcessWithPID_JEDI(vo, prodSourceLabel, queue_id, self.lockID, False)


    def lockSite(self, vo, prodSourceLabel, siteName, queue_id):
        if not self.useLock:
            self.useLock = True
        # FIXME: lockSite method is unused elswhere; lockProcess_JEDI arguments have changed and incompatible with the following line
        # self.taskBufferIF.lockProcess_JEDI(vo, prodSourceLabel, siteName, queue_id, self.lockID, True)


    def checkSiteLock(self, vo, prodSourceLabel, siteName, queue_id, resource_name):
        return self.taskBufferIF.checkProcessLock_JEDI( vo=vo, prodSourceLabel=prodSourceLabel,
                                                        cloud=siteName, workqueue_id=queue_id,
                                                        resource_name=resource_name,
                                                        component=None, pid=self.baseLockID, checkBase=True)


    def setTestMode(self):
        self.testMode = True


    # get list of unified sites
    def get_unified_sites(self, scan_site_list):
        unified_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            unifiedName = tmpSiteSpec.get_unified_name()
            unified_list.add(unifiedName)
        return tuple(unified_list)


    # get list of pseudo sites
    def get_pseudo_sites(self, unified_list, scan_site_list):
        unified_list = set(unified_list)
        pseudo_list = set()
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_list:
                pseudo_list.add(tmpSiteName)
        return tuple(pseudo_list)



    # add pseudo sites to skip
    def add_pseudo_sites_to_skip(self, unified_dict, scan_site_list, skipped_dict):
        for tmpSiteName in scan_site_list:
            tmpSiteSpec = self.siteMapper.getSite(tmpSiteName)
            if tmpSiteSpec.get_unified_name() in unified_dict:
                skipped_dict[tmpSiteName] = unified_dict[tmpSiteSpec.get_unified_name()]
        return skipped_dict


Interaction.installSC(JobBrokerBase)
