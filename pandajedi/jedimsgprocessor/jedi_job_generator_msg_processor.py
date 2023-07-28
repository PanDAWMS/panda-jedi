import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
# from pandajedi.jedicore.FactoryBase import FactoryBase
# from pandajedi.jedicore.JediCoreUtils import convert_config_params, parse_init_params
from pandajedi.jedicore.ThreadUtils import ThreadPool, ListWithLock
from pandajedi.jediorder.JobGenerator import JobGeneratorThread
from pandajedi.jediorder.TaskSetupper import TaskSetupper
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediconfig import jedi_config

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Jedi Job Generator message processor plugin
class JediJobGeneratorMsgProcPlugin(BaseMsgProcPlugin):
    """
    Message-driven Post-Processor
    """
    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        # DDM interface
        self.ddmIF = DDMInterface()
        self.ddmIF.setupInterface()
        # get SiteMapper
        # siteMapper = self.tbIF.getSiteMapper()
        # get work queue mapper
        # workQueueMapper = self.tbIF.getWorkQueueMap()
        # get TaskSetupper
        # taskSetupper = TaskSetupper(self.vos, self.prodSourceLabels)
        # taskSetupper.initializeMods(self.tbIF, self.ddmIF)
        self.pid = self.get_pid()

    def process(self, msg_obj):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='process')
        # start
        tmp_log.info('start')
        tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
        # parse json
        try:
            msg_dict = json.loads(msg_obj.data)
        except Exception as e:
            err_str = 'failed to parse message json {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_obj.data)
            tmp_log.error(err_str)
            raise
        # sanity check
        try:
            msg_type = msg_dict['msg_type']
        except Exception as e:
            err_str = 'failed to parse message object dict {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_dict)
            tmp_log.error(err_str)
            raise
        if msg_type != 'jedi_job_generator':
            # FIXME
            err_str = 'got unknown msg_type {0} , skipped '.format(msg_type)
            tmp_log.error(err_str)
            raise 
        # run
        try:
            # get task to generate jobs
            task_id = int(msg_dict['taskid'])
            _, taskSpec = self.tbIF.getTaskWithID_JEDI(task_id)
            if not taskSpec:
                tmp_log.debug('unknown task {}'.format(task_id))
            else:
                # get WQ
                vo = taskSpec.vo
                prodSourceLabel = taskSpec.prodSourceLabel
                workQueue = self.tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
                # get inputs
                tmpList = self.tbIF.getTasksToBeProcessed_JEDI(self.pid, None, workQueue, None, None, nFiles=1000,
                                                                target_tasks=[task_id])
                if tmpList:
                    inputList = ListWithLock(tmpList)
                    # create thread
                    threadPool = ThreadPool()
                    siteMapper = self.tbIF.getSiteMapper()
                    taskSetupper = TaskSetupper(vo, prodSourceLabel)
                    taskSetupper.initializeMods(self.tbIF, self.ddmIF)
                    gen_thr = JobGeneratorThread(inputList, threadPool, self.tbIF, self.ddmIF, siteMapper,
                                                True, taskSetupper, self.pid, workQueue, 'pjmsg',
                                                None, None, None, False)
                    gen_thr.start()
                    gen_thr.join()
                tmp_log.debug('generated jobs for task {}'.format(task_id))
        except Exception as e:
            err_str = 'failed to run, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
