import os
import json
import socket
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jedicore.ThreadUtils import ThreadPool, ListWithLock
from pandajedi.jediorder.TaskSetupper import TaskSetupper
from pandajedi.jediorder.JobGenerator import JobGeneratorThread

from pandacommon.pandalogger import logger_utils

# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# plugin to process messages from Panda to JEDI
class PandaToJediMsgProcPlugin(BaseMsgProcPlugin):

    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        self.ddmIF = DDMInterface()
        self.ddmIF.setupInterface()
        self.pid = '{0}-{1}_{2}-pjmsg'.format(socket.getfqdn().split('.')[0], os.getpid(), os.getpgrp())

    def process(self, msg_obj, decoded_data=None):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='process')
        # start
        tmp_log.info('start')
        # parse
        if decoded_data is None:
            # json decode
            try:
                msg_dict = json.loads(msg_obj.data)
            except Exception as e:
                err_str = 'failed to parse message json {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e,
                                                                                         msg_obj.data)
                tmp_log.error(err_str)
                raise
        else:
            msg_dict = decoded_data
        # run
        try:
            tmp_log.debug('got message {0}'.format(msg_dict))
            if msg_dict['msg_type'] == 'generate_job':
                # get task to generate jobs
                jediTaskID = int(msg_dict['taskid'])
                s, taskSpec = self.tbIF.getTaskWithID_JEDI(jediTaskID)
                if not taskSpec:
                    tmp_log.debug('unknown task {}'.format(jediTaskID))
                else:
                    # get WQ
                    vo = taskSpec.vo
                    prodSourceLabel = taskSpec.prodSourceLabel
                    workQueue = self.tbIF.getWorkQueueMap().getQueueWithIDGshare(taskSpec.workQueue_ID, taskSpec.gshare)
                    # get inputs
                    tmpList = self.tbIF.getTasksToBeProcessed_JEDI(self.pid, None, workQueue, None, None, nFiles=1000,
                                                                   target_tasks=[jediTaskID])
                    if tmpList:
                        inputList = ListWithLock(tmpList)
                        # create thread
                        threadPool = ThreadPool()
                        siteMapper = self.tbIF.getSiteMapper()
                        taskSetupper = TaskSetupper(vo, prodSourceLabel)
                        taskSetupper.initializeMods(self.tbIF, self.ddmIF)
                        gen = JobGeneratorThread(inputList, threadPool, self.tbIF, self.ddmIF, siteMapper,
                                                 True, taskSetupper, self.pid, workQueue, 'pjmsg',
                                                 None, None, None, False)
                        gen.start()
                        gen.join()
            else:
                tmp_log.debug('unknown message type : {}'.format(msg_dict['msg_type']))
        except Exception as e:
            err_str = 'failed to run, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
