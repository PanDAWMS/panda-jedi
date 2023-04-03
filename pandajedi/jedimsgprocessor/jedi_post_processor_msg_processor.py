import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jedicore.FactoryBase import FactoryBase
from pandajedi.jedicore.JediCoreUtils import convert_config_params, parse_init_params
from pandajedi.jediorder.PostProcessor import PostProcessorThread
from pandajedi.jediddm.DDMInterface import DDMInterface
from pandajedi.jediconfig import jedi_config

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Jedi Post-Processor message processor plugin
class JediPostProcessorMsgProcPlugin(BaseMsgProcPlugin):
    """
    Message-driven Post-Processor
    """
    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        # DDM interface
        ddmIF = DDMInterface()
        ddmIF.setupInterface()
        # factory bases and post processor thread object for all vos and prodsourcelabels
        self.post_processor_thread_dict = dict()
        for itemStr in jedi_config.postprocessor.procConfig.split(';'):
            items = convert_config_params(itemStr)
            vos = parse_init_params(items[0])
            prodsourcelabels = parse_init_params(items[1])
            tmp_factory_base_obj = FactoryBase(vos=vos, 
                                                sourceLabels=prodsourcelabels, 
                                                logger=base_logger, 
                                                modConfig=jedi_config.postprocessor.modConfig)
            tmp_post_processor_thread_obj = PostProcessorThread(taskList=None, 
                                                                threadPool=None, 
                                                                taskbufferIF=self.tbIF, 
                                                                ddmIF=ddmIF, 
                                                                implFactory=tmp_factory_base_obj)
            for vo in vos:
                for prodsourcelabel in prodsourcelabels:
                    self.post_processor_thread_dict[(vo, prodsourcelabel)] = tmp_post_processor_thread_obj
                    
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
        if msg_type != 'task_post_processor':
            # FIXME
            err_str = 'got unknown msg_type {0} , skipped '.format(msg_type)
            tmp_log.error(err_str)
            raise 
        # run
        try:
            task_id = msg_dict['task_id']
            vo = msg_dict['task_vo']
            prodsourcelabel = msg_dict['task_prodsourcelabel']
            ret = self.tbIF.prepareTasksToBeFinished_JEDI(vo, 
                                                            prodSourceLabel, 
                                                            jedi_config.postprocessor.nTasks, 
                                                            self.get_pid())
            task_list = self.tbIF.getTasksToBeFinished_JEDI(vo,
                                                            prodSourceLabel,
                                                            self.get_pid(),
                                                            jedi_config.postprocessor.nTasks)
            if task_list:
                tmp_post_processor_thread_obj = self.post_processor_thread_dict[(vo, prodsourcelabel)]
                tmp_post_processor_thread_obj.post_process_tasks(task_list)
                tmp_log.info('post processed task {0}'.format(task_id))
            else:
                tmp_log.warning('got empty list of task {0}; do nothing '.format(task_id))
        except Exception as e:
            err_str = 'failed to run, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
