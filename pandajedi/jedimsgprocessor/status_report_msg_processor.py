import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# list of task statuses to return to iDDS
to_return_task_status_list = [
    'defined', 
    'ready', 
    'scouting', 
    'pending', 'paused', 
    'running', 
    'prepared', 
    'done', 'finished', 'failed',
    'broken', 'aborted',
]


# status report message processing plugin
class StatusReportMsgProcPlugin(BaseMsgProcPlugin):
    """
    Messaging status of jobs and tasks
    Forward the incoming message to other msg_processors plugin (e.g. Kafka) if configured in params
    Return the processed message to send to iDDS via MQ
    """
    
    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        # forwarding plugins: incoming message will be fowarded to process method of these plugins
        self.forwarding_plugins = []
        forwarding_plugin_names = self.params.get('forwading_plugins', [])
        if 'kafka' in forwarding_plugin_names:
            # Kafka
            from pandajedi.jedimsgprocessor.kafka_msg_processor import KafkaMsgProcPlugin
            plugin_inst = KafkaMsgProcPlugin()
            plugin_inst.initialize()
            self.forwarding_plugins.append(plugin_inst)

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
        # whether to return the message
        to_return_message = False
        # run different plugins according to message type
        try:
            if msg_type == 'task_status':
                tmp_log.debug('task_status')
                # forwarding
                for plugin_inst in self.forwarding_plugins:
                    try:
                        plugin_inst.process(msg_obj, decoded_data=msg_dict)
                    except Exception as exc:
                        tmp_log.error(f'{exc.__class__.__name__}: {exc}')
                # only return certain statuses
                if msg_dict.get('status') in to_return_task_status_list:
                    to_return_message = True
            elif msg_type == 'job_status':
                tmp_log.debug('job_status')
                # forwarding
                for plugin_inst in self.forwarding_plugins:
                    try:
                        plugin_inst.process(msg_obj, decoded_data=msg_dict)
                    except Exception as exc:
                        tmp_log.error(f'{exc.__class__.__name__}: {exc}')
            else:
                warn_str = 'unknown msg_type : {0}'.format(msg_type)
                tmp_log.warning(warn_str)
        except Exception:
            raise
        # done
        tmp_log.info('done')
        # return
        if to_return_message:
            return msg_obj.data

