
from pandacommon.pandamsgbkr import msg_processor
from pandacommon.pandalogger import logger_utils

from pandajedi.jediconfig import jedi_config


# logger
msg_processor.base_logger = logger_utils.setup_logger('JediMsgProcessor')


# Main message processing agent
class MsgProcAgent(msg_processor.MsgProcAgentBase):

    def __init__(self, **kwargs):
        tmp_log = logger_utils.make_logger(msg_processor.base_logger, method_name='MsgProcAgent.__init__')
        tmp_log.debug('start')
        try:
            config_file = jedi_config.msgprocessor.configFile
        except Exception as e:
            tmp_log.warning('failed to read config json file; do nothing. {0}: {1}'.format(e.__class__.__name__, e))
        else:
            tmp_log.debug('really start')
            MsgProcAgentBase.__init__(self, config_file, **kwargs)
