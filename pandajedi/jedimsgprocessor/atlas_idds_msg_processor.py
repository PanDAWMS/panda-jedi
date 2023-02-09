import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jedimsgprocessor.tape_carousel_msg_processor import TapeCarouselMsgProcPlugin
from pandajedi.jedimsgprocessor.hpo_msg_processor import HPOMsgProcPlugin
from pandajedi.jedimsgprocessor.processing_msg_processor import ProcessingMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Atlas iDDS message processing plugin, a bridge connect to other idds related message processing plugins
class AtlasIddsMsgProcPlugin(BaseMsgProcPlugin):

    def initialize(self):
        BaseMsgProcPlugin.initialize(self)
        self.plugin_TapeCarousel = TapeCarouselMsgProcPlugin()
        self.plugin_HPO = HPOMsgProcPlugin()
        self.plugin_Processing = ProcessingMsgProcPlugin()
        # for each plugin
        for _plugin in [self.plugin_TapeCarousel, self.plugin_HPO, self.plugin_Processing]:
            # initialize each
            _plugin.initialize()
            # use the same taskBuffer interface
            try:
                del _plugin.tbIF
            except Exception:
                pass
            _plugin.tbIF = self.tbIF

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
        # run different plugins according to message type
        try:
            if msg_type in ('file_stagein', 'collection_stagein', 'work_stagein'):
                self.plugin_TapeCarousel.process(msg_obj, decoded_data=msg_dict)
                tmp_log.debug('to tape_carousel')
            elif msg_type in ('file_hyperparameteropt', 'collection_hyperparameteropt', 'work_hyperparameteropt'):
                self.plugin_HPO.process(msg_obj, decoded_data=msg_dict)
                tmp_log.debug('to hpo')
            elif msg_type in ('file_processing', 'collection_processing', 'work_processing'):
                self.plugin_Processing.process(msg_obj, decoded_data=msg_dict)
                tmp_log.debug('to processing')
            else:
                # Asked by iDDS and message broker guys, JEDI needs to consume unknown types of messages and do nothing...
                warn_str = 'unknown msg_type : {0}'.format(msg_type)
                tmp_log.warning(warn_str)
        except Exception:
            raise
        # done
        tmp_log.info('done')
