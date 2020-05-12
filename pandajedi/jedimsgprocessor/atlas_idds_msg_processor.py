import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandajedi.jedimsgprocessor.tape_carousel_msg_processor import TapeCarouselMsgProcPlugin
from pandajedi.jedimsgprocessor.hpo_msg_processor import HPOMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Atlas iDDS message processing plugin, a bridge connect to other idds related message processing plugins
class AtlasIddsMsgProcPlugin(BaseMsgProcPlugin):

    def initialize(self):
        super().initialize()
        self.plugin_TapeCarousel = TapeCarouselMsgProcPlugin()
        self.plugin_HPO = HPOMsgProcPlugin()

    def process(self, msg_obj):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, method_name='process')
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
        if msg_type in ('file_stagein', 'collection_stagein'):
            self.plugin_TapeCarousel.process(msg_obj, decoded_data=msg_dict)
        elif msg_type in ('file_hyperparameteropt', 'collection_hpyerparameteropt'):
            self.plugin_HPO.process(msg_obj, decoded_data=msg_dict)
        else:
            raise ValueError('invalid msg_type value: {0}'.format(msg_type))
        # done
        tmp_log.info('done')
