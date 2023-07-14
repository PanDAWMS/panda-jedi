import json
from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin
from pandacommon.pandalogger import logger_utils
from pandacommon.kafkapublisher.KafkaPublisher import KafkaPublisher

# Logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])

# Kafka message processing plugin
class KafkaMsgProcPlugin(BaseMsgProcPlugin):
    def initialize(self):
        """
        initialize plugin instance, run once before loop in thread
        """
        self.publisher = KafkaPublisher()

    def process(self, msg_obj, decoded_data=None):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='process')

        # start
        tmp_log.info('start')
        tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))

        # Parse and access the message content from msg_obj.data
        if decoded_data is None:
            # json decode
            try:
                message_content = json.loads(msg_obj.data)
            except Exception as e:
                err_str = 'failed to parse message json {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_obj.data)
                tmp_log.error(err_str)
                raise
        else:
            message_content = decoded_data

        # Publish the message to Kafka
        self.publisher.publish_message(message_content)
        tmp_log.debug(f'sent {message_content}')
        tmp_log.info('done')

    def terminate(self):
        self.publisher.close()
