import re

import yaml

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

from pandacommon.pandalogger import logger_utils
from pandaserver.dataservice.DDMHandler import DDMHandler


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# panda dataset callback message processing plugin
class PandaCallbackMsgProcPlugin(BaseMsgProcPlugin):

    def process(self, msg_obj):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='process')
        # start
        # tmp_log.info('start')
        # tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
        # parse yaml
        try:
            message_dict = yaml.safe_load(msg_obj.data)
        except Exception as e:
            err_str = 'failed to parse message yaml {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_obj.data)
            tmp_log.error(err_str)
            raise
        # run
        try:
            to_continue = True
            dsn = 'UNKNOWN'
            # check event type
            event_type = message_dict['event_type']
            if event_type not in ['datasetlock_ok']:
                # tmp_log.debug('{0} skip'.format(event_type))
                to_continue = False
            if to_continue:
                # tmp_log.debug('{0} start'.format(event_type))
                message_payload = message_dict['payload']
                # only for _dis or _sub
                dsn = message_payload['name']
                if (re.search('_dis\d+$', dsn) is None) and (re.search('_sub\d+$', dsn) is None):
                    # tmp_log.debug('{0} is not _dis or _sub dataset, skip'.format(dsn))
                    to_continue = False
            if to_continue:
                tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
                tmp_log.debug('{0} start'.format(event_type))
                # take action
                scope = message_payload['scope']
                site  = message_payload['rse']
                tmp_log.debug('{dsn} site={site} type={type}'.format(dsn=dsn, site=site, type=event_type))
                thr = DDMHandler(taskBuffer=self.tbIF, vuid=None, site=site, dataset=dsn, scope=scope)
                # just call run rather than start+join, to run it in main thread instead of spawning new thread
                thr.run()
                del thr
                tmp_log.debug('done {0}'.format(dsn))
        except Exception as e:
            err_str = 'failed to run, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        # tmp_log.info('done')
