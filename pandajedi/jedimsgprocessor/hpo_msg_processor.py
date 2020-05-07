import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Hyper-Parameter-Optimization message processing plugin
class HPOMsgProcPlugin(BaseMsgProcPlugin):

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
            jeditaskid = int(msg_dict['workload_id'])
            if msg_type == 'file_hyperparameteropt':
                target_list = msg_dict['files']
            else:
                raise ValueError('invalid msg_type value: {0}'.format(msg_type))
        except Exception as e:
            err_str = 'failed to parse message object dict {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_dict)
            tmp_log.error(err_str)
            raise
        # run
        try:
            # map
            scope_name_list_map = {}
            # loop over targets
            for target in target_list:
                the_id = target['name']
                # parameter, loss = json.loads(target['path'])
                # insert events
                res = self.tbIF.insertHpoEventAboutIdds_JEDI(   jedi_task_id=jeditaskid,
                                                                start_number=the_id,
                                                                end_number=the_id)
                # check if ok
                if res:
                    tmp_log.debug('jeditaskid={0}, id={1}, event inserted'.format(jeditaskid, the_id))
                else:
                    tmp_log.warning('jeditaskid={0}, id={1}, event not inserted'.format(jeditaskid, the_id))
        except Exception as e:
            err_str = 'failed to parse message object, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
