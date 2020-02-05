import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Tape carousel message processing plugin
class TapeCarouselMsgProcPlugin(BaseMsgProcPlugin):

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
            jeditaskid = msg_dict['workload_id']
            if msg_type == 'file_stagein':
                target_list = msg_dict['files']
            elif msg_type == 'collection_stagein':
                target_list = msg_dict['collections']
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
                # skipped failed target
                if target['status'] != 'Available':
                    continue
                name = target['name']
                scope = target['scope']
                scope_name_list_map.setdefault(scope, [])
                scope_name_list_map[scope].append(name)
            # run by each scope
            for scope, name_list in scope_name_list_map.items():
                # about files or datasets
                if msg_type == 'file_stagein':
                    tmp_log.debug('scope={0}, update about files...'.format(scope))
                    res = self.tbIF.updateInputFilesStagedAboutIdds_JEDI(jeditaskid, scope, name_list)
                    tmp_log.info('scope={0}, updated {1} files'.format(scope, res))
                elif msg_type == 'collection_stagein':
                    tmp_log.debug('scope={0}, update about datasets...'.format(scope))
                    res = self.tbIF.updateInputDatasetsStagedAboutIdds_JEDI(jeditaskid, scope, name_list)
                    tmp_log.info('scope={0}, updated {1} datasets'.format(scope, res))
                # check if all ok
                if res == len(target_list):
                    tmp_log.debug('scope={0}, all OK'.format(scope))
                else:
                    tmp_log.warning('scope={0}, only {1} out of {2} done...'.format(scope, res, len(target_list)))
        except Exception as e:
            err_str = 'failed to parse message object, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
