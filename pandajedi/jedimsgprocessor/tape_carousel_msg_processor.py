import json

from pandajedi.jedimsgprocessor.base_msg_processor import BaseMsgProcPlugin

from pandacommon.pandalogger import logger_utils


# logger
base_logger = logger_utils.setup_logger(__name__.split('.')[-1])


# Tape carousel message processing plugin
class TapeCarouselMsgProcPlugin(BaseMsgProcPlugin):

    def process(self, msg_obj, decoded_data=None):
        # logger
        tmp_log = logger_utils.make_logger(base_logger, token=self.get_pid(), method_name='process')
        # start
        tmp_log.info('start')
        tmp_log.debug('sub_id={0} ; msg_id={1}'.format(msg_obj.sub_id, msg_obj.msg_id))
        # parse
        if decoded_data is None:
            # json decode
            try:
                msg_dict = json.loads(msg_obj.data)
            except Exception as e:
                err_str = 'failed to parse message json {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_obj.data)
                tmp_log.error(err_str)
                raise
        else:
            msg_dict = decoded_data
        # sanity check
        try:
            jeditaskid = int(msg_dict['workload_id'])
            # message type
            msg_type = msg_dict['msg_type']
            if msg_type == 'file_stagein':
                target_list = msg_dict['files']
            elif msg_type == 'collection_stagein':
                target_list = msg_dict['collections']
            elif msg_type == 'work_stagein':
                pass
            else:
                raise ValueError('invalid msg_type value: {0}'.format(msg_type))
            # relation type
            relation_type = msg_dict.get('relation_type')
        except Exception as e:
            err_str = 'failed to parse message object dict {2} , skipped. {0} : {1}'.format(e.__class__.__name__, e, msg_dict)
            tmp_log.error(err_str)
            raise
        # run
        try:
            # initialize to_proceed
            to_proceed = False
            # type filters
            if msg_type in ['file_stagein', 'collection_stagein'] \
                    and relation_type in ['output']:
                to_proceed = True
            # whether to proceed the targets
            if to_proceed:
                # map
                scope_name_dict_map = {}
                # loop over targets
                for target in target_list:
                    name = target['name']
                    scope = target['scope']
                    datasetid = target.get('external_coll_id', None)
                    fileid = target.get('external_content_id', None)
                    if (msg_type == 'file_stagein' and target['status'] in ['Available']) \
                            or (msg_type == 'collection_stagein' and target['status'] in ['Closed']):
                        scope_name_dict_map.setdefault(scope, {})
                        scope_name_dict_map[scope][name] = (datasetid, fileid)
                    else:
                        # got target in bad attributes, do nothing
                        tmp_log.debug('jeditaskid={jeditaskid}, scope={scope}, msg_type={msg_type}, status={status}, did nothing for bad target'.format(
                                        jeditaskid=jeditaskid, scope=scope, msg_type=msg_type, status=target['status']))
                        pass
                # run by each scope
                for scope, name_dict in scope_name_dict_map.items():
                    # about files or datasets in good status
                    if msg_type == 'file_stagein':
                        tmp_log.debug('jeditaskid={0}, scope={1}, update about files...'.format(jeditaskid, scope))
                        res = self.tbIF.updateInputFilesStagedAboutIdds_JEDI(jeditaskid, scope, name_dict)
                        if res is None:
                            # got error and rollback in dbproxy
                            err_str = 'jeditaskid={0}, scope={1}, failed to update files'.format(jeditaskid, scope)
                            raise RuntimeError(err_str)
                        tmp_log.info('jeditaskid={0}, scope={1}, updated {2} files'.format(jeditaskid, scope, res))
                    elif msg_type == 'collection_stagein':
                        tmp_log.debug('jeditaskid={0}, scope={1}, update about datasets...'.format(jeditaskid, scope))
                        res = self.tbIF.updateInputDatasetsStagedAboutIdds_JEDI(jeditaskid, scope, name_dict)
                        if res is None:
                            # got error and rollback in dbproxy
                            err_str = 'jeditaskid={0}, scope={1}, failed to update datasets'.format(jeditaskid, scope)
                            raise RuntimeError(err_str)
                        tmp_log.info('jeditaskid={0}, scope={1}, updated {2} datasets'.format(jeditaskid, scope, res))
                    # check if all ok
                    if res == len(target_list):
                        tmp_log.debug('jeditaskid={0}, scope={1}, all OK'.format(jeditaskid, scope))
                    elif res < len(target_list):
                        tmp_log.warning('jeditaskid={0}, scope={1}, only {2} out of {3} done...'.format(
                                                                                jeditaskid, scope, res, len(target_list)))
                    elif res > len(target_list):
                        tmp_log.warning('jeditaskid={0}, scope={1}, strangely, {2} out of {3} done...'.format(
                                                                                jeditaskid, scope, res, len(target_list)))
                    else:
                        tmp_log.warning('jeditaskid={0}, scope={1}, something unwanted happened...'.format(
                                                                                jeditaskid, scope))
            else:
                # do nothing
                tmp_log.debug('jeditaskid={jeditaskid}, msg_type={msg_type}, relation_type={relation_type}, nothing done'.format(
                                jeditaskid=jeditaskid, msg_type=msg_type, relation_type=relation_type))
        except Exception as e:
            err_str = 'failed to process the message, skipped. {0} : {1}'.format(e.__class__.__name__, e)
            tmp_log.error(err_str)
            raise
        # done
        tmp_log.info('done')
