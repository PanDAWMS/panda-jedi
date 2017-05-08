"""
mapper to map task/job to a work queue

"""

from WorkQueue import WorkQueue, RESOURCE, ACTIVE_FUNCTIONS


class WorkQueueMapper:

    # constructor
    def __init__(self):
        self.workQueueMap = {}
        self.workQueueGlobalDic = {}

    def getSqlQuery(self):
        """
        Generates the SQL to get all work queues
        """
        sql = "SELECT {0} FROM ATLAS_PANDA.JEDI_Work_Queue".format(WorkQueue.column_names())

        return sql

    def makeMap(self, work_queues, global_leave_shares):
        """
        Creates the mapping with work queues and global shares
        :param work_queues: work queues
        :param global_leave_shares: global leave shares
        :return
        """

        self.workQueueMap = {}
        self.workQueueGlobalDic = {}

        # 1. add all workqueues to the map
        for wq in work_queues:
            # pack
            work_queue = WorkQueue()
            work_queue.pack(wq)

            # skip inactive queues
            if not work_queue.isActive():
                continue

            # add VO
            if work_queue.VO not in self.workQueueMap:
                self.workQueueMap[work_queue.VO] = {}

            # add type
            if not self.workQueueMap[work_queue.VO].has_key(work_queue.queue_type):
                self.workQueueMap[work_queue.VO][work_queue.queue_type] = []

            self.workQueueMap[work_queue.VO][work_queue.queue_type].append(work_queue)
            self.workQueueGlobalDic[work_queue.queue_name] = work_queue

        # sort the queue list by order
        for vo in self.workQueueMap:
            for type in self.workQueueMap[vo]:
                # make ordered map
                ordered_map = {}
                queue_map = self.workQueueMap[vo][type]
                for wq in queue_map:
                    if wq.queue_order not in ordered_map:
                        ordered_map[wq.queue_order] = []
                    # append
                    ordered_map[wq.queue_order].append(wq)
                # make sorted list
                ordered_list = ordered_map.keys()
                ordered_list.sort()
                new_list = []
                for order_val in ordered_list:
                    new_list += ordered_map[order_val]
                # set new list
                self.workQueueMap[vo][type] = new_list

        # 2. add all the global shares
        for gs in global_leave_shares:
            work_queue_gs = WorkQueue()
            work_queue_gs.pack_gs(gs)

            # TODO: add vo field in global shares
            if work_queue_gs.VO is None:
                vo = 'atlas'
            else:
                vo = work_queue_gs.VO

            if vo not in self.workQueueMap:
                self.workQueueMap[vo] = {}

            if not self.workQueueMap[vo].has_key(work_queue_gs.queue_type):
                self.workQueueMap[vo][work_queue_gs.queue_type] = []

            # for the moment we don't care about the order
            self.workQueueMap[vo][work_queue_gs.queue_type].append(work_queue_gs)

        # return
        return

    def dump(self):
        """
        Creates a human-friendly string showing the work queue mappings
        :return: string representation of the work queue mappings
        """
        dump_str = 'WorkQueue mapping\n'
        for VO in self.workQueueMap:
            dump_str += '  VO=%s\n' % VO
            for type in self.workQueueMap[VO]:
                dump_str += '    type=%s\n' % type
                for workQueue in self.workQueueMap[VO]:
                    dump_str += '    %s\n' % workQueue.dump()
        # return
        return dump_str

    def getQueueWithSelParams(self, vo, type, **param_map):
        """
        Get a work queue based on the selection parameters
        :param vo: vo
        :param type: type (in practice equivalent to prodsourcelabel)
        :param param_map: parameter selection map
        :return: work queue object and explanation in case no queue was found
        """
        ret_str = ''
        if vo not in self.workQueueMap:
            ret_str = 'queues for vo=%s are undefined' % vo
        elif not self.workQueueMap[vo].has_key(type):
            # check type
            ret_str = 'queues for type=%s are undefined in vo=%s' % (type, vo)
        else:
            for wq in self.workQueueMap[vo][type]:
                # evaluate
                try:
                    ret_queue, result = wq.evaluate(param_map)
                    if result:
                        return ret_queue, ret_str
                except:
                    ret_str += '{0},'.format(wq.queue_name)

            ret_str = ret_str[:-1]
            if ret_str != '':
                new_ret_str = 'eval with VO={0} '.format(vo)
                for tmp_param_key, tmp_param_val in param_map.iteritems():
                    new_ret_str += '{0}={1} failed for {0}'.format(tmp_param_key, tmp_param_val, ret_str)
                ret_str = new_ret_str

        # no queue matched to selection parameters
        return None, ret_str

    def getQueueByName(self, vo, type, queue_name):
        """
        # get queue by name
        :param queue_name: name of the queue
        :param vo: vo
        :param type: type
        :return: queue object or None if not found
        """
        if vo in self.workQueueMap and type in self.workQueueMap[vo]:
            for wq in self.workQueueMap[vo][type]:
                if wq.queue_name == queue_name:
                    return wq
        return None

    # get queue list with VO and type
    def getQueueListWithVoType(self, vo, queue_type):
        if self.workQueueMap.has_key(vo):
            if not queue_type in ['', None, 'any']:
                if self.workQueueMap[vo].has_key(queue_type):
                    return self.workQueueMap[vo][queue_type]
            else:
                # for any
                ret_list = []
                for tmp_type, tmp_list in self.workQueueMap[vo].iteritems():
                    ret_list += self.workQueueMap[vo][tmp_type]
                return ret_list
        # not found
        return []