from pandacommon.pandautils.PandaUtils import naive_utcnow


class DataCarouselRequestStatus(object):
    """
    Data carousel request status
    """

    queued = "queued"
    staging = "staging"
    done = "done"
    cancelled = "cancelled"

    active_statuses = [queued, staging]
    final_statuses = [done, cancelled]


class DataCarouselRequestSpec(object):
    """
    Data carousel request specification
    """

    # attributes
    attributes = (
        "request_id",
        "dataset",
        "source_rse",
        "destination_rse",
        "rucio_rule_id",
        "status",
        "total_files",
        "staged_files",
        "dataset_size",
        "staged_size",
        "creation_time",
        "start_time",
        "end_time",
        "modification_time",
        "check_time",
    )
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()

    # constructor
    def __init__(self):
        # install attributes
        for attr in self._attributes:
            object.__setattr__(self, attr, None)
        # file list
        object.__setattr__(self, "Files", [])
        # map of changed attributes
        object.__setattr__(self, "_changedAttrs", {})
        # distributed
        object.__setattr__(self, "distributed", False)

    # override __setattr__ to collect the changed attributes
    def __setattr__(self, name, value):
        oldVal = getattr(self, name)
        object.__setattr__(self, name, value)
        newVal = getattr(self, name)
        # collect changed attributes
        if oldVal != newVal or name in self._forceUpdateAttrs:
            self._changedAttrs[name] = value

    # reset changed attribute list
    def resetChangedList(self):
        object.__setattr__(self, "_changedAttrs", {})

    # force update
    def forceUpdate(self, name):
        if name in self._attributes:
            self._changedAttrs[name] = getattr(self, name)

    # return map of values
    def valuesMap(self, onlyChanged=False):
        ret = {}
        for attr in self._attributes:
            # only changed attributes
            if onlyChanged:
                if attr not in self._changedAttrs:
                    continue
            val = getattr(self, attr)
            if val is None:
                if attr in self._zeroAttrs:
                    val = 0
                else:
                    val = None
            ret[f":{attr}"] = val
        return ret

    # pack tuple into spec
    def pack(self, values):
        for i in range(len(self._attributes)):
            attr = self._attributes[i]
            val = values[i]
            object.__setattr__(self, attr, val)

    # return column names for INSERT
    @classmethod
    def columnNames(cls, prefix=None):
        ret = ""
        for attr in cls._attributes:
            if prefix is not None:
                ret += f"{prefix}."
            ret += f"{attr},"
        ret = ret[:-1]
        return ret

    # return expression of bind variables for INSERT
    @classmethod
    def bindValuesExpression(cls):
        ret = "VALUES("
        for attr in cls._attributes:
            ret += f":{attr},"
        ret = ret[:-1]
        ret += ")"
        return ret

    # return an expression of bind variables for UPDATE to update only changed attributes
    def bindUpdateChangesExpression(self):
        ret = ""
        for attr in self._attributes:
            if attr in self._changedAttrs:
                ret += f"{attr}=:{attr},"
        ret = ret[:-1]
        ret += " "
        return ret

    # set dataset attribute
    def setDatasetAttribute(self, attr):
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr

    # set dataset attribute with label
    def setDatasetAttributeWithLabel(self, label):
        if label not in self.attrToken:
            return
        attr = self.attrToken[label]
        if self.attributes is None:
            self.attributes = ""
        else:
            self.attributes += ","
        self.attributes += attr


class DataCarouselInterface(object):
    """
    Interface for data carousel methods
    """

    # constructor
    def __init__(self, taskbufferIF, ddmIF):
        # attributes
        self.taskBufferIF = taskbufferIF
        self.ddmIF = ddmIF
        self.tape_rses = []
        self.datadisk_rses = []
        # update
        self._update_rses()

    def _update_rses(self):
        """
        Update RSEs per type cached in this object
        """
        tape_rses = self.ddmIF.list_rses("rse_type=TAPE")
        if isinstance(tape_rses, list):
            self.tape_rses = tape_rses
        datadisk_rses = self.ddmIF.list_rses("type=DATADISK")
        if isinstance(datadisk_rses, list):
            self.datadisk_rses = datadisk_rses

    def _get_input_ds_from_task_params(self, task_params_map):
        """
        Get input datasets from tasks parameters
        """
        ret_list = []
        for job_param in task_params_map.get("jobParameters", []):
            ds_name = job_param.get("dataset")
            if ds_name and job_param.get("param_type") in ["input", "pseudo_input"]:
                ret_list.append({ds_name: job_param})
        return ret_list

    def _get_full_replicas_per_type(self, dataset):
        """
        Get full replicas per type of a dataset
        """
        ds_repli_dict = self.ddmIF.convertOutListDatasetReplicas(dataset, skip_incomplete_element=True)
        tape_replicas = []
        datadisk_replicas = []
        for rse in ds_repli_dict:
            if rse in self.tape_rses:
                tape_replicas.append(rse)
            if rse in self.datadisk_rses:
                datadisk_replicas.append(rse)
        return {"tape": tape_replicas, "datadisk": datadisk_replicas}

    def get_input_datasets_to_prestage(self, task_params_map):
        """
        Get the input datasets of the task which need pre-staging from tapes
        """
        ret_list = []
        input_datasets_map = self._get_input_ds_from_task_params(task_params_map)
        for ds_name in input_datasets_map:
            replicas_map = self._get_full_replicas_per_type(ds_name)
            if not replicas_map["datadisk"] and replicas_map["tape"]:
                # no replica on datadisk, but with replica on tape
                ret_list.append(ds_name)
        return ret_list

    def submit_data_carousel_requests(self, task_id, dataset_list, source_rse=None):
        """
        Submit data carousel requests for a task
        """
        # fill dc request spec for each input dataset
        dc_req_spec_list = []
        now_time = naive_utcnow()
        for ds_name in dataset_list:
            dc_req_spec = DataCarouselRequestSpec()
            dc_req_spec.dataset = ds_name
            dataset_meta = self.ddmIF.getDatasetMetaData(ds_name)
            dc_req_spec.total_files = dataset_meta["length"]
            dc_req_spec.dataset_size = dataset_meta["bytes"]
            dc_req_spec.staged_files = 0
            dc_req_spec.staged_size = 0
            dc_req_spec.status = DataCarouselRequestStatus.queued
            if source_rse:
                dc_req_spec.source_rse = source_rse
            dc_req_spec.creation_time = now_time
            dc_req_spec_list.append(dc_req_spec)
        # insert dc requests for the task
        ret = self.taskBufferIF.insert_data_carousel_requests_JEDI(self, task_id, dc_req_spec_list)
        # return
        return ret
