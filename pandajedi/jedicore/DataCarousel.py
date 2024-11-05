from datetime import datetime, timedelta

from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config


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


class DataCarouselRequestSpec(SpecBase):
    """
    Data carousel request specification
    """

    # attributes
    attributes = (
        "request_id",
        "dataset",
        "source_rse",
        "destination_rse",
        "ddm_rule_id",
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
    # mapping between sequence and attr
    _seqAttrMap = {"request_id": f"{jedi_config.db.schemaJEDI}.JEDI_DATA_CAROUSEL_REQUEST_ID_SEQ.nextval"}


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
        if tape_rses is not None:
            self.tape_rses = list(tape_rses)
        datadisk_rses = self.ddmIF.list_rses("type=DATADISK")
        if datadisk_rses is not None:
            self.datadisk_rses = list(datadisk_rses)

    def _get_input_ds_from_task_params(self, task_params_map):
        """
        Get input datasets from tasks parameters
        """
        ret_map = {}
        for job_param in task_params_map.get("jobParameters", []):
            ds_name = job_param.get("dataset")
            if ds_name and job_param.get("param_type") in ["input", "pseudo_input"]:
                ret_map[ds_name] = job_param
        return ret_map

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
        input_dataset_map = self._get_input_ds_from_task_params(task_params_map)
        for ds_name in input_dataset_map:
            replicas_map = self._get_full_replicas_per_type(ds_name)
            if not replicas_map["datadisk"] and replicas_map["tape"]:
                # no replica on datadisk, but with replica on tape
                ret_list.append(ds_name)
        return ret_list

    def submit_data_carousel_requests(self, task_id: int, dataset_list: list[str], source_rse: str | None = None) -> bool | None:
        """
        Submit data carousel requests for a task

        Args:
        task_id (int): JEDI task ID
        dataset_list (list[str]): list of datasets
        source_rse (str | None): source RSE (optional)

        Returns:
            bool | None : True if submission successful, or None if failed
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
        ret = self.taskBufferIF.insert_data_carousel_requests_JEDI(task_id, dc_req_spec_list)
        # return
        return ret

    def get_requests_to_stage(self, *args, **kwargs) -> list[DataCarouselRequestSpec]:
        """
        Get the queued requests which should proceed to get staging

        Args:
        ? (?): ?

        Returns:
            list[DataCarouselRequestSpec] : list of requests to stage
        """
        ret_list = []
        queued_requests = self.taskBufferIF.get_data_carousel_queued_requests_JEDI()
        if queued_requests is None:
            return ret_list
        for dc_req_spec, task_specs in queued_requests:
            # TODO: add algorithms to filter queued requests according to gshare, priority, etc. ; also limit length according to staging profiles
            # FIXME: currently all queued requests are returned
            ret_list.append(dc_req_spec)
        # return
        return ret_list

    def _submit_ddm_rule(self, dc_req_spec: DataCarouselRequestSpec) -> str | None:
        """
        Submit DDM replication rule to stage the dataset of the request

        Args:
        dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            str | None : DDM rule_id of the new rule if submission successful, or None if failed
        """
        # TODO: configurable params to get from DC config
        expression = "type=DATADISK&datapolicynucleus=True&freespace>300"
        lifetime = None
        weight = None
        source_replica_expression = None
        # submit ddm staging rule
        ddm_rule_id = self.ddmIF.make_staging_rule(
            dataset_name=dc_req_spec.dataset,
            expression=expression,
            activity="Staging",
            lifetime=lifetime,
            weight=weight,
            notify="P",
            source_replica_expression=source_replica_expression,
        )
        # return
        return ddm_rule_id

    def stage_request(self, dc_req_spec: DataCarouselRequestSpec) -> bool:
        """
        Stage the dataset of the request and update request status to staging

        Args:
        dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            bool : True for success, False otherwise
        """
        is_ok = False
        # submit DDM rule
        ddm_rule_id = self._submit_ddm_rule(dc_req_spec)
        now_time = naive_utcnow()
        if ddm_rule_id:
            # DDM rule submitted; update request to be staging
            dc_req_spec.ddm_rule_id = ddm_rule_id
            dc_req_spec.status = DataCarouselRequestStatus.staging
            dc_req_spec.start_time = now_time
            ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
            if ret is not None:
                is_ok = True
        return is_ok

    def _refresh_ddm_rule(self, rule_id: str, lifetime: int):
        """
        refresh lifetime of the DDM rule

        Args:
        rule_id (str): DDM rule ID
        lifetime (int): lifetime in seconds to set
        """
        set_map = {"lifetime": lifetime}
        ret = self.ddmIF.update_rule_by_id(rule_id, set_map)
        return ret

    def keep_alive_ddm_rules(self):
        """
        keep alive DDM rules of requests of active tasks
        """
        dc_req_specs = self.taskBufferIF.get_data_carousel_requests_of_active_tasks_JEDI()
        for dc_req_spec in dc_req_specs:
            if dc_req_spec.status == DataCarouselRequestStatus.queued:
                # skip requests queued
                continue
            # get DDM rule
            ddm_rule_id = dc_req_spec.ddm_rule_id
            the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
            if the_rule is None:
                # got error when getting the rule
                continue
            # rule lifetime
            now_time = naive_utcnow()
            rule_lifetime = now_time - the_rule["expires_at"]
            # trigger renewal when lifetime within the range
            if rule_lifetime < timedelta(days=5) and rule_lifetime > timedelta(hours=2):
                if dc_req_spec.status == DataCarouselRequestStatus.staging:
                    # for requests staging
                    self._refresh_ddm_rule(ddm_rule_id, 86400 * 15)
                elif dc_req_spec.status == DataCarouselRequestStatus.done:
                    # for requests done
                    self._refresh_ddm_rule(ddm_rule_id, 86400 * 30)
