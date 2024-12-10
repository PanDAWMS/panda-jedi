import functools
import json
import random
import traceback
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper

logger = PandaLogger().getLogger(__name__.split(".")[-1])

# schema version of database config
DC_CONFIG_SCHEMA_VERSION = 0

# final task statuses
final_task_statuses = ["done", "finished", "failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"]


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


# ==============================================================
# Dataclasses of configurations #
# ===============================


@dataclass
class SourceTapeConfig:
    """
    Dataclass for source tape configuration parameters

    Fields:
        active                  (bool)  : whether the tape is active
        max_size                (int)   : maximum number of n_files_queued + nfiles_staging from this tape
        max_staging_ratio       (int)   : maximum ratio percent of nfiles_staging / (n_files_queued + nfiles_staging)
        destination_expression  (str)   : rse_expression for DDM to filter the destination RSE
    """

    active: bool = False
    max_size: int = 10000
    max_staging_ratio: int = 50
    destination_expression: str = "type=DATADISK&datapolicynucleus=True&freespace>300"


# Main config; must be at bottommost
@dataclass
class DataCarouselMainConfig:
    """
    Dataclass for DataCarousel main configuration parameters

    Fields:
        source_tapes_config     (dict)  : configurations of source tapes, in form of {"TAPE_1": SourceTapeConfig_of_TAPE_1, ...}
        excluded_destinations   (list)  : excluded destination RSEs
        early_access_users      (list)  : PanDA user names for early access to Data Carousel in PanDA/JEDI
    """

    source_tapes_config: Dict[str, Any] = field(default_factory=dict)
    excluded_destinations: List[str] = field(default_factory=list)
    early_access_users: List[str] = field(default_factory=list)

    def __post_init__(self):
        # for nested dict, convert value-dicts to corresponding dataclasses
        converting_attr_type_map = {
            "source_tapes_config": SourceTapeConfig,
        }
        for attr, klass in converting_attr_type_map.items():
            if isinstance(_map := getattr(self, attr, None), dict):
                converted_dict = {}
                for key, value in _map.items():
                    converted_dict[key] = klass(**value)
                setattr(self, attr, converted_dict)


# ==============================================================


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
        self.dc_config_map = {}
        self._last_update_ts_dict = {}
        # refresh
        self._refresh_all_attributes()

    def _refresh_all_attributes(self):
        """
        Refresh by calling all update methods
        """
        self._update_rses(time_limit_minutes=30)
        self._update_dc_config(time_limit_minutes=5)

    @staticmethod
    def refresh(func):
        """
        Decorator to call _refresh_all_attributes before the method
        """

        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            self._refresh_all_attributes()
            return func(self, *args, **kwargs)

        return wrapper

    def _update_rses(self, time_limit_minutes=30):
        """
        Update RSEs per type cached in this object
        """
        tmp_log = MsgWrapper(logger, "_update_rses")
        nickname = "rses"
        try:
            # check last update
            now_time = naive_utcnow()
            self._last_update_ts_dict.setdefault(nickname, None)
            last_update_ts = self._last_update_ts_dict[nickname]
            if last_update_ts is None or (now_time - last_update_ts) >= timedelta(minutes=time_limit_minutes):
                # get RSEs from DDM
                tape_rses = self.ddmIF.list_rses("rse_type=TAPE")
                if tape_rses is not None:
                    self.tape_rses = list(tape_rses)
                datadisk_rses = self.ddmIF.list_rses("type=DATADISK")
                if datadisk_rses is not None:
                    self.datadisk_rses = list(datadisk_rses)
                # tmp_log.debug(f"TAPE: {self.tape_rses} ; DATADISK: {self.datadisk_rses}")
                # tmp_log.debug(f"got {len(self.tape_rses)} tapes , {len(self.datadisk_rses)} datadisks")
                # update last update timestamp
                self._last_update_ts_dict[nickname] = naive_utcnow()
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")

    def _update_dc_config(self, time_limit_minutes=5):
        """
        Update Data Carousel configuration from DB
        """
        tmp_log = MsgWrapper(logger, "_update_dc_config")
        nickname = "main"
        try:
            # check last update
            now_time = naive_utcnow()
            self._last_update_ts_dict.setdefault(nickname, None)
            last_update_ts = self._last_update_ts_dict[nickname]
            if last_update_ts is None or (now_time - last_update_ts) >= timedelta(minutes=time_limit_minutes):
                # get configs from DB
                res_dict = self.taskBufferIF.getConfigValue("data_carousel", f"DATA_CAROUSEL_CONFIGS", "jedi", "atlas")
                if res_dict is None:
                    tmp_log.error(f"got None from DB ; skipped")
                    return
                # check schema version
                try:
                    schema_version = res_dict["metadata"]["schema_version"]
                except KeyError:
                    tmp_log.error(f"failed to get metadata.schema_version ; skipped")
                    return
                else:
                    if schema_version != DC_CONFIG_SCHEMA_VERSION:
                        tmp_log.error(f"metadata.schema_version does not match ({schema_version} != {DC_CONFIG_SCHEMA_VERSION}); skipped")
                        return
                # get config data
                dc_config_data_dict = res_dict.get("data")
                if dc_config_data_dict is None:
                    tmp_log.error(f"got empty config data; skipped")
                    return
                # update
                self.dc_config_map = DataCarouselMainConfig(**dc_config_data_dict)
                # update last update timestamp
                self._last_update_ts_dict[nickname] = naive_utcnow()
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")

    def _get_input_ds_from_task_params(self, task_params_map):
        """
        Get input datasets from tasks parameters
        """
        ret_map = {}
        for job_param in task_params_map.get("jobParameters", []):
            dataset = job_param.get("dataset")
            if dataset and job_param.get("param_type") in ["input", "pseudo_input"]:
                ret_map[dataset] = job_param
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

    def _get_filtered_replicas(self, dataset):
        """
        Get filtered replicas of a dataset and the staging rule and whether all replicas are without rules
        """
        replicas_map = self._get_full_replicas_per_type(dataset)
        rules = self.ddmIF.list_did_rules(dataset)
        rse_expression_list = []
        staging_rule = None
        for rule in rules:
            if rule["activity"] == "Staging":
                # rule of the dataset already exists; reuse it
                staging_rule = rule
            else:
                rse_expression_list.append(rule["rse_expression"])
        filtered_replicas_map = {"tape": [], "datadisk": []}
        has_datadisk_replica = len(replicas_map["datadisk"]) > 0
        for replica in replicas_map["tape"]:
            if replica in rse_expression_list:
                filtered_replicas_map["tape"].append(replica)
        if len(replicas_map["tape"]) >= 1 and len(filtered_replicas_map["tape"]) == 0 and len(rules) == 0:
            filtered_replicas_map["tape"] = replicas_map["tape"]
        for replica in replicas_map["datadisk"]:
            if staging_rule is not None or replica in rse_expression_list:
                filtered_replicas_map["datadisk"].append(replica)
        all_datadisk_replicas_without_rules = has_datadisk_replica and len(filtered_replicas_map["datadisk"]) == 0
        return filtered_replicas_map, staging_rule, all_datadisk_replicas_without_rules

    def _get_datasets_from_collection(self, collection: str) -> list[str] | None:
        """
        Get a list of datasets from DDM collection (container or dataset) in order to support inputs of container containing multiple datasets
        If the collection is a dataset, the method returns a list of the sole dataset
        If the collection is a container, the method returns a list of datasets inside the container

        Args:
        collection (str): name of the DDM collection (container or dataset)

        Returns:
            list[str] | None : list of datasets if successful; None if failed with exception
        """
        tmp_log = MsgWrapper(logger, f"_get_datasets_from_collections collection={collection}")
        # fill dc request spec for each input dataset
        try:
            ret_list = []
            collection_meta = self.ddmIF.getDatasetMetaData(collection)
            did_type = collection_meta["did_type"]
            if did_type == "CONTAINER":
                # is container, get datasets inside
                dataset_list = self.ddmIF.listDatasetsInContainer(collection)
                if dataset_list is None:
                    tmp_log.warning(f"cannot list datasets in this container")
                else:
                    ret_list = dataset_list
            elif did_type == "DATASET":
                # is dataset
                ret_list = [collection]
            else:
                tmp_log.warning(f"invalid DID type: {did_type}")
                return None
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        return ret_list

    @refresh
    def get_input_datasets_to_prestage(self, task_params_map):
        """
        Get the input datasets, their source RSEs (tape) of the task which need pre-staging from tapes, and DDM rule ID of existing DDM rule

        Args:
        task_params_map (dict): task params of the JEDI task

        Returns:
            list[tuple[str, str|None, str|None]]: list of tuples in the form of (dataset, source_rse, ddm_rule_id)
        """
        tmp_log = MsgWrapper(logger, "get_input_datasets_to_prestage")
        try:
            ret_list = []
            input_collection_map = self._get_input_ds_from_task_params(task_params_map)
            for collection in input_collection_map:
                dataset_list = self._get_datasets_from_collection(collection)
                if dataset_list is None:
                    tmp_log.warning(f"collection={collection} is None ; skipped")
                    return ret_list
                elif not dataset_list:
                    tmp_log.warning(f"collection={collection} is empty ; skipped")
                    return ret_list
                for dataset in dataset_list:
                    filtered_replicas_map, staging_rule, _ = self._get_filtered_replicas(dataset)
                    if rse_list := filtered_replicas_map["datadisk"]:
                        # replicas already on datadisk; skip
                        tmp_log.debug(f"dataset={dataset} already has replica on datadisks {rse_list} ; skipped")
                        continue
                    elif not filtered_replicas_map["tape"]:
                        # no replica on tape; skip
                        tmp_log.debug(f"dataset={dataset} has no replica on any tape ; skipped")
                        continue
                    else:
                        ddm_rule_id = None
                        # keep alive staging rule
                        if staging_rule and staging_rule["expires_at"] and (staging_rule["expires_at"] - naive_utcnow()) < timedelta(days=30):
                            ddm_rule_id = staging_rule["id"]
                            self._refresh_ddm_rule(ddm_rule_id, 86400 * 30)
                            tmp_log.debug(f"dataset={dataset} already has DDM rule ddm_rule_id={ddm_rule_id} ; refreshed it to be 30 days long")
                        # source RSE
                        rse_list = [replica for replica in filtered_replicas_map["tape"]]
                        source_rse = None
                        if len(rse_list) == 1:
                            source_rse = rse_list[0]
                        else:
                            non_CERN_rse_list = [rse for rse in rse_list if "CERN-PROD" not in rse]
                            if non_CERN_rse_list:
                                source_rse = random.choice(non_CERN_rse_list)
                            else:
                                source_rse = random.choice(rse_list)
                        # add to prestage
                        ret_list.append((dataset, source_rse, ddm_rule_id))
                        tmp_log.debug(f"dataset={dataset} chose source_rse={source_rse}")
            return ret_list
        except Exception as e:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            raise e

    def submit_data_carousel_requests(self, task_id: int, dataset_source_list: list[tuple[str, str | None, str | None]]) -> bool | None:
        """
        Submit data carousel requests for a task

        Args:
        task_id (int): JEDI task ID
        dataset_source_list (list[tuple[str, str|None, str|None]]): list of tuples in the form of (dataset, source_rse, ddm_rule_id)

        Returns:
            bool | None : True if submission successful, or None if failed
        """
        tmp_log = MsgWrapper(logger, "submit_data_carousel_requests")
        # fill dc request spec for each input dataset
        dc_req_spec_list = []
        now_time = naive_utcnow()
        for dataset, source_rse, ddm_rule_id in dataset_source_list:
            dc_req_spec = DataCarouselRequestSpec()
            dc_req_spec.dataset = dataset
            dataset_meta = self.ddmIF.getDatasetMetaData(dataset)
            dc_req_spec.total_files = dataset_meta["length"]
            dc_req_spec.dataset_size = dataset_meta["bytes"]
            dc_req_spec.staged_files = 0
            dc_req_spec.staged_size = 0
            dc_req_spec.ddm_rule_id = ddm_rule_id
            dc_req_spec.source_rse = source_rse
            dc_req_spec.status = DataCarouselRequestStatus.queued
            dc_req_spec.creation_time = now_time
            dc_req_spec_list.append(dc_req_spec)
        # insert dc requests for the task
        ret = self.taskBufferIF.insert_data_carousel_requests_JEDI(task_id, dc_req_spec_list)
        # return
        return ret

    def _e(self, task_id: int, dataset_source_list: list[tuple[str, str | None, str | None]]) -> bool | None:
        """
        Get stats of staging of all tapes

        Args:
        task_id (int): JEDI task ID
        dataset_source_list (list[tuple[str, str|None, str|None]]): list of tuples in the form of (dataset, source_rse, ddm_rule_id)

        Returns:
            bool | None : True if submission successful, or None if failed
        """
        tmp_log = MsgWrapper(logger, "submit_data_carousel_requests")
        # insert dc requests for the task
        pass
        # return
        # return ret

    @refresh
    def get_requests_to_stage(self, *args, **kwargs) -> list[DataCarouselRequestSpec]:
        """
        Get the queued requests which should proceed to get staging

        Args:
        ? (?): ?

        Returns:
            list[DataCarouselRequestSpec] : list of requests to stage
        """
        tmp_log = MsgWrapper(logger, "get_requests_to_stage")
        ret_list = []
        queued_requests = self.taskBufferIF.get_data_carousel_queued_requests_JEDI()
        if queued_requests is None:
            return ret_list
        for dc_req_spec, task_specs in queued_requests:
            # TODO: add algorithms to filter queued requests whether with existing DDM rule, and according to gshare, priority, etc. ; also limit length according to staging profiles
            # FIXME: currently all queued requests are returned
            ret_list.append(dc_req_spec)
        tmp_log.debug(f"got {len(ret_list)} requests")
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
        tmp_log = MsgWrapper(logger, f"_submit_ddm_rule request_id={dc_req_spec.request_id}")
        # TODO: configurable params to get from DC config
        expression = "type=DATADISK&datapolicynucleus=True&freespace>300"
        lifetime = None
        weight = None
        source_replica_expression = None
        if dc_req_spec.source_rse:
            source_replica_expression = f"type=DATADISK|{dc_req_spec.source_rse}"
        else:
            # no source_rse; unexpected
            tmp_log.warning(f"source_rse is None ; skipped")
            return
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

    @refresh
    def stage_request(self, dc_req_spec: DataCarouselRequestSpec) -> bool:
        """
        Stage the dataset of the request and update request status to staging

        Args:
        dc_req_spec (DataCarouselRequestSpec): spec of the request

        Returns:
            bool : True for success, False otherwise
        """
        tmp_log = MsgWrapper(logger, f"stage_request request_id={dc_req_spec.request_id}")
        is_ok = False
        # check existing DDM rule of the dataset
        if dc_req_spec.ddm_rule_id is not None:
            # DDM rule exists; no need to submit
            tmp_log.debug(f"dataset={dc_req_spec.dataset} already has active DDM rule ddm_rule_id={ddm_rule_id}")
        else:
            # no existing rule; submit DDM rule
            ddm_rule_id = self._submit_ddm_rule(dc_req_spec)
            now_time = naive_utcnow()
            if ddm_rule_id:
                # DDM rule submitted; update ddm_rule_id
                dc_req_spec.ddm_rule_id = ddm_rule_id
                tmp_log.debug(f"submitted DDM rule ddm_rule_id={ddm_rule_id}")
            else:
                # failed to submit
                tmp_log.warning(f"failed to submitted DDM rule ; skipped")
                return is_ok
        # update request to be staging
        dc_req_spec.status = DataCarouselRequestStatus.staging
        dc_req_spec.start_time = now_time
        ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
        if ret is not None:
            tmp_log.info(f"updated DB about staging; status={dc_req_spec.status}")
            dc_req_spec = ret
            is_ok = True
        # return
        return is_ok

    def _refresh_ddm_rule(self, rule_id: str, lifetime: int):
        """
        Refresh lifetime of the DDM rule

        Args:
        rule_id (str): DDM rule ID
        lifetime (int): lifetime in seconds to set

        Returns:
            bool : True for success, False otherwise
        """
        set_map = {"lifetime": lifetime}
        ret = self.ddmIF.update_rule_by_id(rule_id, set_map)
        return ret

    def keep_alive_ddm_rules(self):
        """
        Keep alive DDM rules of requests of active tasks
        """
        tmp_log = MsgWrapper(logger, "keep_alive_ddm_rules")
        # get requests and relations of active tasks
        ret_requests_map, ret_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_exclusion_list=final_task_statuses)
        for dc_req_spec in ret_requests_map.values():
            try:
                if dc_req_spec.status == DataCarouselRequestStatus.queued:
                    # skip requests queued
                    continue
                # get DDM rule
                ddm_rule_id = dc_req_spec.ddm_rule_id
                the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
                if the_rule is None:
                    # got error when getting the rule
                    tmp_log.error(f"request_id={dc_req_spec.request_id} cannot get rule of ddm_rule_id={ddm_rule_id}")
                    continue
                # rule lifetime
                rule_lifetime = None
                if the_rule["expires_at"]:
                    now_time = naive_utcnow()
                    rule_lifetime = now_time - the_rule["expires_at"]
                # trigger renewal when lifetime within the range
                if rule_lifetime is None or (rule_lifetime < timedelta(days=5) and rule_lifetime > timedelta(hours=2)):
                    days = None
                    if dc_req_spec.status == DataCarouselRequestStatus.staging:
                        # for requests staging
                        days = 15
                    elif dc_req_spec.status == DataCarouselRequestStatus.done:
                        # for requests done
                        days = 30
                    if days:
                        self._refresh_ddm_rule(ddm_rule_id, 86400 * days)
                        tmp_log.debug(
                            f"request_id={dc_req_spec.request_id} status={dc_req_spec.status} ddm_rule_id={ddm_rule_id} refreshed lifetime to be {days} days long"
                        )
                    else:
                        tmp_log.debug(f"request_id={dc_req_spec.request_id} status={dc_req_spec.status} ddm_rule_id={ddm_rule_id} not to renew ; skipped")
            except Exception:
                tmp_log.error(f"request_id={dc_req_spec.request_id} got error ; {traceback.format_exc()}")

    def check_staging_requests(self):
        """
        Check staging requests
        """
        tmp_log = MsgWrapper(logger, "check_staging_requests")
        dc_req_specs = self.taskBufferIF.get_data_carousel_staging_requests_JEDI()
        for dc_req_spec in dc_req_specs:
            try:
                to_update = False
                # get DDM rule
                ddm_rule_id = dc_req_spec.ddm_rule_id
                the_rule = self.ddmIF.get_rule_by_id(ddm_rule_id)
                if the_rule is None:
                    # got error when getting the rule
                    tmp_log.error(f"request_id={dc_req_spec.request_id} cannot get rule of ddm_rule_id={ddm_rule_id}")
                    continue
                # Destination RSE
                if dc_req_spec.destination_rse is None:
                    the_replica_locks = self.ddmIF.list_replica_locks_by_id(ddm_rule_id)
                    try:
                        the_first_file = the_replica_locks[0]
                    except IndexError:
                        tmp_log.warning(
                            f"request_id={dc_req_spec.request_id} no file from replica lock of ddm_rule_id={ddm_rule_id} ; destination_rse not updated"
                        )
                    except TypeError:
                        tmp_log.warning(
                            f"request_id={dc_req_spec.request_id} error listing replica lock of ddm_rule_id={ddm_rule_id} ; destination_rse not updated"
                        )
                    else:
                        # fill in destination RSE
                        destination_rse = the_first_file["rse"]
                        dc_req_spec.destination_rse = destination_rse
                        tmp_log.debug(f"request_id={dc_req_spec.request_id} filled destination_rse={destination_rse} of ddm_rule_id={ddm_rule_id}")
                        to_update = True
                # current staged files
                current_staged_files = int(the_rule["locks_ok_cnt"])
                if current_staged_files > dc_req_spec.staged_files:
                    # have more staged files than before; update request according to DDM rule
                    dc_req_spec.staged_files = current_staged_files
                    dc_req_spec.staged_size = int(dc_req_spec.dataset_size * dc_req_spec.staged_files / dc_req_spec.total_files)
                    to_update = True
                else:
                    tmp_log.debug(f"request_id={dc_req_spec.request_id} no new staged files about ddm_rule_id={ddm_rule_id}")
                # check completion of staging
                if dc_req_spec.staged_files == dc_req_spec.total_files:
                    # all files staged; process request to done
                    now_time = naive_utcnow()
                    dc_req_spec.status = DataCarouselRequestStatus.done
                    dc_req_spec.end_time = now_time
                    dc_req_spec.staged_size = dc_req_spec.dataset_size
                    to_update = True
                # update to DB if attribute updated
                if to_update:
                    ret = self.taskBufferIF.update_data_carousel_request_JEDI(dc_req_spec)
                    if ret is not None:
                        tmp_log.info(f"request_id={dc_req_spec.request_id} updated DB about staging ; status={dc_req_spec.status}")
                        dc_req_spec = ret
                    else:
                        tmp_log.error(f"request_id={dc_req_spec.request_id} failed to update DB for ddm_rule_id={ddm_rule_id} ; skipped")
                        continue
            except Exception:
                tmp_log.error(f"request_id={dc_req_spec.request_id} got error ; {traceback.format_exc()}")

    def _resume_task(self, task_id: int) -> bool:
        """
        Resume task from staging (to staged-pending)

        Args:
        task_id (int): JEDI task ID

        Returns:
            bool : True for success, False otherwise
        """
        tmp_log = MsgWrapper(logger, "_resume_task")
        # send resume command
        ret_val, ret_str = self.taskBufferIF.sendCommandTaskPanda(task_id, "Data Carousel. Resumed from staging", True, "resume", properErrorCode=True)
        # check if ok
        if ret_val == 0:
            return True
        else:
            tmp_log.warning(f"task_id={task_id} failed to resume the task: error_code={ret_val} {ret_str}")
            return False

    def resume_tasks_from_staging(self):
        """
        Get tasks with enough staged files and resume them
        """
        tmp_log = MsgWrapper(logger, "resume_tasks_from_staging")
        ret_requests_map, ret_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_filter_list=["staging"])
        for task_id, request_id_list in ret_relation_map.items():
            to_resume = False
            try:
                _, task_spec = self.taskBufferIF.getTaskWithID_JEDI(task_id, fullFlag=False)
                if not task_spec:
                    # task not found
                    tmp_log.error(f"task_id={task_id} task not found; skipped")
                    continue
                for request_id in request_id_list:
                    dc_req_spec = ret_requests_map[request_id]
                    if task_spec.taskType == "prod":
                        # condition for production tasks: resume if one file staged
                        if dc_req_spec.status == DataCarouselRequestStatus.done or (dc_req_spec.staged_files and dc_req_spec.staged_files > 0):
                            # got at least one data carousel request done for the task, to resume
                            to_resume = True
                            break
                    elif task_spec.taskType == "anal":
                        # condition for analysis tasks
                        # FIXME: temporary conservative condition for analysis tasks: resume if one dataset staged
                        if dc_req_spec.status == DataCarouselRequestStatus.done:
                            # got at least one entire dataset staged, to resume
                            to_resume = True
                            break
                if to_resume:
                    # resume the task
                    ret_val = self._resume_task(task_id)
                    if ret_val:
                        tmp_log.debug(f"task_id={task_id} resumed the task")
                    else:
                        tmp_log.warning(f"task_id={task_id} failed to resume the task; skipped")
            except Exception:
                tmp_log.error(f"task_id={task_id} got error ; {traceback.format_exc()}")

    def clean_up_requests(self, terminated_time_limit_days=3, outdated_time_limit_days=30):
        """
        Clean up terminated and outdated requests
        """
        tmp_log = MsgWrapper(logger, "clean_up_requests")
        try:
            # initialize
            terminated_requests_set = set()
            # get requests of terminated and active tasks
            terminated_tasks_requests_map, terminated_tasks_relation_map = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(
                status_filter_list=final_task_statuses
            )
            active_tasks_requests_map, _ = self.taskBufferIF.get_data_carousel_requests_by_task_status_JEDI(status_exclusion_list=final_task_statuses)
            now_time = naive_utcnow()
            # loop over terminated tasks
            for task_id, request_id_list in terminated_tasks_relation_map.items():
                for request_id in request_id_list:
                    if request_id in active_tasks_requests_map:
                        # the request is also mapped to some active task, not to be cleaned up; skipped
                        continue
                    dc_req_spec = terminated_tasks_requests_map[request_id]
                    if (
                        dc_req_spec.status in DataCarouselRequestStatus.final_statuses
                        and dc_req_spec.end_time
                        and dc_req_spec.end_time < now_time - timedelta(days=terminated_time_limit_days)
                    ):
                        # request terminated and old enough
                        terminated_requests_set.add(request_id)
            # delete ddm rules of terminate requests
            for request_id in terminated_requests_set:
                dc_req_spec = terminated_tasks_requests_map[request_id]
                ddm_rule_id = dc_req_spec.ddm_rule_id
                if ddm_rule_id:
                    try:
                        self.ddmIF.delete_replication_rule(ddm_rule_id)
                        tmp_log.debug(f"request_id={request_id} ddm_rule_id={ddm_rule_id} deleted DDM rule")
                    except Exception:
                        tmp_log.error(f"request_id={request_id} ddm_rule_id={ddm_rule_id} failed to delete DDM rule; {traceback.format_exc()}")
            # delete terminated requests
            if terminated_requests_set:
                ret_terminated = self.taskBufferIF.delete_data_carousel_requests_JEDI(list(terminated_requests_set))
                if ret_terminated is None:
                    tmp_log.warning(f"failed to delete terminated requests; skipped")
                else:
                    tmp_log.debug(f"deleted {ret_terminated} terminated requests older than {terminated_time_limit_days} days")
            else:
                tmp_log.debug(f"no terminated requests to delete; skipped")
            # clean up outdated requests
            ret_outdated = self.taskBufferIF.clean_up_data_carousel_requests_JEDI(time_limit_days=outdated_time_limit_days)
            if ret_outdated is None:
                tmp_log.warning(f"failed to delete outdated requests; skipped")
            else:
                tmp_log.debug(f"deleted {ret_outdated} outdated requests older than {outdated_time_limit_days} days")
        except Exception:
            tmp_log.error(f"got error ; {traceback.format_exc()}")
