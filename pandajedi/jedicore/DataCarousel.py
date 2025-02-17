import functools
import json
import random
import re
import traceback
from collections import namedtuple
from dataclasses import MISSING, InitVar, asdict, dataclass, field
from datetime import datetime, timedelta
from typing import Any, Dict, List

import polars as pl
from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandacommon.pandautils.base import SpecBase
from pandacommon.pandautils.PandaUtils import naive_utcnow

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# ==============================================================

# schema version of database config
DC_CONFIG_SCHEMA_VERSION = 1

# final task statuses
final_task_statuses = ["done", "finished", "failed", "exhausted", "aborted", "toabort", "aborting", "broken", "tobroken"]

# named tuple for attribute with type
AttributeWithType = namedtuple("AttributeWithType", ["attribute", "type"])

# template strings
src_repli_expr_prefix = "type=DATADISK"

# polars config
pl.Config.set_ascii_tables(True)
pl.Config.set_tbl_hide_dataframe_shape(True)
pl.Config.set_tbl_hide_column_data_types(True)
pl.Config.set_tbl_rows(-1)
pl.Config.set_tbl_cols(-1)
pl.Config.set_tbl_width_chars(120)

# ==============================================================


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

    # attributes with types
    attributes_with_types = (
        AttributeWithType("request_id", int),
        AttributeWithType("dataset", str),
        AttributeWithType("source_rse", str),
        AttributeWithType("destination_rse", str),
        AttributeWithType("ddm_rule_id", str),
        AttributeWithType("status", str),
        AttributeWithType("total_files", int),
        AttributeWithType("staged_files", int),
        AttributeWithType("dataset_size", int),
        AttributeWithType("staged_size", int),
        AttributeWithType("creation_time", datetime),
        AttributeWithType("start_time", datetime),
        AttributeWithType("end_time", datetime),
        AttributeWithType("modification_time", datetime),
        AttributeWithType("check_time", datetime),
        AttributeWithType("source_tape", str),
        AttributeWithType("parameters", str),
    )
    # attributes
    attributes = tuple([attr.attribute for attr in attributes_with_types])
    # attributes which have 0 by default
    _zeroAttrs = ()
    # attributes to force update
    _forceUpdateAttrs = ()
    # mapping between sequence and attr
    _seqAttrMap = {"request_id": f"{jedi_config.db.schemaJEDI}.JEDI_DATA_CAROUSEL_REQUEST_ID_SEQ.nextval"}

    @property
    def parameter_map(self) -> dict:
        """
        Get the dictionary parsed by the parameters attribute in JSON

        Returns:
            dict : dict of parameters if it is JSON or empty dict if null
        """
        if self.parameters is None:
            return {}
        else:
            return json.loads(self.parameters)

    @parameter_map.setter
    def parameter_map(self, value_map: dict):
        """
        Set the dictionary and store in parameters attribute in JSON

        Args:
            value_map (dict): dict to set the parameter map
        """
        self.parameters = json.dumps(value_map)

    def set_parameter(self, param: str, value):
        """
        Set the value of one parameter and store in parameters attribute in JSON

        Args:
            param (str): parameter name
            value (Any): value of the parameter to set; must be JSON-serializable
        """
        tmp_dict = self.parameter_map
        tmp_dict[param] = value
        self.parameter_map = tmp_dict


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


@dataclass
class SourceRSEConfig:
    """
    Dataclass for source RSE configuration parameters

    Fields:
        tape                    (str)       : is mapped to this source physical tape
        active                  (bool|None) : whether the source RSE is active
        max_size                (int|None)  : maximum number of n_files_queued + nfiles_staging from this RSE
        max_staging_ratio       (int|None)  : maximum ratio percent of nfiles_staging / (n_files_queued + nfiles_staging)
    """

    tape: str
    active: bool | None = None
    max_size: int | None = None
    max_staging_ratio: int | None = None


# Main config; must be bottommost of all config dataclasses
@dataclass
class DataCarouselMainConfig:
    """
    Dataclass for DataCarousel main configuration parameters

    Fields:
        source_tapes_config     (dict)  : configuration of source physical tapes, in form of {"TAPE_1": SourceTapeConfig_of_TAPE_1, ...}
        source_rses_config      (dict)  : configuration of source RSEs, each should be mapped to some source tape, in form of {"RSE_1": SourceRSEConfig_of_RSE_1, ...}
        excluded_destinations   (list)  : excluded destination RSEs
        early_access_users      (list)  : PanDA user names for early access to Data Carousel
    """

    source_tapes_config: Dict[str, Any] = field(default_factory=dict)
    source_rses_config: Dict[str, Any] = field(default_factory=dict)
    excluded_destinations: List[str] = field(default_factory=list)
    early_access_users: List[str] = field(default_factory=list)

    def __post_init__(self):
        # map of the attributes with nested dict and corresponding dataclasses
        converting_attr_type_map = {
            "source_tapes_config": SourceTapeConfig,
            "source_rses_config": SourceRSEConfig,
        }
        # convert the value-dicts of the attributes to corresponding dataclasses
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
        self.dc_config_map = None
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
                # get DC config from DB
                res_dict = self.taskBufferIF.getConfigValue("data_carousel", f"DATA_CAROUSEL_CONFIG", "jedi", "atlas")
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

    def _get_active_source_tapes(self) -> set[str] | None:
        """
        Get the set of active source physical tapes according to DC config

        Returns:
            set[str] | None : set of source tapes if successful; None if failed with exception
        """
        tmp_log = MsgWrapper(logger, f"_get_active_source_tapes")
        try:
            active_source_tapes_set = {tape for tape, tape_config in self.dc_config_map.source_tapes_config.items() if tape_config.active}
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        else:
            return active_source_tapes_set

    def _get_active_source_rses(self) -> set[str] | None:
        """
        Get the set of active source RSEs according to DC config

        Returns:
            set[str] | None : set of source RSEs if successful; None if failed with exception
        """
        tmp_log = MsgWrapper(logger, f"_get_active_source_tapes")
        try:
            active_source_tapes = self._get_active_source_tapes()
            active_source_rses_set = set()
            for rse, rse_config in self.dc_config_map.source_rses_config.items():
                try:
                    # both physical tape and RSE are active
                    if rse_config.tape in active_source_tapes and rse_config.active is not False:
                        active_source_rses_set.add(rse)
                except Exception:
                    # errors with the rse
                    tmp_log.error(f"got error with {rse} ; {traceback.format_exc()}")
                    continue
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return None
        else:
            return active_source_rses_set

    @refresh
    def get_input_datasets_to_prestage(self, task_id: int, task_params_map: dict) -> tuple[list | list]:
        """
        Get the input datasets, their source RSEs (tape) of the task which need pre-staging from tapes, and DDM rule ID of existing DDM rule

        Args:
            task_id (int): JEDI task ID of the task params
            task_params_map (dict): task params of the JEDI task

        Returns:
            list[tuple[str, str|None, str|None]]: list of tuples in the form of (dataset, source_rse, ddm_rule_id)
            list[str]: list of datasets which are already on datadisk (meant to be marked as no_staging)
        """
        tmp_log = MsgWrapper(logger, f"get_input_datasets_to_prestage task_id={task_id}")
        try:
            # initialize
            ret_prestaging_list = []
            ret_ds_on_disk_list = []
            # get active source rses
            active_source_rses_set = self._get_active_source_rses()
            # loop over inputs
            input_collection_map = self._get_input_ds_from_task_params(task_params_map)
            for collection in input_collection_map:
                dataset_list = self._get_datasets_from_collection(collection)
                if dataset_list is None:
                    tmp_log.warning(f"collection={collection} is None ; skipped")
                    return ret_prestaging_list, ret_ds_on_disk_list
                elif not dataset_list:
                    tmp_log.warning(f"collection={collection} is empty ; skipped")
                    return ret_prestaging_list, ret_ds_on_disk_list
                for dataset in dataset_list:
                    filtered_replicas_map, staging_rule, _ = self._get_filtered_replicas(dataset)
                    if rse_list := filtered_replicas_map["datadisk"]:
                        # replicas already on datadisk; skip
                        ret_ds_on_disk_list.append(dataset)
                        tmp_log.debug(f"dataset={dataset} already has replica on datadisks {rse_list} ; skipped")
                        continue
                    elif not filtered_replicas_map["tape"]:
                        # no replica on tape; skip
                        tmp_log.debug(f"dataset={dataset} has no replica on any tape ; skipped")
                        continue
                    else:
                        # initialize
                        ddm_rule_id = None
                        source_rse = None
                        # source tape RSEs from DDM
                        rse_set = {replica for replica in filtered_replicas_map["tape"]}
                        # filter out inactive source tape RSEs according to DC config
                        if active_source_rses_set is not None:
                            rse_set &= active_source_rses_set
                        # whether with existing staging rule
                        if staging_rule:
                            # with existing staging rule ; prepare to reuse it
                            ddm_rule_id = staging_rule["id"]
                            # extract source RSE from rule
                            source_replica_expression = staging_rule["source_replica_expression"]
                            for rse in rse_set:
                                # match tape rses with source_replica_expression
                                tmp_match = re.search(rse, source_replica_expression)
                                if tmp_match is not None:
                                    source_rse = rse
                                    break
                            if source_rse is None:
                                # direct regex search from source_replica_expression; reluctant as source_replica_expression can be messy
                                tmp_match = re.search(rf"{src_repli_expr_prefix}\|([A-Za-z0-9-_]+)", source_replica_expression)
                                if tmp_match is not None:
                                    source_rse = tmp_match.group(1)
                            if source_rse is None:
                                # still not getting source RSE from rule; unexpected
                                tmp_log.error(
                                    f"dataset={dataset} ddm_rule_id={ddm_rule_id} cannot get source_rse from source_replica_expression: {source_replica_expression}"
                                )
                            else:
                                tmp_log.debug(f"dataset={dataset} already staging with ddm_rule_id={ddm_rule_id} source_rse={source_rse}")
                            # keep alive the rule
                            if (rule_expiration_time := staging_rule["expires_at"]) and (rule_expiration_time - naive_utcnow()) < timedelta(days=30):
                                self._refresh_ddm_rule(ddm_rule_id, 86400 * 30)
                                tmp_log.debug(f"dataset={dataset} ddm_rule_id={ddm_rule_id} refreshed rule to be 30 days long")
                        else:
                            # no existing staging rule ; prepare info for new submission
                            rse_list = list(rse_set)
                            # choose source RSE
                            if len(rse_list) == 1:
                                source_rse = rse_list[0]
                            else:
                                non_CERN_rse_list = [rse for rse in rse_list if "CERN-PROD" not in rse]
                                if non_CERN_rse_list:
                                    source_rse = random.choice(non_CERN_rse_list)
                                else:
                                    source_rse = random.choice(rse_list)
                            tmp_log.debug(f"dataset={dataset} chose source_rse={source_rse}")
                        # add to prestage
                        ret_prestaging_list.append((dataset, source_rse, ddm_rule_id))
            return ret_prestaging_list, ret_ds_on_disk_list
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
            try:
                # source_rse is RSE
                source_tape = self.dc_config_map.source_rses_config[dc_req_spec.source_rse].tape
            except KeyError:
                # source_rse is physical tape
                source_tape = dc_req_spec.source_rse
            finally:
                dc_req_spec.source_tape = source_tape
            dc_req_spec.status = DataCarouselRequestStatus.queued
            dc_req_spec.creation_time = now_time
            if dc_req_spec.ddm_rule_id:
                # already with DDM rule; go to staging directly
                dc_req_spec.status = DataCarouselRequestStatus.staging
                dc_req_spec.start_time = now_time
                dc_req_spec.set_parameter("reuse_rule", True)
            dc_req_spec_list.append(dc_req_spec)
        # insert dc requests for the task
        ret = self.taskBufferIF.insert_data_carousel_requests_JEDI(task_id, dc_req_spec_list)
        # return
        return ret

    def _get_dc_requests_table_dataframe(self) -> pl.DataFrame | None:
        """
        Get Data Carousel requests table as dataframe for statistics

        Returns:
            polars.DataFrame|None : dataframe of current Data Carousel requests table if successful, or None if failed
        """
        sql = f"SELECT {','.join(DataCarouselRequestSpec.attributes)} " f"FROM {jedi_config.db.schemaJEDI}.data_carousel_requests " f"ORDER BY request_id "
        var_map = {}
        res = self.taskBufferIF.querySQL(sql, var_map, arraySize=99999)
        if res is not None:
            dc_req_df = pl.DataFrame(res, schema=DataCarouselRequestSpec.attributes_with_types, orient="row")
            return dc_req_df
        else:
            return None

    def _get_source_tapes_config_dataframe(self) -> pl.DataFrame:
        """
        Get source tapes config as dataframe

        Returns:
            polars.DataFrame : dataframe of source tapes config
        """
        tmp_list = []
        for k, v in self.dc_config_map.source_tapes_config.items():
            tmp_dict = {"source_tape": k}
            tmp_dict.update(asdict(v))
            tmp_list.append(tmp_dict)
        source_tapes_config_df = pl.DataFrame(tmp_list)
        return source_tapes_config_df

    def _get_source_rses_config_dataframe(self) -> pl.DataFrame:
        """
        Get source RSEs config as dataframe

        Returns:
            polars.DataFrame : dataframe of source RSEs config
        """
        tmp_list = []
        for k, v in self.dc_config_map.source_rses_config.items():
            tmp_dict = {"source_rse": k}
            tmp_dict.update(asdict(v))
            tmp_list.append(tmp_dict)
        source_rses_config_df = pl.DataFrame(tmp_list)
        return source_rses_config_df

    def _get_source_tape_stats_dataframe(self) -> pl.DataFrame | None:
        """
        Get statistics of source tapes as dataframe

        Returns:
            polars.DataFrame : dataframe of statistics of source tapes if successful, or None if failed
        """
        # get Data Carousel requests dataframe of staging requests
        dc_req_df = self._get_dc_requests_table_dataframe()
        if dc_req_df is None:
            return None
        dc_req_df = dc_req_df.filter(pl.col("status") == DataCarouselRequestStatus.staging)
        # get source tapes and RSEs config dataframes
        source_tapes_config_df = self._get_source_tapes_config_dataframe()
        # source_rses_config_df = self._get_source_rses_config_dataframe()
        # dataframe of staging requests with physical tapes
        # dc_req_full_df = dc_req_df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        dc_req_full_df = dc_req_df
        # dataframe of source RSE stats; add remaining_files
        source_rse_stats_df = (
            dc_req_full_df.select(
                "source_tape",
                "total_files",
                "staged_files",
                (pl.col("total_files") - pl.col("staged_files")).alias("remaining_files"),
            )
            .group_by("source_tape")
            .sum()
        )
        # make dataframe of source tapes stats; add quota_size
        df = source_tapes_config_df.join(source_rse_stats_df, on="source_tape", how="left")
        df = df.with_columns(
            pl.col("total_files").fill_null(strategy="zero"),
            pl.col("staged_files").fill_null(strategy="zero"),
            pl.col("remaining_files").fill_null(strategy="zero"),
        )
        df = df.with_columns(quota_size=(pl.col("max_size") - pl.col("remaining_files")))
        # return final dataframe
        source_tape_stats_df = df
        return source_tape_stats_df

    def _get_gshare_stats(self) -> dict:
        """
        Get current gshare stats

        Returns:
            dict : dictionary of gshares
        """
        # get share and hs info
        gshare_status = self.taskBufferIF.getGShareStatus()
        # initialize
        gshare_dict = dict()
        # rank and data
        for idx, leaf in enumerate(gshare_status):
            rank = idx + 1
            gshare = leaf["name"]
            gshare_dict[gshare] = {
                "gshare": gshare,
                "rank": rank,
                "queuing_hs": leaf["queuing"],
                "running_hs": leaf["running"],
                "target_hs": leaf["target"],
                "usage_perc": leaf["running"] / leaf["target"] if leaf["target"] > 0 else 999999,
                "queue_perc": leaf["queuing"] / leaf["target"] if leaf["target"] > 0 else 999999,
            }
        # return
        return gshare_dict

    def _queued_requests_tasks_to_dataframe(self, queued_requests: list | None) -> pl.DataFrame:
        """
        Transfrom Data Carousel queue requests and their tasks into dataframe

        Args:
            queued_requests (list|None): list of tuples in form of (queued_request, [taskspec1, taskspec2, ...])

        Returns:
            polars.DataFrame : dataframe of queued requests
        """
        # get source RSEs config for tape mapping
        # source_rses_config_df = self._get_source_rses_config_dataframe()
        # get current gshare rank
        gshare_dict = self._get_gshare_stats()
        gshare_rank_dict = {k: v["rank"] for k, v in gshare_dict.items()}
        # make dataframe of queued requests and their tasks
        tmp_list = []
        for dc_req_spec, task_specs in queued_requests:
            for task_spec in task_specs:
                tmp_dict = {
                    "request_id": dc_req_spec.request_id,
                    "dataset": dc_req_spec.dataset,
                    "source_rse": dc_req_spec.source_rse,
                    "source_tape": dc_req_spec.source_tape,
                    "total_files": dc_req_spec.total_files,
                    "dataset_size": dc_req_spec.dataset_size,
                    "jediTaskID": task_spec.jediTaskID,
                    "userName": task_spec.userName,
                    "gshare": task_spec.gshare,
                    "gshare_rank": gshare_rank_dict.get(task_spec.gshare, 999),
                    "task_priority": task_spec.currentPriority if task_spec.currentPriority else (task_spec.taskPriority if task_spec.taskPriority else 1000),
                }
                tmp_list.append(tmp_dict)
        df = pl.DataFrame(
            tmp_list,
            schema={
                "request_id": int,
                "dataset": str,
                "source_rse": str,
                "source_tape": str,
                "total_files": int,
                "dataset_size": int,
                "jediTaskID": int,
                "userName": str,
                "gshare": str,
                "gshare_rank": int,
                "task_priority": int,
            },
        )
        # fill null
        df.with_columns(pl.col("total_files").fill_null(strategy="zero"), pl.col("dataset_size").fill_null(strategy="zero"))
        # join to add phycial tape
        # df = df.join(source_rses_config_df.select("source_rse", "tape"), on="source_rse", how="left")
        # return final dataframe
        queued_requests_tasks_df = df
        return queued_requests_tasks_df

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
        # get stats of tapes
        source_tape_stats_df = self._get_source_tape_stats_dataframe()
        source_tape_stats_dict_list = source_tape_stats_df.to_dicts()
        # map of request_id and dc_req_spec of queued requests
        request_id_spec_map = {dc_req_spec.request_id: dc_req_spec for dc_req_spec, _ in queued_requests}
        # get dataframe of queued requests and tasks
        queued_requests_tasks_df = self._queued_requests_tasks_to_dataframe(queued_requests)
        # sort queued requests : by gshare_rank, task_priority, jediTaskID, request_id
        df = queued_requests_tasks_df.sort(
            ["gshare_rank", "task_priority", "jediTaskID", "request_id"], descending=[False, True, False, False], nulls_last=True
        )
        # get unique requests with the sorted order
        df = df.unique(subset=["request_id"], keep="first", maintain_order=True)
        # evaluate per tape
        queued_requests_df = df
        for source_tape_stats_dict in source_tape_stats_dict_list:
            source_tape = source_tape_stats_dict["source_tape"]
            quota_size = source_tape_stats_dict["quota_size"]
            # dataframe of the phycial tape
            tmp_df = queued_requests_df.filter(pl.col("source_tape") == source_tape)
            # get cumulative sum of queued files per physical tape
            tmp_df = tmp_df.with_columns(cum_total_files=pl.col("total_files").cum_sum(), cum_dataset_size=pl.col("dataset_size").cum_sum())
            # print dataframe in log
            if len(tmp_df):
                tmp_to_print_df = tmp_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare", "gshare_rank", "task_priority", "total_files", "cum_total_files"]
                )
                tmp_to_print_df = tmp_to_print_df.with_columns(gshare_and_rank=pl.concat_str([pl.col("gshare"), pl.col("gshare_rank")], separator=" : "))
                tmp_to_print_df = tmp_to_print_df.select(
                    ["request_id", "source_rse", "jediTaskID", "gshare_and_rank", "task_priority", "total_files", "cum_total_files"]
                )
                tmp_log.debug(f"  source_tape={source_tape} , quota_size={quota_size} : \n{tmp_to_print_df}")
            # filter requests within the tape quota size
            tmp_df = tmp_df.filter(pl.col("cum_total_files") <= quota_size)
            # append the requests to ret_list
            request_id_list = tmp_df.select(["request_id"]).to_dict(as_series=False)["request_id"]
            sub_count = 0
            for request_id in request_id_list:
                dc_req_spec = request_id_spec_map.get(request_id)
                if dc_req_spec:
                    ret_list.append(dc_req_spec)
                    sub_count += 1
            if sub_count > 0:
                tmp_log.debug(f"source_tape={source_tape} got {sub_count} requests")
        tmp_log.debug(f"totally got {len(ret_list)} requests")
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
        # initialize
        expression = None
        lifetime = None
        weight = None
        source_replica_expression = None
        # source replica expression
        if dc_req_spec.source_rse:
            source_replica_expression = f"{src_repli_expr_prefix}|{dc_req_spec.source_rse}"
        else:
            # no source_rse; unexpected
            tmp_log.warning(f"source_rse is None ; skipped")
            return
        # get source physical tape
        try:
            # source_rse is RSE
            source_tape = self.dc_config_map.source_rses_config[dc_req_spec.source_rse].tape
        except KeyError:
            # source_rse is physical tape
            source_tape = dc_req_spec.source_rse
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return
        # fill parameters about this tape source from DC config
        try:
            source_tape_config = self.dc_config_map.source_tapes_config[source_tape]
        except (KeyError, AttributeError):
            # no destination_expression for this tape; skipped
            tmp_log.warning(f"failed to get destination_expression from config; skipped ; {traceback.format_exc()}")
            return
        except Exception:
            # other unexpected errors
            tmp_log.error(f"got error ; {traceback.format_exc()}")
            return
        else:
            # destination_expression
            expression = source_tape_config.destination_expression
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
        if dc_req_specs is None:
            tmp_log.warning(f"failed to query requests to check ; skipped")
        elif not dc_req_specs:
            tmp_log.debug(f"got no requests to check ; skipped")
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
                new_staged_files = current_staged_files - dc_req_spec.staged_files
                if new_staged_files > 0:
                    # have more staged files than before; update request according to DDM rule
                    dc_req_spec.staged_files = current_staged_files
                    dc_req_spec.staged_size = int(dc_req_spec.dataset_size * dc_req_spec.staged_files / dc_req_spec.total_files)
                    to_update = True
                tmp_log.debug(f"request_id={dc_req_spec.request_id} got {new_staged_files} new staged files")
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
        n_resumed_tasks = 0
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
                    # if task_spec.taskType == "prod":
                    #     # condition for production tasks: resume if one file staged
                    #     if dc_req_spec.status == DataCarouselRequestStatus.done or (dc_req_spec.staged_files and dc_req_spec.staged_files > 0):
                    #         # got at least one data carousel request done for the task, to resume
                    #         to_resume = True
                    #         break
                    # elif task_spec.taskType == "anal":
                    #     # condition for analysis tasks
                    #     # FIXME: temporary conservative condition for analysis tasks: resume if one dataset staged
                    #     if dc_req_spec.status == DataCarouselRequestStatus.done:
                    #         # got at least one entire dataset staged, to resume
                    #         to_resume = True
                    #         break
                    # resume as soon as DDM rules are created
                    if dc_req_spec.ddm_rule_id and dc_req_spec.status in [DataCarouselRequestStatus.staging, DataCarouselRequestStatus.done]:
                        to_resume = True
                        break
                if to_resume:
                    # resume the task
                    ret_val = self._resume_task(task_id)
                    if ret_val:
                        n_resumed_tasks += 1
                        tmp_log.debug(f"task_id={task_id} resumed the task")
                    else:
                        tmp_log.warning(f"task_id={task_id} failed to resume the task; skipped")
            except Exception:
                tmp_log.error(f"task_id={task_id} got error ; {traceback.format_exc()}")
        # summary
        tmp_log.debug(f"resumed {n_resumed_tasks} tasks")

    def clean_up_requests(self, terminated_time_limit_days=15, outdated_time_limit_days=30):
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

    def cancel_request(self, request_id: int) -> bool | None:
        """
        Cancel a request

        Args:
            request_id (int): reqeust_id of the request to cancel

        Returns:
            bool|None : True for success, None otherwise
        """
        ret = self.taskBufferIF.cancel_or_resubmit_data_carousel_request_JEDI(request_id)
        return ret

    def resubmit_request(self, dc_req_spec: DataCarouselRequestSpec) -> bool | None:
        """
        Resubmit a request

        Args:
            dc_req_spec (DataCarouselRequestSpec): spec of the request to resubmit

        Returns:
            bool|None : True for success, None otherwise
        """
        now_time = naive_utcnow()
        # make new request spec
        dc_req_spec_to_resubmit = DataCarouselRequestSpec()
        dc_req_spec_to_resubmit.dataset = dc_req_spec.dataset
        dc_req_spec_to_resubmit.total_files = dc_req_spec.total_files
        dc_req_spec_to_resubmit.dataset_size = dc_req_spec.dataset_size
        dc_req_spec_to_resubmit.staged_files = 0
        dc_req_spec_to_resubmit.staged_size = 0
        dc_req_spec_to_resubmit.status = DataCarouselRequestStatus.queued
        dc_req_spec_to_resubmit.creation_time = now_time
        # TODO: mechanism to exclude problematic source or destination RSE (need approach to store historical datasets/RSEs)
        # return
        ret = self.taskBufferIF.cancel_or_resubmit_data_carousel_request_JEDI(dc_req_spec.request_id, dc_req_spec_to_resubmit)
        return ret
