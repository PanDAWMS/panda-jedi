import datetime
import json
import os
import re
import sys
import traceback
from typing import Any

import requests

try:
    import urllib3

    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
except Exception:
    pass

from pandacommon.pandalogger.PandaLogger import PandaLogger
from pandaserver.dataservice import DataServiceUtils, ddm
from rucio.client import Client as RucioClient
from rucio.common.exception import (
    DataIdentifierAlreadyExists,
    DataIdentifierNotFound,
    DuplicateContent,
    DuplicateRule,
    InvalidObject,
    UnsupportedOperation,
)

from pandajedi.jediconfig import jedi_config
from pandajedi.jedicore.MsgWrapper import MsgWrapper

from .DDMClientBase import DDMClientBase

logger = PandaLogger().getLogger(__name__.split(".")[-1])


# class to access to ATLAS DDM
class AtlasDDMClient(DDMClientBase):
    # constructor
    def __init__(self, con):
        # initialize base class
        DDMClientBase.__init__(self, con)
        # the list of fatal error
        self.fatalErrors = []
        # list of blacklisted endpoints
        self.blackListEndPoints = []
        self.bad_endpoint_read = []
        # time of last update for blacklist
        self.lastUpdateBL = None
        # how frequently update DN/token map
        self.timeIntervalBL = datetime.timedelta(seconds=60 * 10)
        # dict of endpoints
        self.endPointDict = {}
        # time of last update for endpoint dict
        self.lastUpdateEP = None
        # how frequently update endpoint dict
        self.timeIntervalEP = datetime.timedelta(seconds=60 * 10)
        # pid
        self.pid = os.getpid()

    # get files in dataset
    def getFilesInDataset(self, datasetName, getNumEvents=False, skipDuplicate=True, ignoreUnknown=False, longFormat=False, lfn_only=False):
        methodName = "getFilesInDataset"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get Rucio API
            client = RucioClient()
            # extract scope from dataset
            scope, dsn = self.extract_scope(datasetName)
            if dsn.endswith("/"):
                dsn = dsn[:-1]
            # get length
            tmpMeta = client.get_metadata(scope, dsn)
            # get files
            fileMap = {}
            baseLFNmap = {}
            fileSet = set()
            for x in client.list_files(scope, dsn, long=longFormat):
                # convert to old dict format
                lfn = str(x["name"])
                if lfn_only:
                    fileSet.add(lfn)
                    continue
                attrs = {}
                attrs["lfn"] = lfn
                attrs["chksum"] = "ad:" + str(x["adler32"])
                attrs["md5sum"] = attrs["chksum"]
                attrs["checksum"] = attrs["chksum"]
                attrs["fsize"] = x["bytes"]
                attrs["filesize"] = attrs["fsize"]
                attrs["scope"] = str(x["scope"])
                attrs["events"] = str(x["events"])
                if longFormat:
                    attrs["lumiblocknr"] = str(x["lumiblocknr"])
                guid = str(f"{x['guid'][0:8]}-{x['guid'][8:12]}-{x['guid'][12:16]}-{x['guid'][16:20]}-{x['guid'][20:32]}")
                attrs["guid"] = guid
                # skip duplicated files
                if skipDuplicate:
                    # extract base LFN and attempt number
                    baseLFN = re.sub("(\.(\d+))$", "", lfn)
                    attNr = re.sub(baseLFN + "\.*", "", lfn)
                    if attNr == "":
                        # without attempt number
                        attNr = -1
                    else:
                        attNr = int(attNr)
                    # compare attempt numbers
                    addMap = False
                    if baseLFN in baseLFNmap:
                        # use larger attempt number
                        oldMap = baseLFNmap[baseLFN]
                        if oldMap["attNr"] < attNr:
                            del fileMap[oldMap["guid"]]
                            addMap = True
                    else:
                        addMap = True
                    # append
                    if not addMap:
                        continue
                    baseLFNmap[baseLFN] = {"guid": guid, "attNr": attNr}
                fileMap[guid] = attrs
            if lfn_only:
                return_list = fileSet
            else:
                return_list = fileMap
            tmpLog.debug(f"done len={len(return_list)} meta={tmpMeta['length']}")
            if tmpMeta["length"] and tmpMeta["length"] > len(return_list):
                errMsg = f"file list length mismatch len={len(return_list)} != meta={tmpMeta['length']}"
                tmpLog.error(errMsg)
                return self.SC_FAILED, errMsg
            return self.SC_SUCCEEDED, return_list
        except DataIdentifierNotFound as e:
            if ignoreUnknown:
                return self.SC_SUCCEEDED, {}
            errType = e
        except Exception as e:
            errType = e
        errCode, errMsg = self.checkError(errType)
        tmpLog.error(errMsg)
        return errCode, f"{methodName} : {errMsg}"

    # list dataset replicas
    def listDatasetReplicas(self, datasetName, use_vp=False, detailed=False, skip_incomplete_element=False, use_deep=False, element_list=None):
        methodName = "listDatasetReplicas"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            if not datasetName.endswith("/"):
                # get file list
                tmpRet = self.convertOutListDatasetReplicas(datasetName, usefileLookup=use_deep, use_vp=use_vp)
                tmpLog.debug("got new " + str(tmpRet))
                if detailed:
                    return self.SC_SUCCEEDED, tmpRet, {datasetName: tmpRet}
                return self.SC_SUCCEEDED, tmpRet
            else:
                # list of attributes summed up
                retMap = {}
                detailedRetMap = {}
                # get constituent datasets
                if element_list:
                    dsList = ["{}:{}".format(*self.extract_scope(n)) for n in element_list]
                else:
                    tmpS, dsList = self.listDatasetsInContainer(datasetName)
                grandTotal = 0
                for tmpName in dsList:
                    tmpLog.debug(tmpName)
                    tmp_status, tmp_output = self.getDatasetMetaData(tmpName)
                    if tmp_status != self.SC_SUCCEEDED:
                        raise RuntimeError(f"failed to get metadata with {tmp_output}")
                    try:
                        totalFiles = tmp_output["length"]
                        if not totalFiles:
                            totalFiles = 0
                    except Exception:
                        totalFiles = 0
                    tmpRet = self.convertOutListDatasetReplicas(tmpName, usefileLookup=use_deep, use_vp=use_vp, skip_incomplete_element=skip_incomplete_element)
                    detailedRetMap[tmpName] = tmpRet
                    # loop over all sites
                    for tmpSite, tmpValMap in tmpRet.items():
                        # add site
                        retMap.setdefault(tmpSite, [{"found": 0}])
                        # sum
                        try:
                            retMap[tmpSite][-1]["found"] += int(tmpValMap[-1]["found"])
                        except Exception:
                            pass
                        # total
                        try:
                            if totalFiles < int(tmpValMap[-1]["total"]):
                                totalFiles = int(tmpValMap[-1]["total"])
                        except Exception:
                            pass
                    grandTotal += totalFiles
                # set total
                for tmpSite in retMap.keys():
                    retMap[tmpSite][-1]["total"] = grandTotal
                # return
                tmpLog.debug("got " + str(retMap))
                if detailed:
                    return self.SC_SUCCEEDED, retMap, detailedRetMap
                return self.SC_SUCCEEDED, retMap
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg + traceback.format_exc())
            if detailed:
                return errCode, f"{methodName} : {errMsg}", None
            return errCode, f"{methodName} : {errMsg}"

    # list replicas per dataset
    def listReplicasPerDataset(self, datasetName, deepScan=False):
        methodName = "listReplicasPerDataset"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start with deepScan={deepScan}")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            datasets = []
            if not datasetName.endswith("/"):
                datasets = [dsn]
            else:
                # get constituent datasets
                itr = client.list_content(scope, dsn)
                datasets = [i["name"] for i in itr]
            retMap = {}
            for tmpName in datasets:
                retMap[tmpName] = self.convertOutListDatasetReplicas(tmpName, deepScan)
                tmpLog.debug("got " + str(retMap))
            return self.SC_SUCCEEDED, retMap
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # get site property
    def getSiteProperty(self, seName, attribute):
        methodName = "getSiteProperty"
        methodName += f" pid={self.pid}"
        self.updateEndPointDict()
        try:
            retVal = self.endPointDict[seName][attribute]
            return self.SC_SUCCEEDED, retVal
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            return errCode, f"{methodName} : {errMsg}"

    # get site alternateName
    def getSiteAlternateName(self, se_name):
        self.updateEndPointDict()
        if se_name in self.endPointDict:
            return [self.endPointDict[se_name]["site"]]
        return None

    def SiteHasCompleteReplica(self, dataset_replica_map, endpoint, total_files_in_dataset):
        """
        Checks the #found files at site == #total files at site == #files in dataset. VP is regarded as complete
        :return: True or False
        """
        try:
            if "vp" in dataset_replica_map[endpoint][-1]:
                if dataset_replica_map[endpoint][-1]["vp"]:
                    return True
            found_tmp = dataset_replica_map[endpoint][-1]["found"]
            total_tmp = dataset_replica_map[endpoint][-1]["total"]
            if found_tmp is not None and total_tmp == found_tmp and total_tmp >= total_files_in_dataset:
                return True
        except KeyError:
            pass
        return False

    def getAvailableFiles(
        self,
        dataset_spec,
        site_endpoint_map,
        site_mapper,
        check_LFC=False,
        check_completeness=True,
        storage_token=None,
        complete_only=False,
        use_vp=True,
        file_scan_in_container=True,
        use_deep=False,
        element_list=None,
    ):
        """
        :param dataset_spec: dataset spec object
        :param site_endpoint_map: panda sites to ddm endpoints map. The list of panda sites includes the ones to scan
        :param site_mapper: site mapper object
        :param check_LFC: check/ask Tadashi/probably obsolete
        :param check_completeness:
        :param storage_token:
        :param complete_only: check only for complete replicas
        :param use_vp: use virtual placement
        :param file_scan_in_container: enable file lookup for container
        :param use_deep: use deep option for replica lookup
        :param element_list: interesting elements in dataset container

        TODO: do we need NG, do we need alternate names
        TODO: the storage_token is not used anymore
        :return:
        """
        # make logger
        method_name = "getAvailableFiles"
        method_name += f" pid={self.pid}"
        method_name += f" < jediTaskID={dataset_spec.jediTaskID} datasetID={dataset_spec.datasetID} >"
        tmp_log = MsgWrapper(logger, method_name)
        loopStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        try:
            tmp_log.debug(
                "start datasetName={} check_completeness={} nFiles={} nSites={} "
                "complete_only={}".format(dataset_spec.datasetName, check_completeness, len(dataset_spec.Files), len(site_endpoint_map), complete_only)
            )
            # update the definition of all endpoints from AGIS
            self.updateEndPointDict()

            # get the file map
            tmp_status, tmp_output = self.getDatasetMetaData(dataset_spec.datasetName)
            if tmp_status != self.SC_SUCCEEDED:
                regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
                tmp_log.error(f"failed in {regTime.seconds} sec to get metadata with {tmp_output}")
                return tmp_status, tmp_output
            total_files_in_dataset = tmp_output["length"]
            if total_files_in_dataset is None:
                total_files_in_dataset = 0
            if tmp_output["did_type"] == "CONTAINER":
                is_container = True
            else:
                is_container = False

            # get the dataset replica map
            tmp_status, tmp_output, detailed_replica_map = self.listDatasetReplicas(
                dataset_spec.datasetName, use_vp=use_vp, detailed=True, use_deep=use_deep, element_list=element_list
            )
            if tmp_status != self.SC_SUCCEEDED:
                regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
                tmp_log.error(f"failed in {regTime.seconds} sec to get dataset replicas with {tmp_output}")
                return tmp_status, tmp_output
            dataset_replica_map = tmp_output

            # collect GUIDs and LFNs
            file_map = {}  # GUID to LFN
            lfn_filespec_map = {}  # LFN to file spec
            scope_map = {}  # LFN to scope list
            for tmp_file in dataset_spec.Files:
                file_map[tmp_file.GUID] = tmp_file.lfn
                lfn_filespec_map.setdefault(tmp_file.lfn, [])
                lfn_filespec_map[tmp_file.lfn].append(tmp_file)
                scope_map[tmp_file.lfn] = tmp_file.scope

            complete_replica_map = {}
            endpoint_storagetype_map = {}
            rse_list = []

            # figure out complete replicas and storage types
            for site_name, endpoint_list in site_endpoint_map.items():
                tmp_site_spec = site_mapper.getSite(site_name)

                has_complete = False
                tmp_rse_list = []

                # loop over all endpoints
                for endpoint in endpoint_list:
                    # storage type
                    tmp_status, is_tape = self.getSiteProperty(endpoint, "is_tape")
                    if is_tape:
                        storage_type = "localtape"
                    else:
                        storage_type = "localdisk"

                    if (
                        self.SiteHasCompleteReplica(dataset_replica_map, endpoint, total_files_in_dataset)
                        or (endpoint in dataset_replica_map and not check_completeness)
                        or DataServiceUtils.isCachedFile(dataset_spec.datasetName, tmp_site_spec)
                    ):
                        complete_replica_map[endpoint] = storage_type
                        has_complete = True

                    # no scan for many-time datasets or disabled completeness check
                    if dataset_spec.isManyTime() or (not check_completeness and endpoint not in dataset_replica_map) or complete_only:
                        continue

                    # disable file lookup if unnecessary
                    if endpoint not in rse_list and (file_scan_in_container or not is_container):
                        tmp_rse_list.append(endpoint)

                    endpoint_storagetype_map[endpoint] = storage_type

                # add to list to trigger file lookup if complete replica is unavailable
                if not has_complete and tmp_rse_list:
                    rse_list += tmp_rse_list

            # get the file locations from Rucio
            if len(rse_list) > 0:
                tmp_log.debug(f"lookup file replicas in Rucio for RSEs: {rse_list}")
                tmp_status, rucio_lfn_to_rse_map = self.jedi_list_replicas(file_map, rse_list, scopes=scope_map)
                tmp_log.debug(f"lookup file replicas return status: {str(tmp_status)}")
                if tmp_status != self.SC_SUCCEEDED:
                    raise RuntimeError(rucio_lfn_to_rse_map)
            else:
                rucio_lfn_to_rse_map = dict()
                if not file_scan_in_container and is_container:
                    # remove incomplete
                    detailed_comp_replica_map = dict()
                    for tmp_ds_name, tmp_ds_value in detailed_replica_map.items():
                        new_map = {}
                        for tmp_k, tmp_v in tmp_ds_value.items():
                            if tmp_v[0]["total"] and tmp_v[0]["total"] == tmp_v[0]["found"]:
                                new_map[tmp_k] = tmp_v
                        if new_map:
                            detailed_comp_replica_map[tmp_ds_name] = new_map
                    # make file list from detailed replica map
                    files_in_container = {}
                    for tmp_ds_name in detailed_comp_replica_map.keys():
                        tmp_status, tmp_files = self.getFilesInDataset(tmp_ds_name, ignoreUnknown=True, lfn_only=True)
                        if tmp_status != self.SC_SUCCEEDED:
                            raise RuntimeError(tmp_files)
                        for tmp_lfn in tmp_files:
                            files_in_container[tmp_lfn] = tmp_ds_name
                    for tmp_file in dataset_spec.Files:
                        if tmp_file.lfn in files_in_container and files_in_container[tmp_file.lfn] in detailed_comp_replica_map:
                            rucio_lfn_to_rse_map[tmp_file.lfn] = detailed_comp_replica_map[files_in_container[tmp_file.lfn]]

            # initialize the return map and add complete/cached replicas
            return_map = {}
            checked_dst = set()
            for site_name, tmp_endpoints in site_endpoint_map.items():
                return_map.setdefault(site_name, {"localdisk": [], "localtape": [], "cache": [], "remote": []})
                tmp_site_spec = site_mapper.getSite(site_name)

                # check if the dataset is cached
                if DataServiceUtils.isCachedFile(dataset_spec.datasetName, tmp_site_spec):
                    # add to cached file list
                    return_map[site_name]["cache"] += dataset_spec.Files

                # complete replicas
                if not check_LFC:
                    for tmp_endpoint in tmp_endpoints:
                        if tmp_endpoint in complete_replica_map:
                            storage_type = complete_replica_map[tmp_endpoint]
                            return_map[site_name][storage_type] += dataset_spec.Files
                            checked_dst.add(site_name)

            # loop over all available LFNs
            available_lfns = sorted(rucio_lfn_to_rse_map.keys())
            for tmp_lfn in available_lfns:
                tmp_filespec_list = lfn_filespec_map[tmp_lfn]
                tmp_filespec = lfn_filespec_map[tmp_lfn][0]
                for site in site_endpoint_map:
                    for endpoint in site_endpoint_map[site]:
                        if endpoint in rucio_lfn_to_rse_map[tmp_lfn] and endpoint in endpoint_storagetype_map:
                            storage_type = endpoint_storagetype_map[endpoint]
                            if tmp_filespec not in return_map[site][storage_type]:
                                return_map[site][storage_type] += tmp_filespec_list
                            checked_dst.add(site)
                            break

            # aggregate all types of storage types into the 'all' key
            for site, storage_type_files in return_map.items():
                site_all_file_list = set()
                for storage_type, file_list in storage_type_files.items():
                    for tmp_file_spec in file_list:
                        site_all_file_list.add(tmp_file_spec)
                storage_type_files["all"] = site_all_file_list

            # dump for logging
            logging_str = ""
            for site, storage_type_file in return_map.items():
                logging_str += f"{site}:("
                for storage_type, file_list in storage_type_file.items():
                    logging_str += f"{storage_type}:{len(file_list)},"
                logging_str = logging_str[:-1]
                logging_str += ") "
            logging_str = logging_str[:-1]
            tmp_log.debug(logging_str)

            # return
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
            tmp_log.debug(f"done in {regTime.seconds} sec")
            return self.SC_SUCCEEDED, return_map
        except Exception as e:
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
            error_message = f"failed in {regTime.seconds} sec with {str(e)} {traceback.format_exc()} "
            tmp_log.error(error_message)
            return self.SC_FAILED, f"{self.__class__.__name__}.{method_name} {error_message}"

    def jedi_list_replicas(self, files, storages, scopes={}):
        try:
            method_name = "jedi_list_replicas"
            method_name += f" pid={self.pid}"
            tmp_log = MsgWrapper(logger, method_name)
            client = RucioClient()
            i_guid = 0
            max_guid = 1000  # do 1000 guids in each Rucio call
            lfn_to_rses_map = {}
            dids = []
            i_loop = 0
            startTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
            tmp_log.debug("start")
            for guid, lfn in files.items():
                i_guid += 1
                scope = scopes[lfn]
                dids.append({"scope": scope, "name": lfn})
                if len(dids) % max_guid == 0 or i_guid == len(files):
                    i_loop += 1
                    tmp_log.debug(f"lookup {i_loop} start")
                    loopStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    x = client.list_replicas(dids, resolve_archives=True)
                    regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
                    tmp_log.info(f"rucio.list_replicas took {regTime.seconds} sec for {len(dids)} files")
                    loopStart = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
                    for tmp_dict in x:
                        try:
                            tmp_LFN = str(tmp_dict["name"])
                            lfn_to_rses_map[tmp_LFN] = tmp_dict["rses"]
                        except Exception:
                            pass
                    # reset the dids list for the next bulk for Rucio
                    dids = []
                    regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - loopStart
                    tmp_log.debug(f"lookup {i_loop} end in {regTime.seconds} sec")
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - startTime
            tmp_log.debug(f"end in {regTime.seconds} sec")
        except Exception as e:
            regTime = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None) - startTime
            tmp_log.error(f"failed in {regTime.seconds} sec")
            return self.SC_FAILED, f"file lookup failed with {str(e)} {traceback.format_exc()}"

        return self.SC_SUCCEEDED, lfn_to_rses_map

    # list file replicas with dataset name/scope
    def jedi_list_replicas_with_dataset(self, datasetName):
        try:
            scope, dsn = self.extract_scope(datasetName)
            client = RucioClient()
            lfn_to_rses_map = {}
            dids = [{"scope": scope, "name": dsn}]
            for tmp_dict in client.list_replicas(dids, resolve_archives=True):
                try:
                    tmp_LFN = str(tmp_dict["name"])
                except Exception:
                    continue
                lfn_to_rses_map[tmp_LFN] = tmp_dict["rses"]
        except Exception:
            err_type, err_value = sys.exc_info()[:2]
            return self.SC_FAILED, f"file lookup failed with {err_type}:{err_value} {traceback.format_exc()}"
        return self.SC_SUCCEEDED, lfn_to_rses_map

    # get dataset metadata
    def getDatasetMetaData(self, datasetName, ignore_missing=False):
        # make logger
        methodName = "getDatasetMetaData"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # get metadata
            if dsn.endswith("/"):
                dsn = dsn[:-1]
            tmpRet = client.get_metadata(scope, dsn)
            # set state
            if tmpRet["is_open"] is True and tmpRet["did_type"] != "CONTAINER":
                tmpRet["state"] = "open"
            else:
                tmpRet["state"] = "closed"
            tmpLog.debug(str(tmpRet))
            return self.SC_SUCCEEDED, tmpRet
        except DataIdentifierNotFound as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            if ignore_missing:
                tmpLog.debug(errMsg)
                tmpRet = {}
                tmpRet["state"] = "missing"
                return self.SC_SUCCEEDED, tmpRet
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
        tmpLog.error(errMsg)
        return errCode, f"{methodName} : {errMsg}"

    # check error
    def checkError(self, errType):
        errMsg = f"{str(type(errType))} : {str(errType)}"
        if type(errType) in self.fatalErrors:
            # fatal error
            return self.SC_FATAL, errMsg
        else:
            # temporary error
            return self.SC_FAILED, errMsg

    # list dataset/container
    def listDatasets(self, datasetName, ignorePandaDS=True):
        methodName = "listDatasets"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            filters = {}
            if dsn.endswith("/"):
                dsn = dsn[:-1]
            filters["name"] = dsn
            dsList = set()
            for name in client.list_dids(scope, filters, "dataset"):
                dsList.add(f"{scope}:{name}")
            for name in client.list_dids(scope, filters, "container"):
                dsList.add(f"{scope}:{name}/")
            dsList = list(dsList)
            # ignore panda internal datasets
            if ignorePandaDS:
                tmpDsList = []
                for tmpDS in dsList:
                    if re.search("_dis\d+$", tmpDS) is not None or re.search("_sub\d+$", tmpDS):
                        continue
                    tmpDsList.append(tmpDS)
                dsList = tmpDsList
            tmpLog.debug("got " + str(dsList))
            return self.SC_SUCCEEDED, dsList
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # register new dataset/container
    def registerNewDataset(self, datasetName, backEnd="rucio", location=None, lifetime=None, metaData=None, resurrect=False):
        methodName = "registerNewDataset"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug(f"start location={location} lifetime={lifetime}")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # lifetime
            if lifetime is not None:
                lifetime = lifetime * 86400
            # register
            if not datasetName.endswith("/"):
                # register dataset
                name = dsn
                client.add_dataset(scope, name, meta=metaData, lifetime=lifetime, rse=location)
            else:
                # register container
                name = dsn
                client.add_container(scope=scope, name=name)
        except DataIdentifierAlreadyExists:
            pass
        except InvalidObject as e:
            errMsg = f"{InvalidObject} : {str(e)}"
            tmpLog.error(errMsg)
            return self.SC_FATAL, f"{methodName} : {errMsg}"
        except Exception as e:
            errType = e
            resurrected = False
            # try to resurrect
            if "DELETED_DIDS_PK violated" in str(errType) and resurrect:
                try:
                    client.resurrect([{"scope": scope, "name": name}])
                    resurrected = True
                except Exception:
                    pass
            if not resurrected:
                errCode, errMsg = self.checkError(errType)
                tmpLog.error(errMsg)
                return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # wrapper for list_content
    def wp_list_content(self, client, scope, dsn):
        if dsn.endswith("/"):
            dsn = dsn[:-1]
        retList = []
        # get contents
        for data in client.list_content(scope, dsn):
            if data["type"] == "CONTAINER":
                retList += self.wp_list_content(client, data["scope"], data["name"])
            elif data["type"] == "DATASET":
                retList.append(f"{data['scope']}:{data['name']}")
            else:
                pass
        return retList

    # list datasets in container
    def listDatasetsInContainer(self, containerName):
        methodName = "listDatasetsInContainer"
        methodName += f" pid={self.pid}"
        methodName += f" <containerName={containerName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get rucio
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(containerName)
            # get contents
            dsList = self.wp_list_content(client, scope, dsn)
            tmpLog.debug("got " + str(dsList))
            return self.SC_SUCCEEDED, dsList
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # expand Container
    def expandContainer(self, containerName):
        methodName = "expandContainer"
        methodName += f" pid={self.pid}"
        methodName += f" <contName={containerName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            dsList = []
            # get real names
            tmpS, tmpRealNameList = self.listDatasets(containerName)
            if tmpS != self.SC_SUCCEEDED:
                tmpLog.error("failed to get real names")
                return tmpS, tmpRealNameList
            # loop over all names
            for tmpRealName in tmpRealNameList:
                # container
                if tmpRealName.endswith("/"):
                    # get contents
                    tmpS, tmpO = self.listDatasetsInContainer(tmpRealName)
                    if tmpS != self.SC_SUCCEEDED:
                        tmpLog.error(f"failed to get datasets in {tmpRealName}")
                        return tmpS, tmpO
                else:
                    tmpO = [tmpRealName]
                # collect dataset names
                for tmpStr in tmpO:
                    if tmpStr not in dsList:
                        dsList.append(tmpStr)
            dsList.sort()
            # return
            tmpLog.debug(f"got {str(dsList)}")
            return self.SC_SUCCEEDED, dsList
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # add dataset to container
    def addDatasetsToContainer(self, containerName, datasetNames, backEnd="rucio"):
        methodName = "addDatasetsToContainer"
        methodName += f" pid={self.pid}"
        methodName += f" <contName={containerName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get Rucio API
            client = RucioClient()
            c_scope, c_name = self.extract_scope(containerName)
            if c_name.endswith("/"):
                c_name = c_name[:-1]
            dsns = []
            for ds in datasetNames:
                ds_scope, ds_name = self.extract_scope(ds)
                dsn = {"scope": ds_scope, "name": ds_name}
                dsns.append(dsn)
            try:
                # add datasets
                client.add_datasets_to_container(scope=c_scope, name=c_name, dsns=dsns)
            except DuplicateContent:
                # add datasets one by one
                for ds in dsns:
                    try:
                        client.add_datasets_to_container(scope=c_scope, name=c_name, dsns=[ds])
                    except DuplicateContent:
                        pass
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # get latest DBRelease
    def getLatestDBRelease(self):
        methodName = "getLatestDBRelease"
        methodName += f" pid={self.pid}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("trying to get the latest version number of DBR")
        # get ddo datasets
        tmpStat, ddoDatasets = self.listDatasets("ddo.*")
        if tmpStat != self.SC_SUCCEEDED or ddoDatasets == {}:
            tmpLog.error("failed to get a list of DBRelease datasets from DDM")
            return self.SC_FAILED, None
        # reverse sort to avoid redundant lookup
        ddoDatasets.sort()
        ddoDatasets.reverse()
        # extract version number
        latestVerMajor = 0
        latestVerMinor = 0
        latestVerBuild = 0
        latestVerRev = 0
        latestDBR = ""
        for tmpName in ddoDatasets:
            # ignore CDRelease
            if ".CDRelease." in tmpName:
                continue
            # ignore user
            if tmpName.startswith("ddo.user"):
                continue
            # use Atlas.Ideal
            if ".Atlas.Ideal." not in tmpName:
                continue
            match = re.search("\.v(\d+)(_*[^\.]*)$", tmpName)
            if match is None:
                tmpLog.warning(f"cannot extract version number from {tmpName}")
                continue
            # ignore special DBRs
            if match.group(2) != "":
                continue
            # get major,minor,build,revision numbers
            tmpVerStr = match.group(1)
            tmpVerMajor = 0
            tmpVerMinor = 0
            tmpVerBuild = 0
            tmpVerRev = 0
            try:
                tmpVerMajor = int(tmpVerStr[0:2])
            except Exception:
                pass
            try:
                tmpVerMinor = int(tmpVerStr[2:4])
            except Exception:
                pass
            try:
                tmpVerBuild = int(tmpVerStr[4:6])
            except Exception:
                pass
            try:
                tmpVerRev = int(tmpVerStr[6:])
                # use only three digit DBR
                continue
            except Exception:
                pass
            # compare
            if latestVerMajor > tmpVerMajor:
                continue
            elif latestVerMajor == tmpVerMajor:
                if latestVerMinor > tmpVerMinor:
                    continue
                elif latestVerMinor == tmpVerMinor:
                    if latestVerBuild > tmpVerBuild:
                        continue
                    elif latestVerBuild == tmpVerBuild:
                        if latestVerRev > tmpVerRev:
                            continue
            # check if well replicated
            tmpStat, ddoReplicas = self.listDatasetReplicas(tmpName)
            if ddoReplicas == []:
                continue
            # higher or equal version
            latestVerMajor = tmpVerMajor
            latestVerMinor = tmpVerMinor
            latestVerBuild = tmpVerBuild
            latestVerRev = tmpVerRev
            latestDBR = tmpName
        # failed
        if latestDBR == "":
            tmpLog.error("failed to get the latest version of DBRelease dataset from DDM")
            return self.SC_FAILED, None
        tmpLog.debug(f"use {latestDBR}")
        return self.SC_SUCCEEDED, latestDBR

    # freeze dataset
    def freezeDataset(self, datasetName, ignoreUnknown=False):
        methodName = "freezeDataset"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # check metadata to avoid a bug in rucio
            if dsn.endswith("/"):
                dsn = dsn[:-1]
            tmpRet = client.get_metadata(scope, dsn)
            # close
            client.set_status(scope, dsn, open=False)
        except UnsupportedOperation:
            pass
        except DataIdentifierNotFound as e:
            errType = e
            if ignoreUnknown:
                pass
            else:
                isOK = False
        except Exception as e:
            errType = e
            isOK = False
        if isOK:
            tmpLog.debug("done")
            return self.SC_SUCCEEDED, True
        else:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # finger
    def finger(self, dn):
        methodName = "finger"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} userName={dn}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        status, user_info = ddm.rucioAPI.finger(dn)
        if status:
            tmpLog.debug(f"done with {str(user_info)}")
            return self.SC_SUCCEEDED, user_info
        else:
            err_msg = f"failed with {str(user_info)}"
            tmpLog.error(err_msg)
            return self.SC_FAILED, err_msg

    # set dataset metadata
    def setDatasetMetadata(self, datasetName, metadataName, metadaValue):
        methodName = "setDatasetMetadata"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName} metadataName={metadataName} metadaValue={metadaValue}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # set
            client.set_metadata(scope, dsn, metadataName, metadaValue)
        except (UnsupportedOperation, DataIdentifierNotFound):
            pass
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # register location
    def registerDatasetLocation(
        self, datasetName, location, lifetime=None, owner=None, backEnd="rucio", activity=None, grouping=None, weight=None, copies=1, ignore_availability=True
    ):
        methodName = "registerDatasetLocation"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName} location={location}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # lifetime
            if lifetime is not None:
                lifetime = lifetime * 86400
            elif "SCRATCHDISK" in location:
                lifetime = 14 * 86400
            # get owner
            if owner is not None:
                tmpStat, userInfo = self.finger(owner)
                if tmpStat != self.SC_SUCCEEDED:
                    raise RuntimeError(f"failed to get nickname for {owner}")
                owner = userInfo["nickname"]
            else:
                owner = client.account
            if grouping is None:
                grouping = "DATASET"
            # add rule
            dids = []
            did = {"scope": scope, "name": dsn}
            dids.append(did)
            locList = location.split(",")
            for tmpLoc in locList:
                client.add_replication_rule(
                    dids=dids,
                    copies=copies,
                    rse_expression=tmpLoc,
                    lifetime=lifetime,
                    grouping=grouping,
                    account=owner,
                    locked=False,
                    notify="N",
                    ignore_availability=ignore_availability,
                    activity=activity,
                    weight=weight,
                )
        except DuplicateRule:
            pass
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug(f"done for owner={owner}")
        return self.SC_SUCCEEDED, True

    # delete dataset
    def deleteDataset(self, datasetName, emptyOnly, ignoreUnknown=False):
        methodName = "deleteDataset"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        retStr = ""
        nFiles = -1
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # get the number of files
            if emptyOnly:
                nFiles = 0
                for x in client.list_files(scope, dsn):
                    nFiles += 1
            # erase
            if not emptyOnly or nFiles == 0:
                client.set_metadata(scope=scope, name=dsn, key="lifetime", value=0.0001)
                retStr = f"deleted {datasetName}"
            else:
                retStr = f"keep {datasetName} where {nFiles} files are available"
        except DataIdentifierNotFound as e:
            errType = e
            if ignoreUnknown:
                pass
            else:
                isOK = False
        except Exception as e:
            isOK = False
            errType = e
        if isOK:
            tmpLog.debug("done")
            return self.SC_SUCCEEDED, retStr
        else:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"

    # register subscription
    def registerDatasetSubscription(self, datasetName, location, activity, lifetime=None, asynchronous=False):
        methodName = "registerDatasetSubscription"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName} location={location} activity={activity} asyn={asynchronous}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        try:
            if lifetime is not None:
                lifetime = lifetime * 24 * 60 * 60
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            dids = [{"scope": scope, "name": dsn}]
            # check if a replication rule already exists
            for rule in client.list_did_rules(scope=scope, name=dsn):
                if (rule["rse_expression"] == location) and (rule["account"] == client.account):
                    return True
            client.add_replication_rule(
                dids=dids,
                copies=1,
                rse_expression=location,
                weight=None,
                lifetime=lifetime,
                grouping="DATASET",
                account=client.account,
                locked=False,
                notify="N",
                ignore_availability=True,
                activity=activity,
                asynchronous=asynchronous,
            )
        except DuplicateRule:
            pass
        except DataIdentifierNotFound:
            pass
        except Exception as e:
            isOK = False
            errType = e
        if not isOK:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # convert output of listDatasetReplicas
    def convertOutListDatasetReplicas(self, datasetName, usefileLookup=False, use_vp=False, skip_incomplete_element=False):
        retMap = {}
        # get rucio API
        client = RucioClient()
        # get scope and name
        scope, dsn = self.extract_scope(datasetName)
        # get replicas
        itr = client.list_dataset_replicas(scope, dsn, deep=usefileLookup)
        items = []
        for item in itr:
            if "vp" not in item:
                item["vp"] = False
            items.append(item)
        # deep lookup if shallow gave nothing
        if items == [] and not usefileLookup:
            itr = client.list_dataset_replicas(scope, dsn, deep=True)
            for item in itr:
                if "vp" not in item:
                    item["vp"] = False
                items.append(item)
        # VP
        if use_vp:
            itr = client.list_dataset_replicas_vp(scope, dsn)
            for item in itr:
                if item["vp"]:
                    # add dummy
                    if "length" not in item:
                        item["length"] = 1
                    if "available_length" not in item:
                        item["available_length"] = 1
                    if "bytes" not in item:
                        item["bytes"] = 1
                    if "available_bytes" not in item:
                        item["available_bytes"] = 1
                    if "site" in item and "rse" not in item:
                        item["rse"] = item["site"]
                    items.append(item)
        # get blacklist
        bad_rse_list = set(self.get_bad_endpoint_read())
        # loop over all RSEs
        for item in items:
            rse = item["rse"]
            if skip_incomplete_element and (not item["available_length"] or item["length"] != item["available_length"]):
                continue
            retMap[rse] = [
                {
                    "total": item["length"],
                    "found": item["available_length"],
                    "tsize": item["bytes"],
                    "asize": item["available_bytes"],
                    "vp": item["vp"],
                    "immutable": 1,
                    "read_blacklisted": rse in bad_rse_list,
                }
            ]
        return retMap

    # delete files from dataset
    def deleteFilesFromDataset(self, datasetName, filesToDelete):
        methodName = "deleteFilesFromDataset"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # open dataset
            try:
                client.set_status(scope, dsn, open=True)
            except UnsupportedOperation:
                pass
            # exec
            client.detach_dids(scope=scope, name=dsn, dids=filesToDelete)
        except Exception as e:
            isOK = False
            errType = e
        if not isOK:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # extract scope
    def extract_scope(self, dsn):
        if dsn.endswith("/"):
            dsn = re.sub("/$", "", dsn)
        if ":" in dsn:
            return dsn.split(":")[:2]
        scope = dsn.split(".")[0]
        if dsn.startswith("user") or dsn.startswith("group"):
            scope = ".".join(dsn.split(".")[0:2])
        return scope, dsn

    # open dataset
    def openDataset(self, datasetName):
        methodName = "openDataset"
        methodName += f" pid={self.pid}"
        methodName += f" <datasetName={datasetName}>"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # open dataset
            try:
                client.set_status(scope, dsn, open=True)
            except (UnsupportedOperation, DataIdentifierNotFound):
                pass
        except Exception as e:
            isOK = False
            errType = e
        if not isOK:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # update blacklist
    def updateBlackList(self):
        methodName = "updateBlackList"
        methodName += f" pid={self.pid}"
        tmpLog = MsgWrapper(logger, methodName)
        # check freshness
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if self.lastUpdateBL is not None and timeNow - self.lastUpdateBL < self.timeIntervalBL:
            return
        self.lastUpdateBL = timeNow
        # get json
        try:
            tmpLog.debug("start")
            with open("/cvmfs/atlas.cern.ch/repo/sw/local/etc/cric_ddmblacklisting.json") as f:
                ddd = json.load(f)
                self.blackListEndPoints = [k for k in ddd if "write_wan" in ddd[k] and ddd[k]["write_wan"]["status"]["value"] == "OFF"]
                self.bad_endpoint_read = [k for k in ddd if "read_wan" in ddd[k] and ddd[k]["read_wan"]["status"]["value"] == "OFF"]
            tmpLog.debug(f"{len(self.blackListEndPoints)} bad endpoints for write, {len(self.bad_endpoint_read)} bad endpoints for read")
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        return

    # get bad endpoints for read
    def get_bad_endpoint_read(self):
        self.updateBlackList()
        return self.bad_endpoint_read

    # update endpoint dict
    def updateEndPointDict(self):
        methodName = "updateEndPointDict"
        methodName += f" pid={self.pid}"
        tmpLog = MsgWrapper(logger, methodName)
        # check freshness
        timeNow = datetime.datetime.now(datetime.timezone.utc).replace(tzinfo=None)
        if self.lastUpdateEP is not None and timeNow - self.lastUpdateEP < self.timeIntervalEP:
            return
        self.lastUpdateEP = timeNow
        # get json
        try:
            tmpLog.debug("start")
            if hasattr(jedi_config.ddm, "endpoints_json_path"):
                tmp_path = jedi_config.ddm.endpoints_json_path
            else:
                tmp_path = "/cvmfs/atlas.cern.ch/repo/sw/local/etc/cric_ddmendpoints.json"
            if tmp_path.startswith("http"):
                ddd = requests.get(tmp_path, verify=False).json()
            else:
                with open(tmp_path) as f:
                    ddd = json.load(f)
            self.endPointDict = {k: ddd[k] for k in ddd if ddd[k]["state"] == "ACTIVE"}
            tmpLog.debug(f"got {len(self.endPointDict)} endpoints ")
        except Exception as e:
            errStr = f"failed to update EP with {str(e)}"
            tmpLog.error(errStr)
        return

    # check if the dataset is distributed
    def isDistributedDataset(self, dataset_name: str) -> tuple[Any, bool | str]:
        """
        Check if the dataset/container is distributed
        :param dataset_name: dataset name
        :return: status, is_distributed (True if distributed, error message if failed)
        """
        method_name = "isDistributedDataset"
        method_name += f" pid={self.pid}"
        method_name += f" <datasetName={dataset_name}>"
        tmp_log = MsgWrapper(logger, method_name)
        tmp_log.debug("start")
        is_distributed = True
        is_ok = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(dataset_name)
            # get metadata
            if dsn.endswith("/"):
                dsn = dsn[:-1]
            tmp_ret = client.get_metadata(scope, dsn)
            did_type = tmp_ret["did_type"]
            # get rules
            for rule in client.list_did_rules(scope, dsn):
                if rule["grouping"] == "NONE":
                    continue
                elif rule["grouping"] == "DATASET" and did_type == "CONTAINER":
                    continue
                else:
                    is_distributed = False
                    break
        except Exception as e:
            is_ok = False
            err_type = e
        if not is_ok:
            err_code, err_msg = self.checkError(err_type)
            tmp_log.error(err_msg)
            return err_code, f"{method_name} : {err_msg}"
        tmp_log.debug(f"done with {is_distributed}")
        return self.SC_SUCCEEDED, is_distributed

    # update replication rules
    def updateReplicationRules(self, datasetName, dataMap):
        methodName = "updateReplicationRules"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={datasetName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(datasetName)
            # get rules
            for rule in client.list_did_rules(scope=scope, name=dsn):
                for dataKey, data in dataMap.items():
                    if rule["rse_expression"] == dataKey or re.search(dataKey, rule["rse_expression"]) is not None:
                        tmpLog.debug(f"set data={str(data)} on {rule['rse_expression']}")
                        client.update_replication_rule(rule["id"], data)
        except DataIdentifierNotFound:
            pass
        except Exception as e:
            isOK = False
            errType = e
        if not isOK:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, True

    # get active staging rule
    def getActiveStagingRule(self, dataset_name):
        methodName = "getActiveStagingRule"
        methodName += f" datasetName={dataset_name}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        ruleID = None
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(dataset_name)
            # get rules
            for rule in client.list_did_rules(scope=scope, name=dsn):
                if rule["activity"] == "Staging":
                    ruleID = rule["id"]
                    break
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug(f"got ruleID={ruleID}")
        return self.SC_SUCCEEDED, ruleID

    # check quota
    def check_quota(self, userName):
        methodName = "check_quota"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} userName={userName}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        retVal = True, None
        try:
            # get rucio API
            client = RucioClient()
            tmpStat, user_info = self.finger(userName)
            if tmpStat != self.SC_SUCCEEDED:
                retVal = False, "failed to get nickname"
            else:
                owner = user_info["nickname"]
                quota_info = client.get_global_account_usage(owner)
                for info in quota_info:
                    if info["bytes"] >= info["bytes_limit"]:
                        retVal = False, f"exceeded quota on {info['rse_expression']}"
                        break
        except Exception as e:
            errMsg = f"failed to get quota info with {str(e)}"
            tmpLog.error(errMsg)
            retVal = False, errMsg
        tmpLog.debug("done {} {}".format(*retVal))
        return self.SC_SUCCEEDED, retVal

    # make staging rule
    def make_staging_rule(self, dataset_name, expression, activity, lifetime=None):
        methodName = "make_staging_rule"
        methodName += f" pid={self.pid}"
        methodName = f"{methodName} datasetName={dataset_name} expression={expression} activity={activity} lifetime={lifetime}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        isOK = True
        ruleID = None
        try:
            if lifetime is not None:
                lifetime = lifetime * 24 * 60 * 60
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(dataset_name)
            # check if a replication rule already exists
            if ruleID is None:
                dids = [{"scope": scope, "name": dsn}]
                for rule in client.list_did_rules(scope=scope, name=dsn):
                    if rule["rse_expression"] == expression and rule["account"] == client.account and rule["activity"] == activity:
                        ruleID = rule["id"]
                        tmpLog.debug(f"rule already exists: ID={ruleID}")
                        break
            # make new rule
            if ruleID is None:
                rule = client.add_replication_rule(
                    dids=dids,
                    copies=1,
                    rse_expression=expression,
                    weight=None,
                    lifetime=lifetime,
                    grouping="DATASET",
                    account=client.account,
                    locked=False,
                    notify="N",
                    ignore_availability=True,
                    activity=activity,
                    asynchronous=False,
                )
                ruleID = rule["id"]
                tmpLog.debug(f"made new rule : ID={ruleID}")
        except Exception as e:
            isOK = False
            errType = e
        if not isOK:
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug("done")
        return self.SC_SUCCEEDED, ruleID

    # get state of all rules of a dataset
    def get_rules_state(self, dataset_name):
        methodName = "get_rules_state"
        methodName += f" datasetName={dataset_name}"
        tmpLog = MsgWrapper(logger, methodName)
        tmpLog.debug("start")
        res_dict = {}
        all_ok = False
        try:
            # get rucio API
            client = RucioClient()
            # get scope and name
            scope, dsn = self.extract_scope(dataset_name)
            # get rules and state
            for rule in client.list_did_rules(scope=scope, name=dsn):
                rule_id = rule["id"]
                state = rule["state"]
                res_dict[rule_id] = state
            # if all ok
            if all((x == "OK" for x in res_dict.values())):
                all_ok = True
        except Exception as e:
            errType = e
            errCode, errMsg = self.checkError(errType)
            tmpLog.error(errMsg)
            return errCode, f"{methodName} : {errMsg}"
        tmpLog.debug(f"got {all_ok}, {res_dict}")
        return self.SC_SUCCEEDED, (all_ok, res_dict)
