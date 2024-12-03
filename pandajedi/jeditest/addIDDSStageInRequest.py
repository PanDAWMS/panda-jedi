import sys

import idds.common.constants
import idds.common.utils
from idds.client.client import Client as iDDS_Client

from pandajedi.jedicore.DataCarousel import (
    DataCarouselRequestSpec,
    DataCarouselRequestStatus,
)
from pandajedi.jedicore.JediTaskBufferInterface import JediTaskBufferInterface

vo = "atlas"
task_id = int(sys.argv[1])
request_id = int(sys.argv[2])

print("set tbIF")
tbIF = JediTaskBufferInterface()
tbIF.setupInterface(max_size=1)

print("query DB for data carousel request")
sql = (
    f"SELECT req.* "
    f"FROM ATLAS_PANDA.data_carousel_requests req, ATLAS_PANDA.data_carousel_relations rel "
    f"WHERE req.request_id=rel.request_id "
    f"AND rel.request_id=:request_id AND rel.task_id=:task_id "
)
var_map = {
    ":task_id": task_id,
    ":request_id": request_id,
}
res_list = tbIF.querySQL(sql, var_map)

if res_list:
    for res in res_list:
        print("got request; submit iDDS stage-in request")
        # make request spec
        dc_req_spec = DataCarouselRequestSpec()
        dc_req_spec.pack(res)
        # dataset and rule_id
        dataset = dc_req_spec.dataset
        rule_id = dc_req_spec.ddm_rule_id
        ds_str_list = dataset.split(":")
        tmp_scope = ds_str_list[0]
        tmp_name = ds_str_list[1]
        # iDDS request
        c = iDDS_Client(idds.common.utils.get_rest_host())
        req = {
            "scope": tmp_scope,
            "name": tmp_name,
            "requester": "panda",
            "request_type": idds.common.constants.RequestType.StageIn,
            "transform_tag": idds.common.constants.RequestType.StageIn.value,
            "status": idds.common.constants.RequestStatus.New,
            "priority": 0,
            "lifetime": 30,
            "request_metadata": {
                "workload_id": task_id,
                "rule_id": rule_id,
            },
        }
        print(f"iDDS request: {req}")
        ret = c.add_request(**req)
        print(f"Done submit to iDDS: {ret}")
        break
else:
    print(f"Got no request: {res_list}")
    sys.exit(0)
