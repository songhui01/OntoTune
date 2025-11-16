import socketserver
import json
import struct
import sys
import time
import os
import storage
import storage2
import model
import train
import math
import reg_blocker
from constants import (PG_OPTIMIZER_INDEX, DEFAULT_MODEL_PATH,
                       OLD_MODEL_PATH, TMP_MODEL_PATH)

from general import setup_logging
import logging
import numpy as np
import math, json

from featurize import augment_meta_from_plan
from onto_utils_template import template_from_plan_meta
from choose_arm import choose_arm

from sqlglot_parse import parse_sqlglot, enrich_sql_semantics
from featurize_sqlglot_bridge import merge_parsed_sqlglot_into_meta, merge_semantics_into_meta


def add_buffer_info_to_plans(buffer_info, plans):
    for p in plans:
        p["Buffers"] = buffer_info
    return plans
def add_meta_info_to_plans(metadata, plans):
    for p in plans:
        p["metadata"] = metadata
    return plans

class OntoModel:
    def __init__(self):
        self.__current_model = None
        self.logger = logging.getLogger(__name__)

    def select_plan(self, messages):
        start = time.time()
        *arms, buffers, metadata  = messages
        if self.__current_model is None:
            print("__current_model is none.")
            print("PG_OPTIMIZER_INDEX: ", PG_OPTIMIZER_INDEX)
            return PG_OPTIMIZER_INDEX

        arms = add_buffer_info_to_plans(buffers, arms)
        arms = add_meta_info_to_plans(metadata, arms)

        plan_root = arms[0].get("Plan", arms[0]) 
        meta0 = dict(arms[0].get("metadata", {})) 

        key_dbg = {}
        try:
            meta_aug = augment_meta_from_plan(plan_root, meta0)
            for i in range(len(arms)):
                m = dict(arms[i].get("metadata", {}))
                m.update(meta_aug)
                arms[i]["metadata"] = m
        except Exception as e:
            self.logger.warning("[AUGMENT] failed: %s", e)
            meta_aug = meta0

        res = self.__current_model.predict(arms)

        try:
            template_id = template_from_plan_meta(arms[0], meta_aug)
            key_dbg = {
                "has_distinct": bool(meta_aug.get("has_distinct", False)),
                "has_exists": bool(meta_aug.get("has_exists", False)),
                "has_not_exists": bool(meta_aug.get("has_not_exists", False)),
                "has_non_equi_pred": bool(meta_aug.get("has_non_equi_pred", False)),
                "need_sort_for_merge": bool(meta_aug.get("need_sort_for_merge", False)),
                "post_link_present": bool(meta_aug.get("post_link_present", False)),
                "post_link_occurs_2plus": bool(meta_aug.get("post_link_occurs_2plus", False)),
                "rows_bucket": int(meta_aug.get("rows_bucket", 0)),
                "group_by_cols_bucket": int(meta_aug.get("group_by_cols_bucket", 0)),
            }
        except Exception as e:
            # fallback
            idx = int(res.argmin())
            stop = time.time()
            print("Selected index", idx,
                  "after", f"{round((stop - start) * 1000)}ms",
                  "Predicted reward / Predicted PG of index 0:", res[idx], "/", res[0])
            return idx

        try:
            storage.upsert_template(template_id, key_tuple_json=json.dumps(key_dbg))
        except Exception:
            pass

        tpl_seen_n = storage.get_template_seen_n(template_id)
        tpl_stats  = storage.read_tpl_arm_stats(template_id)

        try:
            estimated_total_cost = float(arms[0].get("Plan", {}).get("Total Cost", 0.0))
            if estimated_total_cost > 0:
                estimated_total_cost = math.log10(1.0 + estimated_total_cost) / 3.0 
            else:
                estimated_total_cost = None
        except Exception:
            estimated_total_cost = None

        model_scores = res
        idx, trace = choose_arm(template_id,
                         model_scores=model_scores,
                         tpl_seen_n=tpl_seen_n,
                         min_seen_tpl = 2, 
                         tpl_arm_stats=tpl_stats,
                         est_cost=estimated_total_cost,
                         top_k=3,
                         avoid_bottom_m=1,
                         eps0=0.2,
                         eps_min=0.1,
                         optimism_beta=0.00,
                         higher_is_better=False)

        # idx = res.argmin()
        stop = time.time()
        print("Selected index", idx,
              "after", f"{round((stop - start) * 1000)}ms",
              "Predicted reward / Predicted PG of index 0:", res[idx],
              "/", res[0])
        return idx

    # Predict
    def predict(self, messages):
        plan, buffers, metadata, arm_config = messages

        # if we don't have a model, make a prediction of NaN
        if self.__current_model is None:
            return math.nan

        # if we do have a model, make predictions for each plan.
        plans = add_buffer_info_to_plans(buffers, [plan])
        plans = add_meta_info_to_plans(metadata, plans)

        plans[0]["arm_config"] = arm_config

        res = self.__current_model.predict(plans)
        return res[0][0]
    
    def load_model(self, fp):
        try:
            new_model = model.OntoRegression(have_cache_data=True)
            new_model.load(fp)

            if reg_blocker.should_replace_model(
                    self.__current_model,
                    new_model):
                self.__current_model = new_model
                print("Accepted new model.")
            else:
                print("Rejecting load of new model due to regresison profile.")
                
        except Exception as e:
            print("Failed to load Onto model from", fp,
                  "Exception:", sys.exc_info()[0])
            raise e


class JSONTCPHandler(socketserver.BaseRequestHandler):
    def handle(self):
        logger = logging.getLogger(__name__)

        messages = []

        def recv_exact(n):
            data = b''
            while len(data) < n:
                chunk = self.request.recv(n - len(data))
                if not chunk:
                    print(f"[DEBUG] Socket closed while waiting for {n} bytes, only got {len(data)}")
                    raise ConnectionError("Connection closed unexpectedly")
                data += chunk
            return data

        try:
            while True:
                raw_len = recv_exact(4)
                msg_len = struct.unpack('!I', raw_len)[0]  

                raw_json = recv_exact(msg_len).decode('utf-8')
                data = json.loads(raw_json)

                if "final" in data:
                    break
                messages.append(data)

        except (ConnectionError, json.JSONDecodeError) as e:
            print(f"[ERROR] {e}")
            return

        if not messages:
            return

        mtype = messages[0].get("type")
        payload = messages[1:]
        
        if mtype == "query":
            num_pairs = (len(payload) - 2) // 2
            arms = []
            for i in range(num_pairs):
                plan = payload[2*i]
                cfg  = payload[2*i+1]
                plan["arm_config"] = cfg
                arms.append(plan)
            buffers, metadata = payload[-2], payload[-1]
            idx = self.server.onto_model.select_plan(arms + [buffers, metadata])
            logger.info("[SERVER] Selected arm index: %d", idx)

            self.request.sendall(struct.pack("I", idx))
            logger.info("[SERVER] Sent arm index to client.")
            self.request.close()

        elif mtype == "predict":
            res = self.server.onto_model.predict(payload)
            self.request.sendall(struct.pack("d", res))
            self.request.close()

        elif mtype == "reward":
            plan, buffers, metadata, arm_cfg, reward = payload
            plan = add_buffer_info_to_plans(buffers, [plan])[0]
            plan_root = plan.get("Plan", {})
            if 'arm_config_json' not in metadata:
                metadata['arm_config_json'] = arm_cfg

            metadata = augment_meta_from_plan(plan_root, metadata)

            tpl_id = None
            try:
                tpl_id = template_from_plan_meta(plan, metadata)
                metadata['template_id'] = tpl_id
            except Exception:
                logger.exception("[SERVER] template_from_plan_meta failed; continue without template_id")

            try:
                arm = (plan.get("arm_config", {}) or {}).get("index")
                if arm is None:
                    arm = (metadata.get("arm_config_json", {}) or {}).get("index")
                if tpl_id is not None and arm is not None:
                    storage.upsert_template(tpl_id, key_tuple_json='{}')
                    rt = float(reward.get("reward"))
                    storage.update_tpl_arm_stats(tpl_id, int(arm), rt)
            except Exception:
                logger.exception("[SERVER] Exception while updating template stats")

            raw = storage2.get_sql(metadata.get("sequence_id", ""))
            if raw:
                parsed = parse_sqlglot(raw, read_dialect="postgres")
                metadata = merge_parsed_sqlglot_into_meta(metadata, parsed)

                sem = enrich_sql_semantics(raw, read_dialect="postgres")
                metadata = merge_semantics_into_meta(metadata, sem)

            plan = add_meta_info_to_plans(metadata, [plan])[0]
            storage.record_reward(plan, reward["reward"], reward["pid"])

        elif mtype == "load model":
            path = payload[0]["path"]
            self.server.onto_model.load_model(path)

        else:
            print("Unknown message type:", mtype)

class OntoJSONHandler(JSONTCPHandler):
    def setup(self):
        self.__messages = []

def start_server(listen_on, port):
    setup_logging()

    print("server starting ....")

    logger = logging.getLogger(__name__)
    logger.info("Sever is listening on %d", port)
    logger.info("Server is listening on %s:%d", listen_on, port)

    model = OntoModel()

    if os.path.exists(DEFAULT_MODEL_PATH):
        print("Loading existing model")
        sys.stdout.flush()
        model.load_model(DEFAULT_MODEL_PATH)
    
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer((listen_on, port), OntoJSONHandler) as server:
        server.onto_model = model
        server.serve_forever()


if __name__ == "__main__":

    from config import read_config

    config = read_config()
    port = int(config["Port"])
    listen_on = config["ListenOn"]
    start_server(listen_on, port)
