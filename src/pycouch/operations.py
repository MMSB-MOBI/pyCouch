import sys
sys.path.append("/home/chilpert/Dev/pythonLogger")

import pythonLogger as pl 
import pycouch.exceptions as exceptions
import pycouch.watch as watch

LOGGER = pl.init_logger("operations.py", "debug")
LOGGER.propagate = False

def replicate(source_wrapper, target_wrapper, source_name, target_name):

    def create_doc():
        LOGGER.debug("Create replication document")
        doc = { "_id" : target_wrapper.couchGenerateUUID(),
        "source": source_wrapper.end_point + "/" + source_name,
        "target": target_wrapper.end_point + "/" + target_name,
        "continuous" : False,
        "create_target" : True
        }
        return doc    

    LOGGER.info(f"Replicate {source_name} from {source_wrapper.end_point} to {target_name} in {target_wrapper.end_point}")

    #Check db connection
    if not source_wrapper.couchPing():
        raise exceptions.FailedConnection(f"Can't connect to {source_wrapper.end_point}")
    if not target_wrapper.couchPing():
        raise exceptions.FailedConnection(f"Can't connect to {target_wrapper.end_point}")  

    #Check source existence
    if not source_wrapper.couchTargetExist(source_name):
        raise exceptions.DatabaseNotFound(f"{source_wrapper.end_point}/{source_name} doesn't exist.")

    if target_wrapper.couchTargetExist(target_name):
        LOGGER.warn(f"{target_wrapper.end_point}/{target_name} already exists. Content will be erased.")
    
    replication_doc = create_doc()

    LOGGER.debug(f"Replication doc : {replication_doc}")

    post_resp = target_wrapper.couchPostRequest("_replicator", replication_doc)

    if "error" in post_resp:
        LOGGER.error(f"Put replication doc : {post_resp}")
        exit()

    LOGGER.debug(f"Replication doc successfully put : {post_resp}")

    LOGGER.info("Monitor replication...")

    watch.setWatchObject(target_wrapper)
    failed_rep = watch.launch(replication_doc["_id"])
    if failed_rep:
        LOGGER.error(f"Replication fails for {failed_rep}")
    else:
        LOGGER.info("Replication done")    




    

