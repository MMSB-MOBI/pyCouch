import asyncio
import os
import sys
import pycouch.wrapper_class as couchDB
import requests
import json
import hashlib 
import curses
import time

SESSION = requests.session()
SESSION.trust_env = False

MONITOR = {}
WATCHING = None
LOG_STATUS = "./monitor_status.log"
LOG_RUNNING = "./monitor_running.log"

FAILED = []

# https://stackoverflow.com/questions/3173320/text-progress-bar-in-the-console

def initialize_log():
    status = open(LOG_STATUS, "w")
    running = open(LOG_RUNNING, "w")
    status.close()
    running.close()

def writeStatus(to_write):
    with open(LOG_STATUS, "a") as o:
        o.write(to_write)

def writeRunning(to_write):
     with open(LOG_RUNNING, "a") as o:
        o.write(to_write)

def setWatchObject(wrapperObject):
    global WATCHING
    WATCHING = wrapperObject

def setLogStatus(path):
    global LOG_STATUS
    LOG_STATUS = path

def setLogRunning(path):
    global LOG_RUNNING
    LOG_RUNNING = path   

def replicationStatus(doc):
    if "error" in doc: 
        return "error"
    return doc.get("state", "unknown")

def monitorStop(status):
    if status in ["error", "crashing", "failed", "completed"]:
        return True
    return False

def delete_replication_doc(rep):
    doc_url = WATCHING.end_point + "/_replicator/" + rep
    rev_id = json.loads(SESSION.get(doc_url).text)["_rev"]
    print("Delete failed " + rep + " replication document...")
    res = SESSION.delete(doc_url + "?rev=" + rev_id)
    print(res.text)

async def watch_status(filePath, nb_try=0) -> str:
    #print("WATCH")
    global HANDLE_STATUS
    global FAILED

    try:
        urlToWatch = WATCHING.end_point + "/_scheduler/docs/_replicator/" + filePath
        r = SESSION.get(urlToWatch)

        doc = json.loads(r.text)
        status = replicationStatus(doc)

        to_write = "####" + filePath + " " + time.strftime("%d-%m-%Y/%H:%M:%S") + " " + status + "\n"
        to_write += r.text
        writeStatus(to_write)

        last_md5 = hashlib.md5(r.text.encode()).hexdigest()
        
        if monitorStop(status):
            return

        while(True):
            if filePath in FAILED:
                return
            r = SESSION.get(urlToWatch)
            new_md5 = hashlib.md5(r.text.encode()).hexdigest()
            if new_md5 != last_md5:
                status = replicationStatus(json.loads(r.text))
                to_write = "####" + filePath + " CHANGE " + time.strftime("%d-%m-%Y/%H:%M:%S") + " " + status + "\n"
                to_write += r.text
                writeStatus(to_write)
                if monitorStop(status):
                    return
                last_md5 = new_md5
            await asyncio.sleep(2)  
    
    except Exception as e:
        nb_try += 1
        if nb_try == 500: 
            print(filePath + " watch_status() Retry 500 times")
            print(e)
            delete_replication_doc(filePath)
            writeRunning(filePath + "\t" + "Watch status fail " + str(e) + "\n")
            writeStatus("####" + filePath + " Watch status fail " + str(e) + "\n")
            FAILED.append(filePath)
            return

        await watch_status(filePath, nb_try)
        return


async def watch_advancement(repID, all_docs, target, nb_try = 0):
    global HANDLE_RUNNING
    global FAILED

    try:
        doc = json.loads(SESSION.get(target).text)
        
        nb_replicate = str(doc.get("doc_count", 0))
        writeRunning(repID + " " + all_docs + " " + nb_replicate + " (Init)\n")
        last_replicate = nb_replicate

        while(True):
            if repID in FAILED:
                return
            doc = json.loads(SESSION.get(target).text)
            nb_replicate = str(doc.get("doc_count", 0))
            writeRunning(repID + " " + all_docs + " " + nb_replicate + " (" + str(int(last_replicate)/int(all_docs)*100) + "%)\n")
            if nb_replicate == all_docs:
                return
            last_replicate = nb_replicate
            await asyncio.sleep(5)    

    except Exception as e:
        nb_try += 1
        if nb_try == 500: 
            print(repID + " watch_advancement() Retry 500 times")
            print(e)
            delete_replication_doc(repID)
            writeRunning(repID + "\t" + "Watch advancement fail " + str(e) + "\n")
            writeStatus("####" + repID + " Watch advancement fail " + str(e) + "\n")
            FAILED.append(repID)
            nb_try = 0
            return

        await watch_advancement(repID, all_docs, target, nb_try)
        return

async def get_source_and_target(rep_id):
    global FAILED
    try:
        dic_ret = {rep_id: {}}
        r = SESSION.get(WATCHING.end_point + "/_replicator/" + rep_id)
        doc = json.loads(r.text)
        if "error" in doc:
            raise Exception(rep_id, doc)
        dic_ret[rep_id]["target"] = doc["target"]
        #dic_ret[rep_id]["source"] = doc["source"]
        doc_source = json.loads(SESSION.get(doc["source"]).text)
        if "error" in doc_source:
            raise Exception(doc["source"], doc_source)
        nb_doc = doc_source["doc_count"]
        dic_ret[rep_id]["source"] = str(nb_doc)
        
    except Exception as e:
        print(rep_id + " get_source_and_target() fail")
        delete_replication_doc(rep_id)
        writeRunning(rep_id + "\t" + "Get source and target fail " + "\t" + str(e) + "\n")
        writeStatus("####" + rep_id + " Get source and target fail " + str(e) + "\n")
        FAILED.append(rep_id)
        return {rep_id : "Fail"}

    return dic_ret

async def main(*filePath : str) -> None:
    #print("MAIN")
    ret = await asyncio.gather(*[ get_source_and_target(f) for f in filePath ])
    source_target_dic = {k: v for dic in ret for k, v in dic.items()}

    filePath = [f for f in filePath if source_target_dic[f] != "Fail"]

    await asyncio.gather(*[ watch_status(f) for f in filePath ], *[ watch_advancement(f, source_target_dic[f]["source"], source_target_dic[f]["target"]) for f in filePath])

def launch(*repIDs):
    #source_target_dic = asyncio.run(run_source_target(*repIDs))
    if not repIDs:
        print("No replication to follow. Give at least one replication id as argument.")
        exit()
    initialize_log()
    print("Follow replication states in", LOG_STATUS)
    print("Follow replication advancement in", LOG_RUNNING)
    print()
    asyncio.run(main(*repIDs)) 
    return FAILED
#    fileToWatch = sys.argv[1:]
#    asyncio.run(main(*fileToWatch))

def finish_watching(status):
    global NB_TRY
    NB_TRY = 0 
    return status