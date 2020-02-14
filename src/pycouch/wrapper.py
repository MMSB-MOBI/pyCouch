import requests
import json, re, time, random
from copy import deepcopy
import logging
import sys
import pycouch.error as error

DEBUG_MODE = False

SESSION = requests.session()
SESSION.trust_env = False


def lambdaFuse(old, new):
    for k in new:
        old[k] = new[k]
    #Reinsert new document even if an old one with same id have been deleted    
    if old.get("_deleted"):
        old = {k: v for k,v in old.items() if k not in ["_rev", "_deleted"]} 

    return old

class Wrapper():
    def __init__(self, end_point = 'http://127.0.0.1:5984', admin = (None,None)):
        '''end_point : couch url
        admin : tuple (admin login, admin password)
        '''
        self.end_point = end_point.rstrip("/")
        self._bak_endpoint = "".join(end_point.split("//")[1:])
        self.queue_mapper = {}
        if admin[0] and admin[1]:
            self.setAdmin(admin[0], admin[1])
    
    def setServerUrl(self,path):
        '''path : str'''
        self.end_point = path.rstrip("/")

    def setKeyMappingRules(self,ruleLitt):
        '''ruleLitt : json dic'''
        self.queue_mapper = {}
        for regExp in ruleLitt:
            self.queue_mapper[regExp] = {
                "volName" : ruleLitt[regExp],
                "queue" : {}
            }
    
    @property
    def hasKeyMappingRules(self):
        return bool(self.queue_mapper)

    def setAdmin(self, admin_name, admin_password):
        self.admin = admin_name
        self.end_point = 'http://' + admin_name + ":" + admin_password + "@" + self._bak_endpoint

    ## QUEUE FUNCTIONS
    def putInQueue(self, key, val = None):
        for regExp, cQueue in self.queue_mapper.items():
            if re.search(regExp, key):
                cQueue["queue"][key] = val
                return

    def resetQueue(self):
        for regExp, cQueue in self.queue_mapper.items():
            cQueue["queue"] = {}


    ## MULTIPLE VOLUME BULK FN WRAPPER 
    def volDocAdd(self, iterable, updateFunc=lambdaFuse):
        if not self.queue_mapper:
            raise ValueError ("Please set volume mapping rules")
        
        logging.debug(f"Dispatching {len(iterable)} items")

        data = []
        for k,v in list(iterable.items()):
            self.putInQueue(k,v)

        for regExp, cQueue in self.queue_mapper.items():
            if not cQueue["queue"]:
                continue   
            logging.info(f'inserting {regExp} {len(cQueue["queue"])} element(s) => {cQueue["volName"]}')
            logging.debug(f'DM {cQueue["queue"]}')
            joker = 0
            _data = []
            while True:
                try : 
                    _data = self.bulkDocAdd(cQueue["queue"], updateFunc=updateFunc, target=cQueue["volName"])
                    logging.info(f"Success bulkDocAdd at try {joker}")
                except Exception as e:
                    logging.error("Something wrong append while bulkDocAdd, retrying time " + str(joker))
                    logging.error("Error LOG is " + str(e))
                    joker += 1
                    if joker > 50:
                        logging.error("50 tries failed, giving up")
                        break
                    time.sleep(5)
                    continue
                break
            data += _data
        self.resetQueue()
        return data

    
    def volDocUpdate(self, keys, updateFunc, **kwargs):
        '''Take db keys and an update function. You need to define a function that take a document and optionally other arguments and return a new document to replace the given one. '''

        if not self.queue_mapper:
            raise ValueError ("Please set volume mapping rules")

        for k in keys:
            self.putInQueue(k)

        for regExp, cQueue in self.queue_mapper.items():
            if not cQueue["queue"]:
                continue

            logging.info(f'updating {regExp} {len(cQueue["queue"])} element(s) => {cQueue["volName"]}')
            data = self.bulkDocUpdate(cQueue["queue"], updateFunc=updateFunc, target=cQueue["volName"], **kwargs)
    

    ## BULK FUNCTIONS
    # Doing deepcopy to avoid modification of the input iterable w/ _id, _rev keys
    def bulkDocAdd(self, _iterable, updateFunc=lambdaFuse, target=None, depth=0): # iterable w/ key:value pairs, key is primary _id in DB and value is document to insert

        iterable = deepcopy(_iterable)
        logging.debug(f"bulkDocAdd iterable content {_iterable}")

        if not target:
            raise ValueError ("No target db specified")
        ans = self.bulkRequestByKey(list(iterable.keys()), target)# param iterable msut have a keys method

        logging.debug(f"bulkDocAdd prior key request {ans.keys()}")
        logging.debug(ans)
        
        bulkInsertData = {"docs" : [] }
        for reqItem in ans['results']:       
            key = reqItem["id"]
            dataToPut = iterable[key]
            if "_rev" in dataToPut:
                del dataToPut["_rev"]
            dataToPut['_id'] = key
            
            _datum = reqItem['docs'][0] # mandatory docs key, one value array guaranted
            if 'error' in _datum:
                if self.docNotFound(_datum["error"]):
                    logging.debug(f"creating {key} document")
                else:
                    logging.error(f'Unexpected error here {_datum}')
                
            elif 'ok' in _datum:
                if "error" in _datum["ok"]:
                    raise error.CouchWrapperError("Unexpected \"error\" key in bulkDocAdd answer packet::" + str( _datum["ok"]))   
                dataToPut = updateFunc(_datum["ok"], iterable[key])
            else:
                logging.error(f'unrecognized item packet format {reqItem}')
                continue
                
            bulkInsertData["docs"].append(dataToPut) 

        logging.debug(f"about to bulk_doc that {bulkInsertData}")
        #r = requests.post(DEFAULT_END_POINT + '/' + target + '/_bulk_docs', json=bulkInsertData)
        #ans = json.loads(r.text)
        insertError, insertOk = ([], [])
        r = SESSION.post(self.end_point + '/' + target + '/_bulk_docs', json=bulkInsertData)
        ans = json.loads(r.text)       
        insertOk, insertError = self.bulkDocErrorReport(ans)
        # If unknown_error occurs in insertion, rev tag have to updated, this fn takes care of this business
        # so we filter the input and make a recursive call 

        if insertError:
            depth += 1
            logging.debug(f"Retry depth {depth}")
            if depth == 1:
                logging.error(f"Insert Error Recursive fix\n{insertError}")

            if depth == 50:
                logging.error(f"Giving up at 50th try for {insertError}")
            else:
                idError = [ d['id'] for d in insertError ]
                logging.debug(f"iterable to filter from {iterable}")
                logging.debug(f"depth {depth} insert Error content: {insertError}")
                _iterable = { k:v for k,v in iterable.items() if k in idError}
                insertOk  += self.bulkDocAdd(_iterable, updateFunc=updateFunc, target=target, depth=depth)
        elif depth > 0:
            logging.info(f"No more recursive insert left at depth {depth}", depth)
        
        logging.debug(f"returning {insertOk}")
    
        return insertOk   

    def bulkDocUpdate(self, iterable, updateFunc, target=None, depth=0, **kwargs):
        ans = self.bulkRequestByKey(list(iterable.keys()), target)
        bulkInsertData = {"docs" : [] }
        i = 0
        for reqItem in ans['results']:    
            i += 1   
            current_doc = reqItem['docs'][0]["ok"]
            dataToPut = updateFunc(current_doc, **kwargs)
            bulkInsertData["docs"].append(dataToPut)
        
        insertError, insertOk = ([], [])
        r = SESSION.post(self.end_point + '/' + target + '/_bulk_docs', json=bulkInsertData)
        ans = json.loads(r.text)     
        insertOk, insertError = self.bulkDocErrorReport(ans)
        # If unknown_error occurs in insertion, rev tag have to updated, this fn takes care of this business
        # so we filter the input and make a recursive call 

        if insertError:
            depth += 1
            logging.debug(f"Retry depth {depth}")
            if depth == 1:
                logging.error(f"Insert Error Recursive fix\n{insertError}")

            if depth == 50:
                logging.error(f"Giving up at 50th try for {insertError}")
            else:
                idError = [ d['id'] for d in insertError ]
                logging.debug(f"iterable to filter from {iterable}")
                logging.debug(f"depth {depth} insert Error content: {insertError}")
                _iterable = { k:v for k,v in iterable.items() if k in idError}
                insertOk  += self.bulkDocAdd(_iterable, updateFunc=updateFunc, target=target, depth=depth)
        elif depth > 0:
            logging.info(f"No more recursive insert left at depth {depth}")
        logging.debug("returning ", insertOk)
        return insertOk       

    def bulkRequestByKey(self, keyIter, target, packetSize=2000):      
        data = {"results" : []}
        logging.debug(f"bulkRequestByKey at {target}")  
        for i in range(0,len(keyIter), packetSize):
            j = i + packetSize if i + packetSize < len(keyIter) else len(keyIter)
            keyBag = keyIter[i:j]
            _data = self._bulkRequestByKey(keyBag, target)
        # data["results"].append(_data["results"])
            data["results"] += _data["results"]
        #if DEBUG_MODE:
        #    print(data)
        return data

    def bulkRequestByKeyParallel(self, keyIter, target, packetSize=2000, processus = 6):      
        data = {"results" : []}
        logging.debug(f"bulkRequestByKey at {target}")  

        pool = Pool(processus)
        packets = [keyIter[x:x+packetSize] for x in range(0, len(keyIter), packetSize)]
        data = pool.starmap(self._bulkRequestByKey, [(p, target) for p in packets])
        return data    

    def _bulkRequestByKey(self, keyIter, target):
        req = {
            "docs" : [ {"id" : k } for k in keyIter ]
        }
        url = self.end_point + '/' + target +'/_bulk_get'
        r = SESSION.post(url, json=req)
        data = json.loads(r.text) 
        if not 'results' in data:
            raise TypeError("Unsuccessful bulkRequest at", url)
        return data

    def bulkDocErrorReport(self,data):
        if DEBUG_MODE:
            v = random.randint(0, 1)
            x = random.randint(0, len(data) - 1)
            if v == 0:
                logging.debug(f"Generating error at postion {x}")
                logging.debug(f"prev is {data[x]}")
                errPacket = {'id': data[x]['id'], 'error': 'unknown_error', 'reason': 'undefined', '_rev' : data[x]['rev']}
                logging.debug(f"{data[x]} --> {errPacket}")
                data[x] = errPacket
        
        ok = []
        err = []
        for insertStatus in data:
            if 'ok' in insertStatus:
                if insertStatus['ok']:
                    ok.append(insertStatus)
                else:
                    logging.error(f"NEW ERROR {insertStatus}")
            else :
                err.append(insertStatus)

        return (ok, err)


    
    ## NON BULK FUNCTIONS
    def couchPing(self):
        data = ""
        try :
            r = SESSION.get(self.end_point)
            try :
                data = json.loads(r.text)
            except:
                print('Cant decode ping', file=sys.stderr)
                return False
        except:
            print(f"Cant connect to DB at: {self.end_point}", file = sys.stderr)
            return False

        logging.info(f"Connection established\n{data}")
        return True    

    def couchPS(self):
        r = SESSION.get(self.end_point + '/_active_tasks')
        return json.loads(r.text)    

    def couchGetRequest(self,path, parameters=None):
        r= SESSION.get(self.end_point + '/' + path, params = parameters)
        result = json.loads(r.text)
        return result

    def couchDeleteRequest(self, path, parameters = None):
        r = SESSION.delete(self.end_point + "/" + path, params = parameters)
        resulttext = r.text
        return json.loads(resulttext)

    def couchPutRequest(self, path, data = None):
        r= SESSION.put(self.end_point + '/' + path, json=data)
        result = json.loads(r.text)
        if "error" in result: 
            raise error.CouchWrapperError(result)
        return result
    
    def couchPostRequest(self, path, data):
        r = SESSION.post(self.end_point + "/" + path, json = data)
        result = json.loads(r.text)
        return result

    def couchDbList(self):
        return self.couchGetRequest('_all_dbs')

    def couchGenerateUUID(self):
        return self.couchGetRequest('_uuids')["uuids"][0]   

    def docNotFound(self,data):
        if "error" in data and "reason" in data:
            return data["error"] == "not_found" and (data["reason"] == "missing" or data["reason"] == "deleted")
        return False

    def couchGetDoc(self,target, key):
        if not key:
            raise ValueError("Please specify a document key")
            
        MaybeDoc = self.couchGetRequest(str(target) + '/' + str(key))

        if self.docNotFound(MaybeDoc):
            return None
        
        if "error" in MaybeDoc:
            raise error.CouchWrapperError(MaybeDoc)

        return MaybeDoc

    def couchPutDoc(self, target, key, data):
        MaybePut = self.couchPutRequest(target + '/' + key, data)
        return MaybePut
    
    def couchPostDoc(self, target, doc):
        maybePost = self.couchPostRequest(target, doc)
        if "error" in maybePost:
            raise error.CouchWrapperError(maybePost)
        return maybePost

    def couchAddDoc(self, data, target=None, key=None, updateFunc=lambdaFuse):
        if not target:
            raise ValueError("Please specify a database to target")

        key = self.couchGenerateUUID() if not key else key
        ans = self.couchGetDoc(target,key)

        dataToPut = data
        if not ans:
            logging.debug ("Creating " + target + "/" + key)       
            logging.debug(ans)
        else :
            logging.debug("Updating " + target + "/" + key)
            logging.debug(ans)
            dataToPut = updateFunc(ans, data)   
        ans = self.couchPutDoc(target, key, dataToPut)
        logging.debug(ans)
        
        return ans

    def couchDeleteDoc(self, target, key):
        if not key:
            raise ValueError("Please specify a document key")
        MaybeGet = self.couchGetDoc(target, key)
        if not MaybeGet or self.docNotFound(MaybeGet):
            logging.warn("Document doesn't exist")
            return None
        params = {'rev' : MaybeGet["_rev"]}
        MaybeDelete = self.couchDeleteRequest(f"{target}/{key}", params)
        return MaybeDelete

    def targetNotFound(self,data):
        if "error" in data and "reason" in data:
            return data["error"] == "not_found" and data["reason"] == "Database does not exist."
        return False

    def couchTargetExist(self, target):
        res = self.couchGetRequest(target)
        if self.targetNotFound(res):
            return False  
        if "error" in res:
            raise error.CouchWrapperError(res)
        return True 

    def couchCreateDB(self, target):
        res = self.couchPutRequest(target)
        return res

    