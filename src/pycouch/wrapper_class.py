import requests
import json, re, time, random

class CouchWrapperError(Exception):
    pass


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
        
        if DEBUG_MODE:
            print("Dispatching", len(iterable), "items")

        data = []
        for k,v in list(iterable.items()):
            self.putInQueue(k,v)

        for regExp, cQueue in self.queue_mapper.items():
            if not cQueue["queue"]:
                continue   
            print("inserting ", regExp, str(len(cQueue["queue"])), "element(s) =>", cQueue["volName"])
            if DEBUG_MODE:
                print("DM",cQueue["queue"])
            joker = 0
            _data = []
            while True:
                try : 
                    _data = self.bulkDocAdd(cQueue["queue"], updateFunc=updateFunc, target=cQueue["volName"])
                except Exception as e:
                    print("Something wrong append while bulkDocAdd, retrying time", str(joker))
                    print("Error LOG is ", str(e))
                    exit()
                    joker += 1
                    if joker > 50:
                        print("50 tries failed, giving up")
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

            print("updating ", regExp, str(len(cQueue["queue"])), "element(s) =>", cQueue["volName"])  
            data = self.bulkDocUpdate(cQueue["queue"], updateFunc=updateFunc, target=cQueue["volName"], **kwargs)
    

    ## BULK FUNCTIONS

    def bulkDocAdd(self, iterable, updateFunc=lambdaFuse, target=None, depth=0): # iterable w/ key:value pairs, key is primary _id in DB and value is document to insert

        if DEBUG_MODE:
            print("bulkDocAdd iterable content", iterable)

        if not target:
            raise ValueError ("No target db specified")
        ans = self.bulkRequestByKey(list(iterable.keys()), target)# param iterable msut have a keys method
        
        if DEBUG_MODE:
            print("bulkDocAdd prior key request ", ans.keys())
            print(ans)
        
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
                    if DEBUG_MODE:
                        print("creating ", key, "document" )
                else:
                    print ('Unexpected error here', _datum)
                
            elif 'ok' in _datum:
                if "error" in _datum["ok"]:
                    raise CouchWrapperError("Unexpected \"error\" key in bulkDocAdd answer packet::" + str( _datum["ok"]))   
                dataToPut = updateFunc(_datum["ok"], iterable[key])
            else:
                print('unrecognized item packet format', str(reqItem))
                continue
                
            bulkInsertData["docs"].append(dataToPut) 

        if DEBUG_MODE:
            print("about to bulk_doc that", str(bulkInsertData)) 
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
            if DEBUG_MODE:
                print("Retry depth", depth)
            if depth == 1:
                print("Insert Error Recursive fix\n", insertError)

            if depth == 50:
                print("Giving up at 50th try for", insertError)
            else:
                idError = [ d['id'] for d in insertError ]
                if DEBUG_MODE:
                    print("iterable to filter from", iterable)
                    print("depth", depth, ' insert Error content:', insertError)
                _iterable = { k:v for k,v in iterable.items() if k in idError}
                insertOk  += self.bulkDocAdd(_iterable, updateFunc=updateFunc, target=target, depth=depth)
        elif depth > 0:
            print("No more recursive insert left at depth", depth)
        if DEBUG_MODE:
            print("returning ", insertOk)
    
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
            if DEBUG_MODE:
                print("Retry depth", depth)
            if depth == 1:
                print("Insert Error Recursive fix\n", insertError)

            if depth == 50:
                print("Giving up at 50th try for", insertError)
            else:
                idError = [ d['id'] for d in insertError ]
                if DEBUG_MODE:
                    print("iterable to filter from", iterable)
                    print("depth", depth, ' insert Error content:', insertError)
                _iterable = { k:v for k,v in iterable.items() if k in idError}
                insertOk  += self.bulkDocAdd(_iterable, updateFunc=updateFunc, target=target, depth=depth)
        elif depth > 0:
            print("No more recursive insert left at depth", depth)
        if DEBUG_MODE:
            print("returning ", insertOk)
        return insertOk       

    def bulkRequestByKey(self, keyIter, target, packetSize=2000):      
        data = {"results" : []}
        if DEBUG_MODE:
            print("bulkRequestByKey at", target)  
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
        if DEBUG_MODE:
            print("bulkRequestByKey at", target)  

        pool = Pool(processus)
        packets = [keyIter[x:x+packetSize] for x in range(0, len(keyIter), packetSize)]
        data = pool.starmap(self._bulkRequestByKey, [(p, target) for p in packets])
        print(data[0].keys())
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
                print("Generating error at postion", x)
                print("prev is", data[x])
                errPacket = {'id': data[x]['id'], 'error': 'unknown_error', 'reason': 'undefined', '_rev' : data[x]['rev']}
                print(data[x], "-->", errPacket)
                data[x] = errPacket
        
        ok = []
        err = []
        for insertStatus in data:
            if 'ok' in insertStatus:
                if insertStatus['ok']:
                    ok.append(insertStatus)
                else:
                    print ("NEW ERROR", str(insertStatus))
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
                print('Cant decode ping')
                return False
        except:
            print("Cant connect to DB at:", self.end_point)
            return False

        print("Connection established\n", data)
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
            raise CouchWrapperError(result)
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
            raise CouchWrapperError(MaybeDoc)

        return MaybeDoc

    def couchPutDoc(self, target, key, data):
        MaybePut = self.couchPutRequest(target + '/' + key, data)
        return MaybePut
    
    def couchPostDoc(self, target, doc):
        maybePost = self.couchPostRequest(target, doc)
        if "error" in maybePost:
            raise CouchWrapperError(maybePost)
        return maybePost

    def couchAddDoc(self, data, target=None, key=None, updateFunc=lambdaFuse):
        if not target:
            raise ValueError("Please specify a database to target")

        key = self.couchGenerateUUID() if not key else key
        ans = self.couchGetDoc(target,key)

        dataToPut = data
        if not ans:
            if DEBUG_MODE:
                print ("Creating " + target + "/" + key)       
                print(ans)
        else :
            if DEBUG_MODE:
                print ("Updating " + target + "/" + key)
                print(ans)
            dataToPut = updateFunc(ans, data)   
        ans = self.couchPutDoc(target, key, dataToPut)
        if DEBUG_MODE:
            print(ans)
        
        return ans

    def couchDeleteDoc(self, target, key):
        if not key:
            raise ValueError("Please specify a document key")
        MaybeGet = self.couchGetDoc(target, key)
        if not MaybeGet or self.docNotFound(MaybeGet):
            print("Document doesn't exist")
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
            raise CouchWrapperError(res)
        return True 

    def couchCreateDB(self, target):
        res = self.couchPutRequest(target)
        return res

    def couchReplicate(self, source, target, continuous = False):
        '''Replicate source to target. With continuous = True, changes on source will trigger changes on target'''
        create_target = False
        if not self.couchTargetExist(source):
            print(f"{source} doesn't exist")
            return
        
        if not self.couchTargetExist(target):
            create_target = True

        replication_doc = {
            "source": self.end_point + "/" + source,
            "target" : self.end_point + "/" + target,
            "create_target" : create_target,
            "continuous" : continuous
        }

        try:
            self.couchPostDoc("_replicator", replication_doc)
        except CouchWrapperError as e: 
            print(f"Error while post document\n{e}")

        print(f"Replicate document posted")


    