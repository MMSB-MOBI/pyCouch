# TO DO : 
# option to give view names

import pycouch.wrapper as couchDB
import argparse
import json
import requests

SESSION = requests.session()
SESSION.trust_env = False


def args_gestion():
    parser = argparse.ArgumentParser(description = "Delete from couchDB. Can delete all entries from an organism.")
    parser.add_argument("--org", metavar="<organismName>", help = "Organism to delete", required = True)
    parser.add_argument("--url", metavar="<urlLocation>", help = "DB URL end-point [default: http://localhost:5984]", default = "http://localhost:5984")
    parser.add_argument("--map", metavar="<mappingFile>", help = "DB volumes end-point mapper rules", required = True)
    args = parser.parse_args()

    if args.url:
        args.url = args.url.rstrip("/")

    return args

def request_view(volumes, org): 
    org_format = org.replace(" ", "%20")
    all_sgrna = []
    for v in volumes:
        print(v)
        url = ARGS.url + '/' + v + '/_design/organisms_view/_view/organisms?key="' + org_format + '"'
        r = SESSION.get(url)
        if not r.ok:
            raise Exception(r.url, r, r.reason)
        sgrnas = get_sgrna_from_view(r.json())   
        all_sgrna += sgrnas

    return all_sgrna    

          
def get_sgrna_from_view(json_response):
    sgrna = [ r["id"] for r in json_response["rows"]]
    return sgrna

def updateDocOrg(old, **kwargs):
    new = {}
    if not "org" in kwargs:
        raise Exception("Give org argument in volDocUpdate for updateDocOrg")
    org = kwargs["org"]    
    
    for key in old:
        if not key.startswith("_"): #organism
            if key != org:
                new[key] = old[key]

    if new: 
        new["_id"] = old["_id"]
        new["_rev"] = old["_rev"]

    return new               


if __name__ == "__main__":
    ARGS = args_gestion()

    if ARGS.url:
        couchDB.setServerUrl(ARGS.url)

    if not couchDB.couchPing():
        exit()

    with open(ARGS.map, 'r') as fp:
        couchDB.setKeyMappingRules( json.load(fp) )

    volumes = [couchDB.QUEUE_MAPPER[k]["volName"] for k in couchDB.QUEUE_MAPPER]
    volumes = volumes[1:3]

    sgrnas = request_view(volumes, ARGS.org)

    print(len(sgrnas), "sgrnas in", ARGS.org)
    
    couchDB.volDocUpdate(sgrnas, updateFunc = updateDocOrg, org = ARGS.org)
    #print(couchDB.QUEUE_MAPPER)

    