import pycouch.wrapper as couchDB
import argparse
import re

def args_gestion():
    parser = argparse.ArgumentParser(description = "Build couchDB views to access sgrna by organism (specific to crispr database).\n Change create_view to create other views.")
    parser.add_argument("--db", metavar="<databaseRegex>", help = "Create view for database(s) corresponding to this regular expression", required = True)
    parser.add_argument("--url", metavar="<urlLocation>", help = "DB URL end-point [default: http://localhost:5984]")
    args = parser.parse_args()

    if args.url:
        args.url = args.url.rsplit("/")

    return args

def create_view():
    view = {
        "views" : {
            "organisms" : {
                "map" : "function(doc) { for (var org in doc) { if (org.charAt(0) != '_') { var nb_occurences = 0; for (var seq_ref in doc[org]){ nb_occurences = nb_occurences + doc[org][seq_ref].length} emit(org,nb_occurences)}}}"
            }
        }
    }
    return view


if __name__ == "__main__":
    ARGS = args_gestion()

    if not couchDB.couchPing():
        exit(1)

    if ARGS.url:
        couchDB.setServerUrl(ARGS.url)

    db_names = [db_name for db_name in couchDB.couchGetRequest("_all_dbs") if not db_name.startswith("_")]
    regExp = re.compile("^" + ARGS.db + "$")
    db_names = [db_name for db_name in db_names if regExp.match(db_name)]

    if not db_names:
        print("No database selected")
        exit()
    
    print("== Create view for :")
    for db_name in db_names:
        print(db_name)

    confirm = input("Confirm ? (y/n) ")
    while (confirm != "y" and confirm != "n"):
        confirm = input("I need y or n answer : ") 

    if confirm == "n":
        exit()

    view = create_view()
    
    for db in db_names: 
        print("Create view for " + db + "...")
        couchDB.couchAddDoc(data = view, target = db + "/_design", key = "organisms_view")

