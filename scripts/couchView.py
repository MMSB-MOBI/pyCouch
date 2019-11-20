"""
    Build couchDB views to access sgrna by organism (specific to crispr database)
    Usage:

    Options:
"""

from docopt import docopt
import pycouch.wrapper as couchDB

if __name__ == "__main__":
    arguments = docopt(__doc__, version='couchBuild 1.0')

    db = "crispr_rc01_v10"

    view = {
        "views" : {
            "organisms" : {
                "map" : "function(doc) { for (var org in doc) { if (org.charAt(0) != '_') { var nb_occurences = 0; for (var seq_ref in doc[org]){ nb_occurences = nb_occurences + doc[org][seq_ref].length} emit(org,nb_occurences)}}}"
            }
        }
    }

    couchDB.couchPing()
    couchDB.couchAddDoc(data = view, target = db + "/_design", key = "organisms_view")

