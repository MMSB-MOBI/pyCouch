import pycouch.wrapper_class as wrapper
import pycouch.operations as op

source = wrapper.Wrapper("http://localhost:1234")
target = wrapper.Wrapper("http://localhost:5984")

target.setAdmin("couch_agent", "couch")

op.replicate(source, target, "crispr_rc01_v47", "crispr_rc01_v47_arwen")
