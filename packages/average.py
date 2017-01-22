#!/usr/bin/env python

NOSQL_TYPE = "mongodb"

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
sys.path.append( adaptersPath )

#sys.exit( adaptersPath )

if NOSQL_TYPE == "mongodb" :
  from adapters import piper_mongodb
elif NOSQL_TYPE == "rocksdb" :
  from adapters import piper_rocksdb
else :
  sys.exit( "NOSQL_TYPE not specified.\nAborting..." )

path  = os.path.abspath( __file__ + "/.." )
sys.path.append( path )
import count, sum_agg

# -------------------------------------- #


DEBUG = True


#############
#  AVERAGE  #
#############
def average( idList, cursor, pred ) :
  if DEBUG :
    print "... Running aggsPack AVERAGE ..."

  theSum   = sum_agg.sum_agg( idList, cursor, pred )
  theCount = count.count( idList, cursor, pred )

  return float(theSum) / theCount

