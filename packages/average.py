#!/usr/bin/env python

# -------------------------------------- #
import os, sys

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../../../core" )
sys.path.append( settingsPath )
import settings

# aggs pack dir
path  = os.path.abspath( __file__ + "/.." )
sys.path.append( path )
import count, sum_agg

# -------------------------------------- #


DEBUG = settings.DEBUG


#############
#  AVERAGE  #
#############
def average( nosql_type, cursor, idList, pred ) :
  if DEBUG :
    print "... Running aggsPack AVERAGE ..."

  theSum   = sum_agg.sum_agg( nosql_type, cursor, idList, pred )
  theCount = count.count( nosql_type, cursor, idList, pred )

  return float(theSum) / theCount

