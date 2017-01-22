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

# -------------------------------------- #


DEBUG = True

##################
#  PARSE RESULT  #
##################
# assume results are collected in a dictionary for convenience.
def parseResult( res, lhs ) :
  print "res = " + str(res)
  print "lhs = " + lhs

  print "res[lhs] = " + str(res[lhs])

  return str(res[ lhs ])

################
#  CHECK PRED  #
################
# preds assumed of the form "lhs op rhs"
# lhs is a single attribute
# rhs is a value
# op  is >, <, >=, <= == 
def checkPred( ID, cursor, parsedPred ) :
  ret = False

  if len(parsedPred) > 0 :
    lhs = parsedPred[0]
    op  = parsedPred[1]
    rhs = parsedPred[2]

  resFull   = piper_mongodb.get( ID, cursor ) # need to make an adapters.py adapters( "mongodb" ), generalize
  resTarget = parseResult( resFull, lhs )

  if rhs.isdigit() :
    resTarget = int( resTarget )
    rhs       = int( rhs )

  #return resTarget

  if op == ">" :
    if resTarget > rhs :
      ret = True
  elif op == ">=" :
    if resTarget < rhs :
      ret = True
  elif op == "<" :
    if resTarget < rhs :
      ret = True
  elif op == "<=" :
    if resTarget <= rhs :
      ret = True
  elif op == "==" :
    if resTarget == rhs :
      ret = True

  if ret :
    return resTarget
  else :
    return None

#########
#  SUM  #
#########
def sum_agg( idList, cursor, pred ) :
  if DEBUG :
    print "... Running aggsPack SUM ..."

  mySum = 0
  for i in idList :
    if pred :
      parsedPred = pred.split(",")
      result = checkPred( i, cursor, parsedPred )
      print "result = " + str(result)

    if (pred == None) or not (result == None) :
      mySum += result

  return mySum

