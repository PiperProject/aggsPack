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
  print res
  print lhs

  print res[lhs]

  return res[ lhs ]

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
    resTarge = int( rhs )
    rhs      = int( rhs )

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

  return ret

###########
#  COUNT  #
###########
def count( idList, cursor, pred ) :
  if DEBUG :
    print "... Running aggsPack COUNT ..."

  num = 0
  for i in idList :
    if pred :
      parsedPred = pred.split(",")
      pred_true = checkPred( i, cursor, parsedPred )

    if (pred == None) or pred_true :
      num += 1

  return num

