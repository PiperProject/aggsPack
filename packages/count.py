#!/usr/bin/env python

NOSQL_TYPE = "mongodb"

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../adapters" )
sys.path.append( adaptersPath )

sys.exit( adaptersPath )

if NOSQL_TYPE == "mongodb" :
  from adapters import piper_mongodb
elif NOSQL_TYPE == "rocksdb" :
  from adapters import piper_rocksdb
else :
  sys.exit( "NOSQL_TYPE not specified.\nAborting..." )

# -------------------------------------- #


DEBUG = True

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

  res = adapters.get( ID, cursor )
  res = cleanGet( res, lhs )

  if op == ">" :
    if res > rhs :
      ret = True
  elif op == ">=" :
    if res < rhs :
      ret = True
  elif op == "<" :
    if res < rhs :
      ret = True
  elif op == "<=" :
    if res <= rhs :
      ret = True
  elif op == "==" :
    if res == rhs :
      ret = True

  return ret

###########
#  COUNT  #
###########
def count( idList, pred ) :
  if DEBUG :
    print "... Running aggsPack COUNT ..."

  num = 0
  for i in idList :
    parsedPred = pred.split(",")
    pred_true = checkPred()

    if (pred == None) or pred_true :
      num += 1

  return num


##########
#  MAIN  #
##########
def main() :
  print count( [1,2,3,4,5] )  

#############################
#  MAIN THRED OF EXECUTION  #
#############################
main()
