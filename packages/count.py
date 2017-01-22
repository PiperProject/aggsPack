#!/usr/bin/env python

# -------------------------------------- #
import os, sys

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
sys.path.append( adaptersPath )
#from adapters import Adapter
import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
sys.path.append( settingsPath )
import settings

# -------------------------------------- #

DEBUG      = settings.DEBUG
NOSQL_TYPE = settings.NOSQL_TYPE

##################
#  PARSE RESULT  #
##################
# assume results are collected in a dictionary for convenience.
def parseResult( res, lhs ) :
  if DEBUG :
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

  ad        = Adapter.Adapter( NOSQL_TYPE )
  resFull   = ad.get( ID, cursor ) 
  resTarget = parseResult( resFull, lhs )

  if rhs.isdigit() :
    resTarget = int( resTarget )
    rhs       = int( rhs )

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

