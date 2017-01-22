#!/usr/bin/env python

NOSQL_TYPE = "mongodb"

# -------------------------------------- #
import os, sys

# adapter dir
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
sys.path.append( adaptersPath )
from adapters import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../../../core" )
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

  ad        = Adapter.Adapter( NOSQL_TYPE )
  resFull   = ad.get( ID, cursor )
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

