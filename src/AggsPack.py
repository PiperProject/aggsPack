#/usr/bin/env python

##########################################################################
# AggsPack usage notes:
#
# 1. //
#
##########################################################################

#############
#  IMPORTS  #
#############
# standard python packages
import copy, logging, math, os, pickledb, string, sys, unittest

# ------------------------------------------------------ #

# import sibling packages HERE!!!
adaptersPath  = os.path.abspath( __file__ + "/../../../../adapters" )
if not adaptersPath in sys.path :
  sys.path.append( adaptersPath )
import Adapter

# settings dir
settingsPath  = os.path.abspath( __file__ + "/../../core" )
if not settingsPath in sys.path :
  sys.path.append( settingsPath )
import settings

# ------------------------------------------------------ #

DEBUG = settings.DEBUG

class AggsPack( object ) :

  ################
  #  ATTRIBUTES  #
  ################
  nosql_type = None   # the type of nosql database under consideration
  cursor     = None   # pointer to target database instance


  ##########
  #  INIT  #
  ##########
  def __init__( self, nosql_type, dbcursor ) :
    self.cursor   = dbcursor
    self.nosql_type = nosql_type


  #############
  #  AVERAGE  #
  #############
  def average( self, idList, pred ) :
    if DEBUG :
      print "... Running aggsPack AVERAGE ..."
  
    theSum   = self.sum_agg( idList, pred )
    theCount = self.count( idList, pred )
  
    return float(theSum) / theCount


  ###########
  #  COUNT  #
  ###########
  def count( self, idList, pred ) :
    if DEBUG :
      print "... Running aggsPack COUNT ..."
  
    num = 0
    for i in idList :
      if pred :
        parsedPred = pred.split(",")
        pred_true  = self.checkPred( i, parsedPred )
  
      if (pred == None) or pred_true :
        num += 1
  
    return num


  #########
  #  MAX  #
  #########
  def max_agg( self, idList, pred ) :
    if DEBUG :
      print "... Running aggsPack MAX ..."
  
    myList = []
    for i in idList :
      if pred :
        parsedPred = pred.split(",")
        result     = self.checkPred( i, parsedPred )
        if DEBUG :
          print "result = " + str(result)
  
      if (pred == None) or not (result == None) :
        myList.append( result )
  
    return max( myList )


  #########
  #  MIN  #
  #########
  def min_agg( self, idList, pred ) :
    if DEBUG :
      print "... Running aggsPack MIN ..."
  
    myList = []
    for i in idList :
      if pred :
        parsedPred = pred.split(",")
        result     = self.checkPred( i, parsedPred )
        if DEBUG :
          print "result = " + str(result)
  
      if (pred == None) or not (result == None) :
        myList.append( result )
  
    return min( myList )


  #########
  #  SUM  #
  #########
  def sum_agg( self, idList, pred ) :
    if DEBUG :
      print "... Running aggsPack SUM ..."

    mySum = 0
    for i in idList :
      if pred :
        parsedPred = pred.split(",")
        result     = self.checkPred( i, parsedPred )
        if DEBUG :
          print "result = " + str(result)

      if (pred == None) or not (result == None) :
        mySum += result

    return mySum


  ##################
  #  PARSE RESULT  #
  ##################
  # assume results are collected in a dictionary for convenience.
  def parseResult( self, res, lhs ) :
    if DEBUG :
      print "res = " + str(res)
      print "lhs = " + lhs
      print "res[lhs] = " + str(res[lhs])
  
    return res[ lhs ]

  
  ################
  #  CHECK PRED  #
  ################
  # preds assumed of the form "lhs op rhs"
  # lhs is a single attribute
  # rhs is a value
  # op  is >, <, >=, <= == 
  def checkPred( self, ID, parsedPred ) :
    ret = False
  
    if len(parsedPred) > 0 :
      lhs = parsedPred[0]
      op  = parsedPred[1]
      rhs = parsedPred[2]
  
    ad        = Adapter.Adapter( self.nosql_type )
    resFull   = ad.get( ID, self.cursor )
    resTarget = self.parseResult( resFull, lhs )
  
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
#  EOF  #
#########
