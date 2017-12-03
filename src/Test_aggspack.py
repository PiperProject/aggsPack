#!/usr/bin/env python

'''
Test_aggspack.py
'''

#############
#  IMPORTS  #
#############
# standard python packages
import logging, os, pickledb, sys, time, unittest

from pymongo import MongoClient

import AggsPack


###################
#  TEST AGGSPACK  #
###################
class Test_aggspack( unittest.TestCase ) :

  logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.DEBUG )
  #logging.basicConfig( format='%(levelname)s:%(message)s', level=logging.INFO )

  ################
  #  ATTRIBUTES  #
  ################

  MONGODB_PATH = os.path.abspath( __file__ + "/../dbtmp")
  CURR_PATH    = os.path.abspath( __file__ + "/..")


  #################
  #  AVG MONGODB  #
  #################
  # tests AVG on a mongodb instance
  def test_avg( self ) :

    test_id    = "test_avg"
    NOSQL_TYPE = "mongodb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing mongodb instance." )

    self.createInstance_mongodb()

    # wait 5 seconds for the db to set
    time.sleep( 5 )

    client = MongoClient()
    dbInst = client.bookdb

    # --------------------------------------------------------------- #
    # insert data

    book1 = { "author" : "Elsa Menzel",  \
              "title" : "A Martial Arts Primer", \
              "pubYear" : 2018, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 10 }
    book2 = { "author" : "Anna Summers", \
              "title" : "The Rising", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy"], \
              "cost(Dollars)" : 9.99 }
    book3 = { "author" : "Kat Green", \
              "title" : "Frozen Space", \
              "pubYear" : 2017, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }
  
    b    = dbInst.bookdb
    bid1 = b.insert_one( book1 ).inserted_id
    bid2 = b.insert_one( book2 ).inserted_id
    bid3 = b.insert_one( book3 ).inserted_id

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, b )

    print "********************\nRunning COUNT : aggspack_op.count(     [ bid1, bid2 ], 'pubYear,>,2000' )\n" + str( aggspack_op.count(   [ bid1, bid2 ], "pubYear,>,2000" ) )
    print "********************\nRunning SUM   : aggspack_op.sum_agg( [ bid1, bid2 ], 'pubYear,>,2000' )\n" + str( aggspack_op.sum_agg( [ bid1, bid2 ], "pubYear,>,2000" ) )
    print "********************\nRunning AVG   : aggspack_op.average( [ bid1, bid2 ], 'pubYear,>,2000' )\n" + str( aggspack_op.average( [ bid1, bid2 ], "pubYear,>,2000" ) )
    print "********************\nRunning MIN   : aggspack_op.min_agg( [ bid1, bid2 ], 'pubYear,>,2000' )\n" + str( aggspack_op.min_agg( [ bid1, bid2 ], "pubYear,>,2000" ) )
    print "********************\nRunning MAX   : aggspack_op.max_agg( [ bid1, bid2 ], 'pubYear,>,2000' )\n" + str( aggspack_op.max_agg( [ bid1, bid2 ], "pubYear,>,2000" ) )

    # ---------------------------------------------------- #
    # drop collections

    dbInst.bookdb.drop()

    # ---------------------------------------------------- #
    # close mongo db instance

    client.close()
  
    # get instance id
    os.system( "pgrep mongod 2>&1 | tee dbid.txt" )
    fo     = open( "dbid.txt", "r" )
    dbid   = fo.readline()
    fo.close()
    os.system( "rm " + self.CURR_PATH + "/dbid.txt" )
  
    logging.debug( "TEST_AVG : dbid = " + dbid )
  
    os.system( "kill " + dbid )



  #############################
  #  CREATE INSTANCE MONGODB  #
  #############################
  def createInstance_mongodb( self ) :
  
    logging.info( "CREATE INSTANCE MONGODB : Creating mongo db instance at " + self.MONGODB_PATH + "\n\n" )
  
    # establsih clean target dir for db
    if not os.path.exists( self.MONGODB_PATH ) :
      os.system( "mkdir " + self.MONGODB_PATH + " ; " )
    else :
      os.system( "rm -rf " + self.MONGODB_PATH + " ; " )
      os.system( "mkdir " + self.MONGODB_PATH + " ; " )
  
    # build mongodb instance
    os.system( "mongod --dbpath " + self.MONGODB_PATH + " &" )


#########
#  EOF  #
#########
