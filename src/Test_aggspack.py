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


  ##################
  #  AVG PICKLEDB  #
  ##################
  # tests AVG on a pickledb instance
  def test_avg_pickledb( self ) :

    test_id    = "test_avg_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

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
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }

    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )  

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, dbInst )

    self.assertEqual( 2017.0, aggspack_op.average( [ "bid1", "bid2", "bid3" ], "pubYear,>,2000" ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  ####################
  #  COUNT PICKLEDB  #
  ####################
  # tests COUNT on a pickledb instance
  def test_count_pickledb( self ) :

    test_id    = "test_count_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

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
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }

    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )  

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, dbInst )

    self.assertEqual( 1, aggspack_op.count( [ "bid1", "bid2", "bid3" ], "pubYear,>,2017" ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  ##################
  #  MAX PICKLEDB  #
  ##################
  # tests MAX on a pickledb instance
  def test_max_pickledb( self ) :

    test_id    = "test_max_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

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
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }

    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )  

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, dbInst )

    self.assertEqual( 2018, aggspack_op.max_agg( [ "bid1", "bid2", "bid3" ], "pubYear,>,2000" ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  ##################
  #  MIN PICKLEDB  #
  ##################
  # tests MIN on a pickledb instance
  def test_min_pickledb( self ) :

    test_id    = "test_min_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

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
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }

    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )  

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, dbInst )

    self.assertEqual( 2016, aggspack_op.min_agg( [ "bid1", "bid2", "bid3" ], "pubYear,>,2000" ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  ##################
  #  SUM PICKLEDB  #
  ##################
  # tests SUM on a pickledb instance
  def test_sum_pickledb( self ) :

    test_id    = "test_sum_pickledb"
    NOSQL_TYPE = "pickledb"

    # --------------------------------------------------------------- #
    # create db instance

    logging.info( "  " + test_id + ": initializing pickledb instance." )

    dbInst = pickledb.load( "./test_aggspack.db", False )

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
              "pubYear" : 2016, \
              "numCopies" : 0, \
              "categories" : ["fantasy", "scifi"], \
              "cost(Dollars)" : 0 }

    dbInst.set( "bid1", book1 )
    dbInst.set( "bid2", book2 )
    dbInst.set( "bid3", book3 )  

    # --------------------------------------------------------------- #
    # agg ops

    aggspack_op = AggsPack.AggsPack( NOSQL_TYPE, dbInst )

    self.assertEqual( 6051, aggspack_op.sum_agg( [ "bid1", "bid2", "bid3" ], "pubYear,>,2000" ) )

    # ---------------------------------------------------- #
    # close pickle db instance

    dbInst.deldb()


  #################
  #  AVG MONGODB  #
  #################
  # tests AVG on a mongodb instance
  def test_avg_mongodb( self ) :

    test_id    = "test_avg_mongodb"
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
              "pubYear" : 2016, \
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

    self.assertEqual( 2017.0, aggspack_op.average( [ bid1, bid2, bid3 ], "pubYear,>,2000" ) )

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
  
    logging.debug( "TEST_AVG_MONGODB : dbid = " + dbid )
  
    os.system( "kill " + dbid )


  ###################
  #  COUNT MONGODB  #
  ###################
  # tests COUNT on a mongodb instance
  def test_count_mongodb( self ) :

    test_id    = "test_count_mongodb"
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
              "pubYear" : 2016, \
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

    self.assertEqual( 1, aggspack_op.count( [ bid1, bid2, bid3 ], "pubYear,>,2017" ) )

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
  
    logging.debug( "TEST_COUNT_MONGODB : dbid = " + dbid )
  
    os.system( "kill " + dbid )


  #################
  #  MAX MONGODB  #
  #################
  # tests MAX on a mongodb instance
  def test_max_mongodb( self ) :

    test_id    = "test_max_mongodb"
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
              "pubYear" : 2016, \
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

    self.assertEqual( 2018, aggspack_op.max_agg( [ bid1, bid2, bid3 ], "pubYear,>,2000" ) )

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
  
    logging.debug( "TEST_MAX_MONGODB : dbid = " + dbid )
  
    os.system( "kill " + dbid )


  #################
  #  MIN MONGODB  #
  #################
  # tests MIN on a mongodb instance
  def test_min_mongodb( self ) :

    test_id    = "test_min_mongodb"
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
              "pubYear" : 2016, \
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

    self.assertEqual( 2016, aggspack_op.min_agg( [ bid1, bid2, bid3 ], "pubYear,>,2000" ) )

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
  
    logging.debug( "TEST_MAX_MONGODB : dbid = " + dbid )
  
    os.system( "kill " + dbid )


  #################
  #  SUM MONGODB  #
  #################
  # tests SUM on a mongodb instance
  def test_sum_mongodb( self ) :

    test_id    = "test_sum_mongodb"
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
              "pubYear" : 2016, \
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

    self.assertEqual( 6051, aggspack_op.sum_agg( [ bid1, bid2, bid3 ], "pubYear,>,2000" ) )

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
  
    logging.debug( "TEST_SUM_MONGODB : dbid = " + dbid )
  
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
