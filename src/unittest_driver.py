#!/usr/bin/env python

import copy, os, pickledb, string, sys, unittest

#####################
#  UNITTEST DRIVER  #
#####################
def unittest_driver() :

  print
  print "***************************************"
  print "*   RUNNING TEST SUITE FOR AGGSPACK   *"
  print "***************************************"
  print

  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_avg_mongodb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_count_mongodb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_max_mongodb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_min_mongodb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_sum_mongodb" )

  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_avg_pickledb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_count_pickledb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_max_pickledb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_min_pickledb" )
  os.system( "python -m unittest Test_aggspack.Test_aggspack.test_sum_pickledb" )


#########################
#  THREAD OF EXECUTION  #
#########################
unittest_driver()


#########
#  EOF  #
#########
