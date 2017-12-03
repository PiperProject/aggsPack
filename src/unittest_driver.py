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

  os.system( "python -m unittest Test_aggsPack.Test_aggsPack.test_avg" )


#########################
#  THREAD OF EXECUTION  #
#########################
unittest_driver()


#########
#  EOF  #
#########
