#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger()

import configparser

class ConfigurationContext:

   def __init__(self, commandLineArgs):
      self._commandLineArgs = commandLineArgs

      if commandLineArgs.config_file is None:
         logger.error("No configuration file specified")
         raise Exception("No configuration file specified")
      else:
         self._config = configparser.ConfigParser()
         self._config.read(commandLineArgs.config_file)
   
   def getConfig(self):
      return self._config
   
   def getArgs(self):
      return self._commandLineArgs
   

   

      
        
       
