#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger()

import configparser

class ConfigurationContext:

   def __init__(self, commandLineArgs):
      self._commandLineArgs = commandLineArgs

      config_file_path = self._commandLineArgs.get('config_file_path', None)
      
      if config_file_path is None:
         logger.error("No configuration file specified")
         raise Exception("No configuration file specified")
      else:
         self._config = configparser.ConfigParser()
         self._config.read(config_file_path)
   
   def getConfig(self):
      return self._config
   
   def getArgs(self):
      return self._commandLineArgs
   

   

      
        
       
