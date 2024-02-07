#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
logger = logging.getLogger()

import configparser

GENERAL = 'GENERAL'
LABELS = 'LABELS'
ACTIONS = 'ACTIONS'

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

      if GENERAL in self._config:
         if ACTIONS in self._config[GENERAL]:
            self.actions = list(map(lambda x: x.strip(), self._config[GENERAL][ACTIONS].split(',')))   
         else:
            logger.error("No ACTIONS section in configuration file")
            raise Exception("No ACTIONS section in configuration file")
      else:
         logger.error("No GENERAL section in configuration file")
         raise Exception("No GENERAL section in configuration file")
      
   
   def getConfig(self, section=None, option=None):

      print("Get config %s %s" % (section, option))

      if section not in self._config.sections():
         raise Exception("Section %s not found" % section)
      
      if option not in self._config[section]:
         raise Exception("Option %s not found" % option)

      return self._config[section][option]
   
   def getArg(self, name):

      if name not in self._commandLineArgs:
         raise Exception("Argument %s not found" % name)
      else:
         return self._commandLineArgs[name]
   
   def getActions(self):
      return self.actions
   
   def getLabel(self, label):
      try:
         return self._config[LABELS][label]
      except:
         raise Exception("Label %s not found" % label)
      
        
   

   

      
        
       
