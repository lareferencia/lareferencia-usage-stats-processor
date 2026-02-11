#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Pipeline classes  """

from abc import abstractmethod, ABC
from configcontext import ConfigurationContext
from typing import List
import logging
import traceback
logger = logging.getLogger()

## Function to get class from class path
def get_class(class_path):
    groups = class_path.split('.') 

    if len(groups) == 1:
        return globals()[class_path]
    else: 
        if len(groups) == 2:
            module_path = ".".join(groups[:-1]) 
            class_name = groups[-1]

            module = __import__( module_path )
            return getattr(module, class_name )
        else:
            raise Exception("Invalid class path {}".format(class_path) )


## Class for data transfer between pipeline stages
class UsageStatsData:

    def __init__(self):
        object.__setattr__(self, "_data_dict", {})

    def __getattr__(self, attribute):
        return self._data_dict.get(attribute, None)

    def __setattr__(self, name, value):
        if name == "_data_dict":
            object.__setattr__(self, name, value)
            return
        self._data_dict[name] = value

    def __str__(self):
        return self._data_dict.__str__()

## Abstract class for pipeline stages
class AbstractUsageStatsPipelineStage(ABC):

    def __init__(self, configContext: ConfigurationContext):
        self._configContext = configContext

    @abstractmethod
    def run(self, data: UsageStatsData) -> UsageStatsData:
        pass

    def getCtx(self):
        return self._configContext

## Class for pipeline build and processing
class UsageStatsProcessorPipeline:

    _input_stage = None
    _filters_stage = []
    _output_stage = None

    def __init__(self, configContext:ConfigurationContext, input: str, filters: List[str], output: str):

        self._input_stage = get_class(input)(configContext)
        self._filters_stage = [ get_class(filter)(configContext) for filter in filters ]
        self._output_stage = get_class(output)(configContext)

    def run(self):
        data = self._input_stage.run(UsageStatsData())

        for filter in self._filters_stage:
            data = filter.run(data)            

        try:
            return self._output_stage.run(data)
        except Exception as e: 
            logger.error( 'A fatal exception ocurred processing data !!!! {}'.format(e) )
            traceback.print_exc()

