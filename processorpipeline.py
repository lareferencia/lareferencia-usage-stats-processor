#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Pipeline classes  """

from abc import abstractmethod, ABC
from configcontext import ConfigurationContext
from typing import List
import logging
import traceback
logger = logging.getLogger()

## Class for data transfer between pipeline stages
class UsageStatsData:

    _data_dict = {}

    def __init__(self):
        None

    def __getattr__(self, attribute):
        return self._data_dict.get(attribute, None)

    def __setattr__(self, name, value):
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

## Class for pipeline build and processing
class UsageStatsProcessorPipeline:

    _input_stage = None
    _filters_stage = []
    _output_stage = None

    def __init__(self, input: AbstractUsageStatsPipelineStage , filters: List[AbstractUsageStatsPipelineStage], outputs: AbstractUsageStatsPipelineStage):
        self._input_stage = input
        self._filters_stage = filters
        self._output_stage = outputs

    def run(self):
        data = self._input_stage.run()

        for filter in self._filters_stage:
            data = filter.run(data)            

        try:
            self._output_stage.run(data)
        except Exception as e: 
            logger.error( 'A fatal exception ocurred processing data !!!! {}'.format(e) )
            traceback.print_exc()


