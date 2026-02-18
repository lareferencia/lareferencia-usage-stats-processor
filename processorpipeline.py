#!/usr/bin/env python
# -*- coding: utf-8 -*-
""" Pipeline classes  """

from abc import abstractmethod, ABC
from configcontext import ConfigurationContext
from typing import List
import logging
import traceback
import time
import datetime
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

    @staticmethod
    def _now():
        return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    @staticmethod
    def _len_or_none(value):
        if value is None:
            return None
        try:
            return len(value)
        except Exception:
            return None

    @classmethod
    def _snapshot(cls, data):
        events_rows = cls._len_or_none(getattr(data, "events_df", None))
        visits_rows = cls._len_or_none(getattr(data, "visits_df", None))
        agg_count = cls._len_or_none(getattr(data, "agg_dict", None))
        docs_count = cls._len_or_none(getattr(data, "documents", None))

        parts = []
        if events_rows is not None:
            parts.append(f"events={events_rows}")
        if visits_rows is not None:
            parts.append(f"visits={visits_rows}")
        if agg_count is not None:
            parts.append(f"identifiers={agg_count}")
        if docs_count is not None:
            parts.append(f"documents={docs_count}")

        return ", ".join(parts) if parts else "no-stats"

    def _run_stage(self, stage, stage_name, data):
        print(f"[{self._now()}] Stage start: {stage_name}")
        stage_start = time.time()
        output = stage.run(data)
        elapsed = time.time() - stage_start

        if output is None:
            print(f"[{self._now()}] Stage end: {stage_name} ({elapsed:.2f}s, output=None)")
            return output

        print(f"[{self._now()}] Stage end: {stage_name} ({elapsed:.2f}s, {self._snapshot(output)})")
        return output

    def run(self):
        pipeline_start = time.time()
        data = UsageStatsData()
        data = self._run_stage(self._input_stage, self._input_stage.__class__.__name__, data)

        for filter in self._filters_stage:
            if data is None:
                break
            data = self._run_stage(filter, filter.__class__.__name__, data)

        if data is None:
            print(f"[{self._now()}] Pipeline aborted before output stage (data=None)")
            return None

        try:
            data = self._run_stage(self._output_stage, self._output_stage.__class__.__name__, data)
            total_elapsed = time.time() - pipeline_start
            print(f"[{self._now()}] Pipeline finished ({total_elapsed:.2f}s)")
            return data
        except Exception as e:
            logger.error( 'A fatal exception ocurred processing data !!!! {}'.format(e) )
            traceback.print_exc()
            return None
