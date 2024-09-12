#!/usr/bin/env python3
# exglobal_stat_analysis.py
# This script creates an StatAnalysis class
# and runs the initialize method
# which create and stage the runtime directory
# and create the YAML configuration
# for a global stat analysis
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pygfs.task.stat_analysis import StatAnalysis

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    # Instantiate the atm analysis task
    StatAnl = StatAnalysis(config, 'statanlvar')

    # Initialize JEDI variational analysis
    StatAnl.initialize_jedi()
    StatAnl.initialize_analysis()

