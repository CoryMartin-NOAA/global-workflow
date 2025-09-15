#!/usr/bin/env python3
# exglobal_prep_atm_ioda_obs.py
# This script either, depending on configuration,
# will process atmospheric observations into IODA format
# or will copy pre-processed observations from ObsForge
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pygfs.task.aero_prepobs import AerosolObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    # AeroObs = AerosolObsPrep(config)
    if config.DO_CONVERT_IODA:
        print('processing observations into IODA format')
    else:
        # just sync files from COMINobsforge
        print('syncing files from COMINobsforge')