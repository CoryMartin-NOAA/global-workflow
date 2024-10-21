#!/usr/bin/env python3
# exgdas_aero_analysis_generate_bmatrix.py
# This script creates an AerosolBMatrix object
# and runs the methods needed
# to stage files, compute the variance, and write to com
# files needed for the variational solver
import os

from wxflow import Logger, cast_strdict_as_dtypedict
from pygfs.task.aero_bmatrix import AerosolBMatrix

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':

    # Take configuration from environment and cast it as python dictionary
    config = cast_strdict_as_dtypedict(os.environ)

    # This job is going to run 3 different executables,
    # we create a BMatrix object for all 3 but only stage and finalize once each
    config['JEDIEXE'] = os.path.join(config['EXECgfs'], 'gdas.x')
    BkgInterp = AerosolBMatrix(config, 'aero_convertstate')
    BkgInterp.initialize_jedi('aero_convert_background')
    config['JEDIEXE'] = os.path.join(config['EXECgfs'], 'gdas_fv3jedi_error_covariance_toolbox.x')
    BDiffusion = AerosolBMatrix(config, 'aero_diffusion')
    BDiffusion.initialize_jedi('aero_gen_bmatrix_diffusion')
    config['JEDIEXE'] = os.path.join(config['EXECgfs'], 'gdasapp_chem_diagb.x')
    BVariance = AerosolBMatrix(config, 'aero_diagb')
    BVariance.initialize_jedi('aero_gen_bmatrix_diagb')

    # Initialize the runtime directory
    BVariance.initialize_genb()

    # Execute the 3 different utilities
    BkgInterp.execute(config.APRUN_AEROANLGENB, ['fv3jedi', 'convertstate'])
    BDiffusion.execute(config.APRUN_AEROANLGENB)
    BVariance.execute(config.APRUN_AEROANLGENB)

    # Finalize the generate B matrix task
    BVariance.finalize()
