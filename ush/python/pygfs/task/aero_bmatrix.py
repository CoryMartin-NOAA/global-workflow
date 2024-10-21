#!/usr/bin/env python3

import os
from logging import getLogger
from pprint import pformat
from typing import List, Dict, Any, Union, Optional

from wxflow import (AttrDict, FileHandler, rm_p,
                    add_to_datetime, to_fv3time, to_timedelta,
                    to_fv3time, chdir, Executable, WorkflowException,
                    parse_j2yaml, save_as_yaml, logit, Task)
from pygfs.jedi import Jedi

logger = getLogger(__name__.split('.')[-1])


class AerosolBMatrix(Task):
    """
    Class for global aerosol BMatrix tasks
    """
    @logit(logger, name="AerosolBMatrix")
    def __init__(self, config: Dict[str, Any], yaml_name: Optional[str] = None):
        """Constructor global aero analysis bmatrix task

        This method will construct a global aero bmatrix task object.
        This includes:
        - extending the task_config attribute AttrDict to include parameters required for this task
        - instantiate the Jedi attribute object

        Parameters
        ----------
        config: Dict
            dictionary object containing task configuration
        yaml_name: str, optional
            name of YAML file for JEDI configuration

        Returns
        ----------
        None
        """
        super().__init__(config)

        _res = int(self.task_config['CASE'][1:])
        _res_anl = int(self.task_config['CASE_ANL'][1:])
        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config['assim_freq']}H") / 2)

        # Create a local dictionary that is repeatedly used across this class
        local_dict = AttrDict(
            {
                'npx_ges': _res + 1,
                'npy_ges': _res + 1,
                'npz_ges': self.task_config.LEVS - 1,
                'npz': self.task_config.LEVS - 1,
                'npx_anl': _res_anl + 1,
                'npy_anl': _res_anl + 1,
                'npz_anl': self.task_config['LEVS'] - 1,
                'AERO_WINDOW_BEGIN': _window_begin,
                'AERO_WINDOW_LENGTH': f"PT{self.task_config['assim_freq']}H",
                'aero_bkg_fhr': map(int, str(self.task_config['aero_bkg_times']).split(',')),
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'GPREFIX': f"gdas.t{self.task_config.previous_cycle.hour:02d}z.",
                'aero_obsdatain_path': f"{self.task_config.DATA}/obs/",
                'aero_obsdataout_path': f"{self.task_config.DATA}/diags/",
            }
        )

        # task_config is everything that this task should need
        self.task_config = AttrDict(**self.task_config, **local_dict)

        # Create JEDI object
        self.jedi = Jedi(self.task_config, yaml_name)

    @logit(logger)
    def initialize_jedi(self, algorithm: Optional[str] = None):
        """Initialize JEDI application

        This method will initialize a JEDI application used in the global aero analysis.
        This includes:
        - generating and saving JEDI YAML config
        - linking the JEDI executable

        Parameters
        ----------
        None

        Returns
        ----------
        None
        """

        # get JEDI config
        logger.info(f"Generating JEDI YAML config: {self.jedi.yaml}")
        self.jedi.set_config(self.task_config, algorithm)
        logger.debug(f"JEDI config:\n{pformat(self.jedi.config)}")

        # save JEDI config to YAML file
        logger.debug(f"Writing JEDI YAML config to: {self.jedi.yaml}")
        save_as_yaml(self.jedi.config, self.jedi.yaml)

        # link JEDI executable
        logger.info(f"Linking JEDI executable {self.task_config.JEDIEXE} to {self.jedi.exe}")
        self.jedi.link_exe(self.task_config)

    @logit(logger)
    def initialize_genb(self) -> None:
        """Initialize a global aerosol bmatrix

        This method will initialize a global aerosol bmatrix using JEDI.
        This includes:
        - staging fix files
        - staging model backgrounds
        """
        # stage fix files
        logger.info(f"Staging JEDI fix files from {self.task_config.JEDI_FIX_YAML}")
        jedi_fix_list = parse_j2yaml(self.task_config.JEDI_FIX_YAML, self.task_config)
        FileHandler(jedi_fix_list).sync()

        # stage backgrounds
        logger.info(f"Staging backgrounds prescribed from {self.task_config.AERO_BMATRIX_STAGE_TMPL}")
        aero_bmat_stage_list = parse_j2yaml(self.task_config.AERO_BMATRIX_STAGE_TMPL, self.task_config)
        FileHandler(aero_bmat_stage_list).sync()

    @logit(logger)
    def execute(self, aprun_cmd: str, jedi_args: Optional[str] = None) -> None:
        """Run JEDI executable

        This method will run JEDI executables for the global aero analysis

        Parameters
        ----------
        aprun_cmd : str
           Run command for JEDI application on HPC system
        jedi_args : List
           List of additional optional arguments for JEDI application

        Returns
        ----------
        None
        """

        if jedi_args:
            logger.info(f"Executing {self.jedi.exe} {' '.join(jedi_args)} {self.jedi.yaml}")
        else:
            logger.info(f"Executing {self.jedi.exe} {self.jedi.yaml}")

        self.jedi.execute(self.task_config, aprun_cmd, jedi_args)

    @logit(logger)
    def finalize(self) -> None:
        """Finalize a global aerosol bmatrix

        This method will finalize a global aerosol bmatrix using JEDI.
        This includes:
        - copying the bmatrix files to COM
        - copying YAMLs to COM

        """
        # save files to COMOUT
        logger.info(f"Saving files to COMOUT based on {self.task_config.AERO_BMATRIX_FINALIZE_TMPL}")
        aero_bmat_finalize_list = parse_j2yaml(self.task_config.AERO_BMATRIX_FINALIZE_TMPL, self.task_config)
        FileHandler(aero_bmat_finalize_list).sync()
