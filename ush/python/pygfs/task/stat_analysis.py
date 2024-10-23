#!/usr/bin/env python3

import os
import glob
import gzip
import tarfile
import yaml
from logging import getLogger
from pprint import pformat
from typing import Optional, Dict, Any

from wxflow import (AttrDict,
                    FileHandler,
                    add_to_datetime, to_fv3time, to_timedelta, to_YMDH,
                    Task,
                    parse_j2yaml, save_as_yaml,
                    logit)
from pygfs.jedi import Jedi

logger = getLogger(__name__.split('.')[-1])


class StatAnalysis(Task):
    """
    Class for JEDI-based global stat analysis tasks
    """
    @logit(logger, name="StatAnalysis")
    def __init__(self, config: Dict[str, Any], yaml_name: Optional[str] = None):
        """
        Constructor global stat analysis task

        This method will construct a global stat analysis task.
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

        _res = int(self.task_config.CASE[1:])
        # _res_anl = int(self.task_config.CASE_ANL[1:])
        _window_begin = add_to_datetime(self.task_config.current_cycle, -to_timedelta(f"{self.task_config.assim_freq}H") / 2)
        print(_window_begin)

        # Create a local dictionary that is repeatedly used across this class
        local_dict = AttrDict(
            {
                'npx_ges': _res + 1,
                'npy_ges': _res + 1,
                'npz_ges': self.task_config.LEVS - 1,
                'npz': self.task_config.LEVS - 1,
                # 'npx_anl': _res_anl + 1,
                # 'npy_anl': _res_anl + 1,
                'npz_anl': self.task_config.LEVS - 1,
                'ATM_WINDOW_BEGIN': _window_begin,
                'ATM_WINDOW_LENGTH': f"PT{self.task_config.assim_freq}H",
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'GPREFIX': f"gdas.t{self.task_config.previous_cycle.hour:02d}z.",
                'OBSPACE_YAML': "/scratch1/NCEPDEV/da/Kevin.Dougherty/global-workflow/parm/stat/obspace_stat.yaml"
            }
        )

        # Extend task_config with local_dict
        self.task_config = AttrDict(**self.task_config, **local_dict)

        # Create JEDI object
        self.jedi = Jedi(self.task_config, yaml_name)

    @logit(logger)
    def initialize_jedi(self):
        """Initialize JEDI application

        This method will initialize a JEDI application used in the global stat analysis.
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

        # get JEDI-to-FV3 increment converter config and save to YAML file
        logger.info(f"Generating JEDI YAML config: {self.jedi.yaml}")
        self.jedi.set_config(self.task_config)
        logger.debug(f"JEDI config:\n{pformat(self.jedi.config)}")

        # save JEDI config to YAML file
        logger.debug(f"Writing JEDI YAML config to: {self.jedi.yaml}")
        save_as_yaml(self.jedi.config, self.jedi.yaml)

        # link JEDI executable
        logger.info(f"Linking JEDI executable {self.task_config.JEDIEXE} to {self.jedi.exe}")
        self.jedi.link_exe(self.task_config)

    @logit(logger)
    def initialize_analysis(self) -> None:
        """
        Initialize a global stat analysis

        This method will initialize a global stat analysis.
        This includes:
        - copying stat files

        Parameters
        ----------
        None

        Returns
        ----------
        None
        """
        super().initialize()

        logger.info(f"Copying files to {self.task_config.DATA}/stats")

        # Copy stat files to DATA path
        aerostat = os.path.join(self.task_config.COM_CHEM_ANALYSIS, f"{self.task_config['APREFIX']}aerostat")
        dest = os.path.join(self.task_config.DATA, "aerostats")
        statlist = [[aerostat, dest]]
        FileHandler({'copy': statlist}).sync()

        # Open tar file
        logger.info(f"Open tarred stat file in {dest}")
        with tarfile.open(dest, "r") as tar:
            # Extract all files to the current directory
            tar.extractall()

        # Gunzip .nc files
        logger.info("Gunzip files from tar file")
        gz_files = glob.glob(os.path.join(self.task_config.DATA, "*gz"))

        for diagfile in gz_files:
            with gzip.open(diagfile, 'rb') as f_in:
                with open(diagfile[:-3], 'wb') as f_out:
                    f_out.write(f_in.read())

        # Get list of .nc4 files
        obs_space_paths = glob.glob(os.path.join(self.task_config.DATA, "*.nc4"))

        for path in obs_space_paths:
            filename = os.path.basename(path)
            obspace = '_'.join(filename.split('_')[1:3])

            # Load g-w obs space intermediate yaml file
            with open(self.task_config.OBSPACE_YAML, 'r') as yaml_file:
                parsed_yaml_file = yaml.safe_load(yaml_file)

            obs_space_template = parsed_yaml_file['obs space'][obspace]['path']

            # Load specific GDASApp yaml file
            with open(obs_space_template, 'r') as obspace_yaml:
                parsed_obspace_yaml_file = yaml.safe_load(obspace_yaml)

            print("open obspace yaml and do stuff")