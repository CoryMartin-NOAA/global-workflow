#!/usr/bin/env python3

import os
import glob
import gzip
import tarfile
from logging import getLogger
from pprint import pformat
from netCDF4 import Dataset
from typing import Dict, List, Any, Optional

from wxflow import (AttrDict,
                    FileHandler,
                    add_to_datetime, to_fv3time, to_timedelta,
                    chdir,
                    to_fv3time,
                    Task,
                    YAMLFile, parse_j2yaml, save_as_yaml,
                    logit,
                    Executable,
                    WorkflowException)
from pygfs.jedi import Jedi

logger = getLogger(__name__.split('.')[-1])


class AerosolAnalysis(Task):
    """
    Class for JEDI-based global aerosol analysis tasks
    """
    @logit(logger, name="AerosolAnalysis")
    def __init__(self, config: Dict[str, Any], yaml_name: Optional[str] = None):
        """Constructor global aero analysis task

        This method will construct a global aero analysis task.
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
                'aero_bkg_fhr': self.task_config['aero_bkg_times'],
                'OPREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'APREFIX': f"{self.task_config.RUN}.t{self.task_config.cyc:02d}z.",
                'GPREFIX': f"gdas.t{self.task_config.previous_cycle.hour:02d}z.",
                'aero_obsdatain_path': f"{self.task_config.DATA}/obs/",
                'aero_obsdataout_path': f"{self.task_config.DATA}/diags/",
                'BKG_TSTEP': "PT1H"  # Placeholder for 4D applications
            }
        )

        # Extend task_config with local_dict
        self.task_config = AttrDict(**self.task_config, **local_dict)

        # Create JEDI object
        self.jedi = Jedi(self.task_config, yaml_name)

    @logit(logger)
    def initialize_jedi(self):
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
        """Initialize a global aerosol analysis

        This method will initialize a global aerosol analysis using JEDI.
        This includes:
        - staging observation files
        - staging bias correction files
        - staging CRTM fix files
        - staging FV3-JEDI fix files
        - staging B error files
        - staging model backgrounds
        - creating output directories
        """
        super().initialize()
        # stage observations
        logger.info(f"Staging list of observation files generated from JEDI config")
        obs_dict = self.jedi.get_obs_dict(self.task_config)
        FileHandler(obs_dict).sync()
        logger.debug(f"Observation files:\n{pformat(obs_dict)}")

        # stage CRTM fix files
        logger.info(f"Staging CRTM fix files from {self.task_config.CRTM_FIX_YAML}")
        crtm_fix_list = parse_j2yaml(self.task_config.CRTM_FIX_YAML, self.task_config)
        FileHandler(crtm_fix_list).sync()

        # stage fix files
        logger.info(f"Staging JEDI fix files from {self.task_config.JEDI_FIX_YAML}")
        jedi_fix_list = parse_j2yaml(self.task_config.JEDI_FIX_YAML, self.task_config)
        FileHandler(jedi_fix_list).sync()

        # stage files from COM and create working directories
        logger.info(f"Staging files prescribed from {self.task_config.AERO_STAGE_VARIATIONAL_TMPL}")
        aero_var_stage_list = parse_j2yaml(self.task_config.AERO_STAGE_VARIATIONAL_TMPL, self.task_config)
        FileHandler(aero_var_stage_list).sync()

        # generate variational YAML file
        logger.debug(f"Generate variational YAML file: {self.task_config.jedi_yaml}")
        save_as_yaml(self.task_config.jedi_config, self.task_config.jedi_yaml)
        logger.info(f"Wrote variational YAML to: {self.task_config.jedi_yaml}")

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
        """Finalize a global aerosol analysis

        This method will finalize a global aerosol analysis using JEDI.
        This includes:
        - tarring up output diag files and place in ROTDIR
        - copying the generated YAML file from initialize to the ROTDIR
        - copying the guess files to the ROTDIR
        - applying the increments to the original RESTART files
        - moving the increment files to the ROTDIR

        """
        # ---- tar up diags
        # path of output tar statfile
        logger.info('Preparing observation space diagnostics for archiving')
        aerostat = os.path.join(self.task_config.COMOUT_CHEM_ANALYSIS, f"{self.task_config['APREFIX']}aerostat")

        # get list of diag files to put in tarball
        diags = glob.glob(os.path.join(self.task_config['DATA'], 'diags', 'diag*nc4'))

        # gzip the files first
        for diagfile in diags:
            logger.info(f'Adding {diagfile} to tar file')
            with open(diagfile, 'rb') as f_in, gzip.open(f"{diagfile}.gz", 'wb') as f_out:
                f_out.writelines(f_in)

        # ---- add increments to RESTART files
        logger.info('Adding increments to RESTART files')
        self._add_fms_cube_sphere_increments()

        # copy files back to COM
        logger.info(f"Copying files to COM based on {self.task_config.AERO_FINALIZE_VARIATIONAL_TMPL}")
        aero_var_final_list = parse_j2yaml(self.task_config.AERO_FINALIZE_VARIATIONAL_TMPL, self.task_config)
        FileHandler(aero_var_final_list).sync()

        # open tar file for writing
        with tarfile.open(aerostat, "w") as archive:
            for diagfile in diags:
                diaggzip = f"{diagfile}.gz"
                archive.add(diaggzip, arcname=os.path.basename(diaggzip))
        logger.info(f'Saved diags to {aerostat}')

    def clean(self):
        super().clean()

    @logit(logger)
    def _add_fms_cube_sphere_increments(self) -> None:
        """This method adds increments to RESTART files to get an analysis
        """
        if self.task_config.DOIAU:
            bkgtime = self.task_config.AERO_WINDOW_BEGIN
        else:
            bkgtime = self.task_config.current_cycle
        # only need the fv_tracer files
        restart_template = f'{to_fv3time(bkgtime)}.fv_tracer.res.tile{{tilenum}}.nc'
        increment_template = f'{to_fv3time(self.task_config.current_cycle)}.fv_tracer.res.tile{{tilenum}}.nc'
        inc_template = os.path.join(self.task_config.DATA, 'anl', 'aeroinc.' + increment_template)
        bkg_template = os.path.join(self.task_config.DATA, 'anl', restart_template)
        # get list of increment vars
        incvars_list_path = os.path.join(self.task_config['PARMgfs'], 'gdas', 'aeroanl_inc_vars.yaml')
        incvars = YAMLFile(path=incvars_list_path)['incvars']
        self.add_fv3_increments(inc_template, bkg_template, incvars)

    @logit(logger)
    def add_fv3_increments(self, inc_file_tmpl: str, bkg_file_tmpl: str, incvars: List) -> None:
        """Add cubed-sphere increments to cubed-sphere backgrounds

        Parameters
        ----------
        inc_file_tmpl : str
           template of the FV3 increment file of the form: 'filetype.tile{tilenum}.nc'
        bkg_file_tmpl : str
           template of the FV3 background file of the form: 'filetype.tile{tilenum}.nc'
        incvars : List
           List of increment variables to add to the background
        """

        for itile in range(1, self.task_config.ntiles + 1):
            inc_path = inc_file_tmpl.format(tilenum=itile)
            bkg_path = bkg_file_tmpl.format(tilenum=itile)
            with Dataset(inc_path, mode='r') as incfile, Dataset(bkg_path, mode='a') as rstfile:
                for vname in incvars:
                    increment = incfile.variables[vname][:]
                    bkg = rstfile.variables[vname][:]
                    anl = bkg + increment
                    rstfile.variables[vname][:] = anl[:]
                    try:
                        rstfile.variables[vname].delncattr('checksum')  # remove the checksum so fv3 does not complain
                    except (AttributeError, RuntimeError):
                        pass  # checksum is missing, move on