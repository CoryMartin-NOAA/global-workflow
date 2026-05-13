
#!/usr/bin/env python3
# exglobal_prep_ocean_obs.py
# This script collects and preprocesses ocean and seaice observations for global marine assimilation.
import os

from wxflow import AttrDict, Logger, cast_strdict_as_dtypedict, parse_j2yaml
from pygfs.task.marine_prepobs import MarineObsPrep
from pygfs.task.marine_bufr_prepobs import MarineBufrObsPrep

# Initialize root logger
logger = Logger(level='DEBUG', colored_log=True)


if __name__ == '__main__':
	# Take configuration from environment and cast it as python dictionary
	config_env = cast_strdict_as_dtypedict(os.environ)

	# Take configuration from YAML file to augment/append config dict
	config_yaml = parse_j2yaml(os.path.join(config_env['HOMEobsforge'], 'parm', 'config.yaml'), config_env)
	# Extract obsforge specific configuration
	obsforge_dict = {}
	for key, value in config_yaml['obsforge'].items():
		if key not in config_env.keys():
			obsforge_dict[key] = value

	# --- Marine Dump Task ---
	config_marine = AttrDict(**config_env, **obsforge_dict)
	config_marine = AttrDict(**config_marine, **config_yaml['marinedump'])

	marineObs = MarineObsPrep(config_marine)
	logger.info('Starting MarineObsPrep (marine dump) task...')
	marineObs.initialize()
	marineObs.execute()
	marineObs.finalize()
	logger.info('MarineObsPrep (marine dump) task completed.')

	# --- Marine BUFR Dump Task ---
	# Load BUFR-specific config YAML
	bufr_yaml_path = os.path.join(config_env['HOMEobsforge'], 'parm', 'marine_bufr_dump_config.yaml')
	task_yaml = parse_j2yaml(bufr_yaml_path, config_env)

	config_bufr = AttrDict(**config_env, **obsforge_dict)
	config_bufr = AttrDict(**config_bufr, **task_yaml['marinebufrdump'])

	marineBufrObs = MarineBufrObsPrep(config_bufr)
	logger.info('Starting MarineBufrObsPrep (marine bufr dump) task...')
	marineBufrObs.initialize()
	marineBufrObs.execute()
	marineBufrObs.finalize()
	logger.info('MarineBufrObsPrep (marine bufr dump) task completed.')
