#! /usr/bin/env bash

export STRICT="NO"

###############################################################
# Source UFSDA workflow modules
source "${HOMEgfs}/dev/ush/load_modules.sh" ufsda
status=$?
if [[ ${status} -ne 0 ]]; then
    exit "${status}"
fi

export job="marineobsbufrdump"
export jobid="${job}.$$"

###############################################################
# Execute the JJOB
"${HOMEgfs}"/dev/jobs/JGLOBAL_MARINE_OBS_BUFR_DUMP
status=$?
exit "${status}"
