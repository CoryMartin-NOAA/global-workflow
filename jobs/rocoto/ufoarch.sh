#!/bin/bash
# quick script to copy/move/link files around from the ROTDIR to the ARCHDIR

set -x

NDATE=${NDATE:-/apps/contrib/NCEPLIBS/orion/utils/prod_util.v1.2.0/exec/ndate}

# get time of future cycle for UFO/GSI
FDATE=$($NDATE +6 $CDATE)
fPDY=$(echo $FDATE | cut -c1-8)
fcyc=$(echo $FDATE | cut -c9-10)

# create ARCHDIR
mkdir -p $ARCHDIR/$FDATE/$CDUMP.$PDY/$cyc/

# move output from ROTDIR to ARCHDIR
mv -f $ROTDIR/$CDUMP.$PDY/$cyc/* $ARCHDIR/$FDATE/$CDUMP.$PDY/$cyc/.

# link bias corr files
mkdir -p $ARCHDIR/$FDATE/$CDUMP.$fPDY/$fcyc/atmos
ln -sf $ICSDIR/$FDATE/$CDUMP.$PDY/$cyc/*abias* $ARCHDIR/$FDATE/$CDUMP.$PDY/$cyc/atmos/.

# move the ROT to start fresh; can delete later if requested...
mkdir -p $ROTDIR/$CDATE
mv -f $ROTDIR/$CDUMP.* $ROTDIR/$CDATE
