#!/bin/bash
# quick script to copy/move/link files around from the ICs staged dir to the ROTDIR

NDATE=${NDATE:-/apps/contrib/NCEPLIBS/orion/utils/prod_util.v1.2.0/exec/ndate}

# create ROTDIR
mkdir -p $ROTDIR/$CDUMP.$PDY/$cyc/atmos

# get time of future cycle for UFO/GSI
FDATE=$($NDATE +6 $CDATE)

# link previous cycle RESTART
BDATE=$($NDATE -6 $CDATE)
bPDY=$(echo $BDATE | cut -c1-8)
bcyc=$(echo $BDATE | cut -c9-10)

mkdir -p $ROTDIR/$CDUMP.$bPDY/$bcyc/atmos/
ln -sf $ICSDIR/$FDATE/$CDUMP.$bPDY/$bcyc/RESTART $ROTDIR/$CDUMP.$bPDY/$bcyc/atmos/.

# copy current cycle RESTART
cp -r $ICSDIR/$FDATE/$CDUMP.$PDY/$cyc/RESTART $ROTDIR/$CDUMP.$PDY/$cyc/atmos/.

# link increment
ln -sf $ICSDIR/$FDATE/$CDUMP.$PDY/$cyc/*atmi* $ROTDIR/$CDUMP.$PDY/$cyc/atmos/.
