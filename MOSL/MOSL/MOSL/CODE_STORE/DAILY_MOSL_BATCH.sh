#!/bin/ksh
###############################################################################
# 
# Author	N.Henderson
# Version	0.3
# Subversion $Revision: 4023 $
# Rebuild packages and procedures, then execute the main batch.  
# Main batch also truncates tables
# 0.1		N.Henderson. -- Initial script.  Added some more logging features
# 0.2		M.Marron     -- Altered script for MOUTRAN & MOUDEL batch runs
# 0.3		M.Marron     -- Updated the script to run all enviroments
#################################################################################



clear

# Clean up
if [[ -e "*.log" ]]; then
	rm *.log
fi



# Check if we have a couple of parameters before we continue

if ! [[ "$1" == "DOWP" || "$1" == "DOWS" || "$1" == "DOWD" ]] ; then
	echo ""
	echo "Error, should be DOWP, DOWS or DOWD"
	exit 2;
fi

if ! [[ "$1" == "$ORACLE_SID" ]] ; then
	echo ""
	echo "Error, you tried to run against $1 and your ORACLE_SID is $ORACLE_SID"
	exit 3;
fi


#Set ORACLE environment
ORACLE_SID=$1; export ORACLE_SID
ORACLE_HOME=/oravwa/11.2.0.4 ; export ORACLE_HOME
TNS_ADMIN=/oravwa/11.2.0.4/network/admin ; export TNS_ADMIN


# Get the current date and time into a variable
timestamp=`date '+%Y_%m_%d__%H_%M'`;


#  Set ORACLE specifics
ORACLE_SID=$1; export ORACLE_SID
ORACLE_HOME=/oravwa/11.2.0.4 ; export ORACLE_HOME
TNS_ADMIN=/oravwa/11.2.0.4/network/admin ; export TNS_ADMIN

# Get the current date and time into a variable 
timestamp=`date '+%Y_%m_%d__%H_%M'`;


printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1


printf "\nProceeding with build\n"


# Execute procedure which runs the TARIFF build and the MAIN batch.  Truncates are done in this script as well.
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/MOUTRAN_BATCH_START.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

# Execute procedure which runs the DELIVERY build.  
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/MOUDEL_BATCH_START.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1


# Get updated timstamp for log
endtime=`date '+%Y_%m_%d__%H_%M'`;
printf "\n\n\nBatch process completed at $endtime" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

