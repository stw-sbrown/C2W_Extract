#!/bin/ksh
###############################################################################
# 
# Author	S.Badhan
# Subversion $Revision: 5304 $
# 
# 0.1		S.Badhan  -- Inital Build.
###############################################################################

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


printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/FIN/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1


printf "\nProceeding with FIN batch run\n"

# RUN GATHER STATS ON Reception tables for FINTRAN
printf "\nRun stats on RECEPTION tables used by FINTRAN\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINSRECEPTION.sql >> /recload/$1/FIN/LOGS/DAILY_BATCH/BATCH-FIN-$1-$timestamp.log 2>&1

# Execute procedure which runs the TARIFF build and the MAIN batch.  Truncates are done in this script as well.
printf "\nRun FINTRAN batch\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/FINTRAN_BATCH_START.sql >> /recload/$1/FIN/LOGS/DAILY_BATCH/BATCH-FIN-$1-$timestamp.log 2>&1

# RUN GATHER STATS ON FINTRAN
printf "\nRun stats on FINTRAN\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/DAILY_BATCH/BATCH-FIN-$1-$timestamp.log 2>&1

# Execute procedure which runs the DELIVERY build.  
printf "\nRun FINDEL batch\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus FINDEL/$1 @/recload/$1/FIN/CODE_STORE/FINDEL_BATCH_START.sql >> /recload/$1/FIN/LOGS/DAILY_BATCH/BATCH-FIN-$1-$timestamp.log 2>&1


# Get updated timstamp for log
endtime=`date '+%Y_%m_%d__%H_%M'`;
printf "\n\n\nBatch process completed at $endtime" >> /recload/$1/FIN/LOGS/DAILY_BATCH/BATCH-FIN-$1-$timestamp.log 2>&1

