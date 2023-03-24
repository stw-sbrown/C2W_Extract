#!/bin/ksh
###############################################################################
# 
# Author	N.Henderson
# Version	0.3
# Subversion $Revision: 6444 $
# Rebuild packages and procedures, then execute the main batch.  
# Main batch also truncates tables
# 0.1		N.Henderson. -- Initial script.  Added some more logging features
# 0.2		M.Marron     -- Altered script for MOUTRAN & MOUDEL batch runs
# 0.3		M.Marron     -- Updated the script to run all enviroments
# 0.4		D.Cheung     -- Add gather stats on MOUTRAN before MOUDEL batch
# 0.5 	D.Cheung     -- Add prompt to read in phase input
# 0.6 	S.Badhan     -- Remove phase 1 from phase input list
# 0.7   D.Cheung     -- Add call to Generate batch stats at end
# 0.8   K.Burton     -- Added call to 03_UPDATE_DWRCYMRU_SPIDS.sql
# 0.9   D.Cheung     -- Added processing to copy master MOSL files
# 0.10  S.Badhan     -- Remove phase 2 from phase input list
# 0.11  D.Cheung     -- Comment out gather stats
# 0.12  L.Smith      -- Import stats and recon
# 0.13  K.Burton     -- Added UPDATE_NOSPIDS script after MOUTRAN batch
# 0.14  S.Badhan     -- Point to DBA_SCRIPTS library for stats dmp files
# 0.15  S.Badhan     -- 01.14 removed. Point to base library
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

# PROMPT USER TO ENTER A PHASE NUMBER TO SELECT DATASET TO RUN
echo "Which phase number do you wish to run (enter number)?"
select phase in 3 4 5 6 7 8 9 "ALL" "Exit"; do
    case $phase in
        [3456789] ) break;;
        ALL ) break;;
        Exit ) exit;;
    esac
done

##################################         DROP STATS TABLES                ################################################
  printf "\nDrop stats tables"
  printf "\nMOUDEL"
        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/DROP_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
	printf "\nMOUTRAN"
        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/DROP_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
	printf "\nRECEPTION"
        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/DROP_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
        
 ##################################         IMPORT STATS TABLES                ################################################
  printf "\nImport stats tables"
  printf "\nRECEPTION"
  imp userid=RECEPTION/$1 file=RECSTATSTABLE.dmp tables=RECSTATSTABLE >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
  printf "\nMOUTRAN"
  imp userid=MOUTRAN/$1 file=TRANSTATSTABLE.dmp tables=TRANSTATSTABLE >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
  printf "\nMOUDEL"
  imp userid=MOUDEL/$1 file=DELSTATSTABLE.dmp tables=DELSTATSTABLE >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

##################################         START IMPORT STATS TABLE DATA             ################################################
  printf "\nImport stats from previous build"
  printf "\nMOUDEL"
        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
	printf "\nMOUTRAN"
        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
	printf "\nRECEPTION"
        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

#################################      RUN PRECLONE_RECON SCRIPTS         #################################################
  printf "\nRUN PRECLONE RECON SCRIPTS\n"
  printf "\nRunning PRECLONE_RECON_TARGET"  | tee -a /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log
      sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TARGET.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
  printf "\nRunning PRECLONE_RECON_TEACCESS"  | tee -a /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log
      sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TEACCESS.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1






echo "Processing daily batch for phase - $phase"
# CALL CORRECT SCRIPT TO WRITE ROWS TO MIG_PHASE_KEYGEN table
if [[ "$phase" == "ALL" ]] ; then
	printf "\nRunning batch for ALL phases\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
	/orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/INSERT_MIG_PHASE_MOSL_ALL.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
else 
	printf "\nRunning batch for phase - $phase\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
	/orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/INSERT_MIG_PHASE_MOSL_SINGLE.sql $phase >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
fi



#  Set ORACLE specifics
ORACLE_SID=$1; export ORACLE_SID
ORACLE_HOME=/oravwa/11.2.0.4 ; export ORACLE_HOME
TNS_ADMIN=/oravwa/11.2.0.4/network/admin ; export TNS_ADMIN

# Get the current date and time into a variable 
timestamp=`date '+%Y_%m_%d__%H_%M'`;


printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

##################################           COPY GOLD MOSL MASTER FILES TO EXPORT DIRECTORY         ################################################

	########           DELETE ANY EXISTING COPIES    #########

  printf "\n\nDeleting any existing master copies in EXPORT directory\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

  rm /recload/EXPORT/$1/CALCULATED_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/DISCHARGE_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/METER_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/METER_NETWORK_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/METER_READING_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/METER_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/METER_SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/SERVICE_COMPONENT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  rm /recload/EXPORT/$1/SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

	########           CREATE NEW COPY FILES         #########

  printf "\n\nCopying MOSL Delivery MASTER files to EXPORT directory\n" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

  cp /recload/$1/MOSL/FILES/CALCULATED_DISCHARGE_SEVERN-W.dat /recload/EXPORT/$1/CALCULATED_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/DISCHARGE_POINT_SEVERN-W.dat /recload/EXPORT/$1/DISCHARGE_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/METER_DISCHARGE_SEVERN-W.dat /recload/EXPORT/$1/METER_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/METER_NETWORK_SEVERN-W.dat /recload/EXPORT/$1/METER_NETWORK_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/METER_READING_SEVERN-W.dat /recload/EXPORT/$1/METER_READING_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/METER_SEVERN-W.dat /recload/EXPORT/$1/METER_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/METER_SUPPLY_POINT_SEVERN-W.dat /recload/EXPORT/$1/METER_SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/SERVICE_COMPONENT_SEVERN-W.dat /recload/EXPORT/$1/SERVICE_COMPONENT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  cp /recload/$1/MOSL/FILES/SUPPLY_POINT_SEVERN-W.dat /recload/EXPORT/$1/SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

	########           SET PERMISSIONS ON MASTER COPY FILES         #########
# Make sure all files have correct permissions assigned. We nay get errors on this.
  printf "\nApplying correct permissions to all Files" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

# Change current location to export directory
#  cd /recload/EXPORT/$1 >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

  chmod -R 777 /recload/$1/MOSL/FILES/CALCULATED_DISCHARGE_SEVERN-W.dat /recload/EXPORT/$1/CALCULATED_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/DISCHARGE_POINT_SEVERN-W.dat /recload/EXPORT/$1/DISCHARGE_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/METER_DISCHARGE_SEVERN-W.dat /recload/EXPORT/$1/METER_DISCHARGE_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/METER_NETWORK_SEVERN-W.dat /recload/EXPORT/$1/METER_NETWORK_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/METER_READING_SEVERN-W.dat /recload/EXPORT/$1/METER_READING_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/METER_SEVERN-W.dat /recload/EXPORT/$1/METER_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/METER_SUPPLY_POINT_SEVERN-W.dat /recload/EXPORT/$1/METER_SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/SERVICE_COMPONENT_SEVERN-W.dat /recload/EXPORT/$1/SERVICE_COMPONENT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
  chmod -R 777 /recload/$1/MOSL/FILES/SUPPLY_POINT_SEVERN-W.dat /recload/EXPORT/$1/SUPPLY_POINT_SEVERN-W.dat >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

# Set current location back to original directory
  cd /recload/$1/MOSL >> /recload/$1/MOSL/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1


##################################           MAIN BATCH PROCESSING         ################################################

printf "\nProceeding with MOUTRAN/MOUDEL run\n"

#Update DWRCYMRU SPIDs
printf "\nUpdating DWRCYMRU SPIDs"   | tee -a /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
/orahome/oravwa/11.2.0.4/bin/sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/03_UPDATE_DWRCYMRU_SPIDS.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

printf "\nRunning MOUTRAN batch"   | tee -a /recload/$1//MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
# Execute procedure which runs the TARIFF build and the MAIN batch.  Truncates are done in this script as well.
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/MOUTRAN_BATCH_START.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

printf "\nRunning UPDATE NOSPIDs"   | tee -a /recload/$1//MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/09_UPDATE_NOSPIDS.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

# RUN GATHER STATS ON MOUTRAN BEFORE MOUDEL RUN
# printf "\nGATHER STATS ON MOUTRAN"
# /orahome/oravwa/11.2.0.4/bin/sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

# Execute procedure which runs the DELIVERY build. 
printf "\nRUNNING MOUDEL batch" 
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/MOUDEL_BATCH_START.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

# RUN GENERATE BATCH STATS FILES
printf "\nGENERATE BATCH STAT FILES\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/GENERATE_BATCH_STATS_MOSL.sql >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

# Get updated timstamp for log
endtime=`date '+%Y_%m_%d__%H_%M'`;
printf "\n\n\nBatch process completed at $endtime" >> /recload/$1/MOSL/LOGS/DAILY_BATCH/BATCH-MOUTRAN-$1-$timestamp.log 2>&1

