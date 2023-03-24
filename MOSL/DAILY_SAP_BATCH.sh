###############################################################################
# 
# Author	N.Henderson
# Version	0.3
# Subversion $Revision: 6469 $
# Rebuild packages and procedures, then execute the main batch.  
# Main batch also truncates tables
# 0.1		N.Henderson. -- Initial script.  Added some more logging features
# 0.2		M.Marron     -- Altered script for SAPTRAN & SAPDEL batch runs
# 0.3		M.Marron     -- Updated the script to run all enviroments
# 0.4		D.Cheung     -- Add gather stats on SAPTRAN before SAPDEL batch
# 0.5		S.Badhan     -- Add run of reconciliation summary counts
# 0.6   D.Cheung     -- Add Generate Batch Stats files to end
# 0.7   D.Cheung     -- Fix bug causing SAPDEL to run twice in batch
# 0.8   K.Burton     -- Added call to 03_UPDATE_DWRCYMRU_SPIDS.sql
# 0.9   K.Burton     -- Added call to 09_CREATE_DUMMY_SAPFLOCNUMBERS.sql
# 1.0   S.Badhan     -- Use SAP version 04_UPDATE_DWRCYMRU_SPIDS_SAP.sql
# 1.1   K.Burton     -- Commented out 04_UPDATE_DWRCYMRU_SPIDS_SAP.sql and
#                       09_CREATE_DUMMY_SAPFLOCNUMBERS.sql - not needed
# 1.2   D.Cheung     -- Comment out gather stats
# 1.3   D.Cheung     -- Separate ALL phase script for SAP
# 1.4   K.Burton     -- Added UPDATE_NOSPSIDs script
# 1.5   D.Cheung     -- Import GOLDEN stats
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
select phase in 1 2 3 4 5 6 7 8 9 "ALL" "Exit"; do
    case $phase in
        [123456789] ) break;;
        ALL ) break;;
        Exit ) exit;;
    esac
done

echo "Processing daily batch for phase - $phase"
# CALL CORRECT SCRIPT TO WRITE ROWS TO MIG_PHASE_KEYGEN table
if [[ "$phase" == "ALL" ]] ; then
	printf "\nRunning batch for ALL phases\n" >> /recload/$1/SAP/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
	/orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/DBA_SCRIPTS/INSERT_MIG_PHASE_SAP_ALL.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
else 
	printf "\nRunning batch for phase - $phase\n" >> /recload/$1/SAP/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1
	/orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/DBA_SCRIPTS/INSERT_MIG_PHASE_MOSL_SINGLE.sql $phase >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
fi


#  Set ORACLE specifics
ORACLE_SID=$1; export ORACLE_SID
ORACLE_HOME=/oravwa/11.2.0.4 ; export ORACLE_HOME
TNS_ADMIN=/oravwa/11.2.0.4/network/admin ; export TNS_ADMIN

# Get the current date and time into a variable 
timestamp=`date '+%Y_%m_%d__%H_%M'`;

printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/SAP/LOGS/DAILY_BATCH/DAILY-BATCH-$1-$timestamp.log 2>&1

printf "\nProceeding with SAPTRAN/SAPDEL run\n"

printf "\nProceeding with build\n"

##################################         DROP STATS TABLES                ################################################
  printf "\nDrop stats tables"
  printf "\nSAPDEL"
        sqlplus SAPDEL/$1 @/recload/$1/SAP/DBA_SCRIPTS/DROP_STATS_MOUDEL.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
	printf "\nSAPTRAN"
        sqlplus SAPTRAN/$1 @/recload/$1/SAP/DBA_SCRIPTS/DROP_STATS_MOUTRAN.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
        
 ##################################         IMPORT STATS TABLES                ################################################
  printf "\nImport stats tables"
  printf "\nSAPTRAN"
  imp userid=SAPTRAN/$1 file=SAP_TRANSTATSTABLE.dmp tables=TRANSTATSTABLE >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
  printf "\nSAPDEL"
  imp userid=SAPDEL/$1 file=SAP_DELSTATSTABLE.dmp tables=DELSTATSTABLE >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

##################################         START IMPORT STATS TABLE DATA             ################################################
  printf "\nImport stats from previous build"
  printf "\nSAPDEL"
        sqlplus SAPDEL/$1 @/recload/$1/SAP/DBA_SCRIPTS/IMPORT_STATS_SAPDEL.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
	printf "\nSAPTRAN"
        sqlplus SAPTRAN/$1 @/recload/$1/SAP/DBA_SCRIPTS/IMPORT_STATS_SAPTRAN.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

##################################         RUN-PRE TRANSFORM OWC SCRIPTS            ################################################

#Update DWRCYMRU SPIDs
#printf "\nUpdating DWRCYMRU SPIDs"   | tee -a /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
#/orahome/oravwa/11.2.0.4/bin/sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/04_UPDATE_DWRCYMRU_SPIDS_SAP.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# Execute procedure which runs the SAPTRAN MAIN batch.  Truncates are done in this script as well.
printf "\nRunning SAPTRAN batch"   | tee -a /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
/orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/SAPTRAN_BATCH_START.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

printf "\nRunning UPDATE NOSPIDs"   | tee -a /recload/$1//SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1
/orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/MO/CREATE/09_UPDATE_NOSPIDS.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# RUN GATHER STATS ON SAPTRAN BEFORE SAPDEL RUN
# printf "\nGATHER STATS ON SAPTRAN"
# /orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/DBA_SCRIPTS/UPDATE_STATS_SAPTRAN.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# RUN Update dummy SAPFLOCNUMBERS
#printf "\nUpdate dummy SAPFLOCNUMBERS"
#/orahome/oravwa/11.2.0.4/bin/sqlplus SAPTRAN/$1 @/recload/$1/SAP/MO/CREATE/09_CREATE_DUMMY_SAPFLOCNUMBERS.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# Execute procedure which runs the DELIVERY build.  
printf "\nRUNNING SAPDEL batch" 
/orahome/oravwa/11.2.0.4/bin/sqlplus SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/SAPDEL_BATCH_START.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# Run reconciliation summary counts
printf "\nRun reconciliation summary counts\n"
/orahome/oravwa/11.2.0.4/bin/sqlplus SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/01D_AUDITINGREC/SAP_RECONCILIATION_SUMMARY.sql $1 >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# RUN GENERATE BATCH STATS FILES
 printf "\nGENERATE BATCH STAT FILES\n"
 /orahome/oravwa/11.2.0.4/bin/sqlplus SAPDEL/$1 @/recload/$1/SAP/DBA_SCRIPTS/GENERATE_BATCH_STATS_SAP.sql >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

# Get updated timstamp for log
endtime=`date '+%Y_%m_%d__%H_%M'`;
printf "\n\n\nBatch process completed at $endtime" >> /recload/$1/SAP/LOGS/DAILY_BATCH/BATCH-SAPTRAN-$1-$timestamp.log 2>&1

