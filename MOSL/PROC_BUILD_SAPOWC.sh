#!/bin/ksh
#
# Author	S.Badhan
# Subversion $Revision: 5830 $
# 
# 0.1		S.Badhan  -- Inital Build.
#
# Rebuild OWC packages and procedures
#

clear
# Check if we SID set

if ! [[ "$1" == "DOWP" || "$1" == "DOWS" || "$1" == "DOWD" ]] ; then
	printf "\nError, please set your environment to one of three values, DOWP, DOWS or DOWD"
	printf "\nUSAGE:./PROC_BUILD_SAPTRAN environment_name\n"
	exit;
fi

#Now check to make sure the environment has been se to the same as the actual ORACLE_SID
if ! [[ "$1" == "$ORACLE_SID" ]] ; then
	printf "\nError, you have chosen to run against $1, yet your ORACLE_SID is set to $ORACLE_SID"
	printf "\nPlease correct and retry\n"
	exit;
fi


# Get the current date and time into a variable 
timestamp=`date '+%Y_%m_%d__%H_%M'`;

printf "\nOWC Batch build process starting at $timestamp\n" >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1

printf "\nLog is /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log"

# Rebuild all packages/procedures/specifications 

#05 - SUPPLY POINT
printf "\nRebuilding OWC 05-SUPPLY POINT procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/05_SUPPLY_POINT/P_OWC_SAP_SUPPLY_POINT.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1

#07 - SERVICE COMPONENT
printf "\nRebuilding OWC 07-SERVICE COMPONENT procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/07_SERVICE_COMPONENT/P_OWC_TRAN_SERVICE_COMPONENT.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1

#08 - METER
printf "\nRebuilding OWC 08-METER procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/08_METER/P_OWC_SAP_METER.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/08_METER/P_OWC_TRAN_METER_SPID_ASSOC.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/08_METER/P_OWC_TRAN_TE_METER_DPID_XREF.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1


#09 - METER READINGS
printf "\nRebuilding OWC 09-METER READINGS procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/09_METER_READINGS/P_OWC_TRAN_METER_READING.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1


#01A -  Rebuild OWC main MIG_BATCH procedures
printf "\nRebuilding 01A-MIG_BATCH package specification" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/43_SAPTRAN_BATCH_CONTROL/P_MIG_BATCH_SAP_OWC.pks >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
printf "\nRebuilding 01A-MIG_BATCH package" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1
sqlplus -s SAPTRAN/$1 @/recload/$1/SAP/CODE_STORE/43_SAPTRAN_BATCH_CONTROL/P_MIG_BATCH_SAP_OWC.pkb >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_OWCTRAN-$timestamp.log 2>&1

printf "\n\nPlease check the logs\n"

