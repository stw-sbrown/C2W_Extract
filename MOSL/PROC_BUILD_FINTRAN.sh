#!/bin/ksh
#
# Author	S.Badhan
# Subversion $Revision: 5292 $
# 
# 0.1		S.Badhan  -- Inital Build.
#
# Rebuild packages and procedures
#

clear
# Check if we SID set

if ! [[ "$1" == "DOWP" || "$1" == "DOWS" || "$1" == "DOWD" ]] ; then
	printf "\nError, please set your environment to one of three values, DOWP, DOWS or DOWD"
	printf "\nUSAGE:./PROC_BUILD_FINTRAN environment_name\n"
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

printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

printf "\nLog is /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log"

# Rebuild all packages/procedures/specifications 

#10 - Bad Data
#printf "\nRebuilding 01E_BAD_DATA procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
#sqlplus -s FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/01E_BAD_DATA/P_MOU_TRAN_BAD_DATA.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#02 - SUPPLY POINT
printf "\nRebuilding 02-SUPPLY POINT procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/02_SUPPLY_POINT/P_FIN_TRAN_SUPPLY_POINT.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#03 - SERVICE COMPONENT
printf "\nRebuilding 03_SERVICE_COMPONENT procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/03_SERVICE_COMPONENT/P_FIN_TRAN_SERVICE_COMPONENT.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#04 - DISCHARGE POINT
printf "\nRebuilding 04_DISCHARGE_POINT procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/04_DISCHARGE_POINT/P_FIN_TRAN_DISCHARGE_POINT.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/04_DISCHARGE_POINT/P_FIN_TRAN_CALC_DISCHARGE.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#10C DEL FUNCTIONS
#printf "\nRebuilding 10C GIS Function procedure" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
#sqlplus -s FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_GIS.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
printf "\nRebuilding 10C POST CODE Function procedure" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#05 - METER
printf "\nRebuilding 05_METER procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/05_METER/P_FIN_TRAN_METER.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#06- METER READING
printf "\nRebuilding 06_METER_READING procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/06_METER_READING/P_FIN_TRAN_METER_READING.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#07 - METER_SPID_ASSOC
printf "\nRebuilding 07_METER_SPID_ASSOC procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/07_METER_SPID_ASSOC/P_FIN_TRAN_METER_SPID_ASSOC.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#08 - METER_NETWORK
printf "\nRebuilding 08_METER_NETWORK procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/08_METER_NETWORK/P_FIN_TRAN_METER_NETWORK.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#09 - METER_DPID_XREF
printf "\nRebuilding 09_METER_DPID_XREF procedures" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/09_METER_DPID_XREF/P_FIN_TRAN_METER_DPID_XREF.sql >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1


#01 -  Rebuild main MIG_BATCH procedures
printf "\nRebuilding 01_FIN_BATCH_CONTROL package specification" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/01_FIN_BATCH_CONTROL/P_MIG_BATCH.pks >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

printf "\nRebuilding 01_FIN_BATCH_CONTROL package" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/CODE_STORE/01_FIN_BATCH_CONTROL/P_MIG_BATCH.pkb >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

#01C - TARIFF IMPORT
printf "\nRebuilding 01C-TARIFF package specification" | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/01C_TARIFF_IMPORT/P_MIG_TARIFF.pks >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
printf "\nRebuilding 01C-TARIFF package " | tee -a /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1
sqlplus -s FINTRAN/$1 @/recload/$1/FIN/CODE_STORE/01C_TARIFF_IMPORT/P_MIG_TARIFF.pkb >> /recload/$1/FIN/LOGS/PROC_BUILD/PROC_BUILD_$1_FINTRAN-$timestamp.log 2>&1

printf "\n\nPlease check the logs\n"

