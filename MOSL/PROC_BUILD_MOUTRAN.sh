#!/bin/ksh
#
# N.Henderson	22nd March 2016
#
# Subversion $Revision: 6068 $
#
# Rebuild packages and procedures, then execute the main batch.  
# Main batch also truncates tables
#
# 1.0   S.Badhan  -- I-320. Move compile of Tariff batch after main batch
#
#


clear
# Check if we SID set

if ! [[ "$1" == "DOWP" || "$1" == "DOWS" || "$1" == "DOWD" ]] ; then
	printf "\nError, please set your environment to one of three values, DOWP, DOWS or DOWD"
	printf "\nUSAGE:./PROC_BUILD_MOUTRAN environment_name\n"
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

printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

printf "\nLog is /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log"

# Rebuild all packages/procedures/specifications 


#01B - KEY GEN - Key generation is an adhoc procedure, not run here
printf "\nRebuilding 01B-KEY GEN procedures." | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01B_KEY_GEN/P_MOU_TRAN_KEY_GEN.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1


#01D - AUDITING REC
#Commented out PRECLONE_RECON_TARGET.sql and PRECLONE_RECON_TEACCESS.sql as these do not need to be executed during a batch run
printf "\nRebuilding 01D-AUDITING procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/P_MIG_RECON.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
#sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TARGET.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
#sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TEACCESS.sql  >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#02 - PROPERTY
printf "\nRebuilding 02-PROPERTY procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/02_PROPERTY/P_MOU_TRAN_PROPERTY.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#03 - CUSTOMER
printf "\nRebuilding 03-CUSTOMER procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/03_CUSTOMER/P_MOU_TRAN_CUSTOMER.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#04 - ADDRESS
printf "\nRebuilding 10C POST CODE Function procedure" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
printf "\nRebuilding 04-ADDRESS procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/04_ADDRESS/P_MOU_TRAN_ADDRESS.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#05 - SUPPLY POINT
printf "\nRebuilding 05-SUPPLY POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/05_SUPPLY_POINT/P_MOU_TRAN_SUPPLY_POINT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#06 - DISCHARGE POINT
printf "\nRebuilding 06-DISCHARGE POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/06_DISCHARGE_POINT/P_MOU_TRAN_DISCHARGE_POINT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/06_DISCHARGE_POINT/P_MOU_TRAN_TE_WORKING.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/06_DISCHARGE_POINT/P_MOU_TRAN_TE_SUMMARY.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/06_DISCHARGE_POINT/P_MOU_TRAN_CALC_DISCHARGE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1


#07 - SERVICE COMPONENT
printf "\nRebuilding 07-SERVICE COMPONENT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
#sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/Run_1_ServProvTariffDDL.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/P_MOU_TRAN_SC_PRE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/P_MOU_TRAN_SC_MPW.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/P_MOU_TRAN_SC_UW.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/P_MOU_TRAN_SC_AS.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/07_SERVICE_COMPONENT/P_MOU_TRAN_SERVICE_COMPONENT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#08 - METER
printf "\nRebuilding 10C GIS Function procedure" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_GIS.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
printf "\nRebuilding 08-METER procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_TARGET.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_NETWORK.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_SPID_ASSOC.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_TE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_DPIDXREF.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_RTS_MDVOL.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/08_METER/P_MOU_TRAN_METER_NETWORK_TE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#09 - METER READINGS
printf "\nRebuilding 09-METER READINGS procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/09_METER_READINGS/P_MOU_TRAN_ROLLOVER.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/09_METER_READINGS/P_MOU_TRAN_METER_READING.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/09_METER_READINGS/P_MOU_TRAN_METER_READING_TE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#10 - Bad Data
printf "\nRebuilding 09-METER READINGS procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01E_BAD_DATA/P_MOU_TRAN_BAD_DATA.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#01A -  Rebuild main MIG_BATCH procedures
printf "\nRebuilding 01A-MIG_BATCH package specification" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01A_BATCH_CONTROL/P_MIG_BATCH.pks >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
printf "\nRebuilding 01A-MIG_BATCH package" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01A_BATCH_CONTROL/P_MIG_BATCH.pkb >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

#01C - TARIFF IMPORT
printf "\nRebuilding 01C-TARIFF package specification" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01C_TARIFF_IMPORT/P_MIG_TARIFF.pks >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
printf "\nRebuilding 01C-TARIFF package " | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1
sqlplus -s MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01C_TARIFF_IMPORT/P_MIG_TARIFF.pkb >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUTRAN-$timestamp.log 2>&1

printf "\n\nPlease check the logs\n"

