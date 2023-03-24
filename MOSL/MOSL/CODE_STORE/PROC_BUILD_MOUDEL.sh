#!/bin/ksh
#
# N.Henderson	22nd March 2016
#
# Subversion $Revision: 4023 $
#
# Rebuild packages and procedures, then execute the main batch.  
# Main batch also truncates tables
clear

# Check if we SID set

if ! [[ "$1" == "DOWP" || "$1" == "DOWS" || "$1" == "DOWD" ]] ; then
	printf "\nError, please set your environment to one of three values, DOWP, DOWS or DOWD"
	printf "\nUSAGE:./PROC_BUILD_MOUDEL environment_name\n"
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

printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
printf "\nLog file is /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log"


#10B - DELIVERY UTILS
printf "\nRebuilding 10B-DELIVERY UTILS procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10B_DEL_UTILITY/P_DEL_UTIL_ARCHIVE_TABLE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
printf "\nRebuilding 10B-DELIVERY Function Write file" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10B_DEL_UTILITY/P_DEL_UTIL_WRITE_FILE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1


#10C - DELIVERY FUNCTIONS 
printf "\nRebuilding 10C-DELIVERY Functions" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1


#11 - DELIVERY SUPPLY POINT
printf "\nRebuilding 11-DELIVERY SUPPLY POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/11_DEL_SUPPLY_POINT/P_MOU_DEL_SUPPLY_POINT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#12 - DELIVERY SERVICE COMPONENT
printf "\nRebuilding 12-DELIVERY SERVICE COMPONENT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/12_DEL_SERVICE_COMPONENT/P_MOU_DEL_SERVICE_COMPONENT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#13 - DELIVERY METER
printf "\nRebuilding 06-DISCHARGE POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/13_DEL_METER/P_MOU_DEL_METER.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#14 - DELIVERY METER SUPPLY POINT
printf "\nRebuilding 14-DELIVERY METER SUPPLY POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/14_DEL_METER_SUPPLY_POINT/P_MOU_DEL_METER_SUPPLY_POINT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#15 - DELIVERY METER NETWORK
printf "\nRebuilding 15-DELIVERY METER NETWORK procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/15_DEL_METER_NETWORK/P_MOU_DEL_METER_NETWORK.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#16 - DELIVERY METER READINGS
printf "\nRebuilding 16-DELIVERY METER READINGS procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/16_DEL_METER_READINGS/P_MOU_DEL_METER_READING.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#17 - DELIVERY DISCHARGE POINT
printf "\nRebuilding 17-DELIVERY DISCHARGE POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/17_DEL_DISCHARGE_POINT/P_MOU_DEL_DISCHARGE_POINT.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#18 - DELIVERY METER DISCHARGE POINT
printf "\nRebuilding 18-DELIVERY METER DISCHARGE POINT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/18_DEL_METER_DISCHARGE_POINT/P_MOU_DEL_METER_DISCHARGE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1


#20 - DELIVERY CALCULATED DISCHARGE
printf "\nRebuilding 20-DELIVERY CALCULATED DISCHARGE procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/20_DEL_CALC_DISCHARGE/P_MOU_DEL_CALCULATED_DISCHARGE.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1



#19 - DELIVERY TARIFF EXPORT
printf "\nRebuilding 19-DELIVERY TARIFF EXPORT procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/19_DEL_TARIFF_EXPORT/P_MOU_DEL_TARIFF_EXPORT.pks >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/19_DEL_TARIFF_EXPORT/P_MOU_DEL_TARIFF_EXPORT.pkb >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
#sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/19_DEL_TARIFF_EXPORT/P_MOU_DEL_TARIFF_EXPORT_SPEC.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
#sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/19_DEL_TARIFF_EXPORT/P_MOU_DEL_TARIFF_EXPORT_BODY.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

#10A - DELIVERY BATCH CONTROL
printf "\nRebuilding 10A-DELIVERY BATCH CONTROL procedures" | tee -a /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10A_DEL_BATCH_CONTROL/P_MIG_BATCH.pks >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10A_DEL_BATCH_CONTROL/P_MIG_BATCH.pkb >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
#sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10A_DEL_BATCH_CONTROL/P_MIG_BATCH_SPEC.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1
#sqlplus -s MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10A_DEL_BATCH_CONTROL/P_MIG_BATCH_BODY.sql >> /recload/$1/MOSL/LOGS/PROC_BUILD/PROC_BUILD_$1_MOUDEL-$timestamp.log 2>&1

printf "\n\nPlease check the log\n"


