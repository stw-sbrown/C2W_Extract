#!/bin/ksh
#################################################################################
# Author	M.Marron
# Version	0.1#
# Subversion $Revision: 5821 $
# Notes	Initial SAPDEL Packages and Procedures build
# 0.1 Convert script for SAPDEl and removed all MOUDEL scripts (19/05/2016)
#
################################################################################# 
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

printf "\nBatch build process starting at $timestamp\n" >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
printf "\nLog file is /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log"

#10C - DELIVERY FUNCTIONS 
printf "\nRebuilding 10C POST CODE Function procedure" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
printf "\nRebuilding 10C GIS Function procedure" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_GIS.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

# 32 - SAPDEL_UTILS
printf "\nRebuilding 32_SAPDEL_UTILS procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/32_SAPDEL_UTILS/P_SAP_DEL_UTIL_ARCHIVE_TABLE.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/32_SAPDEL_UTILS/P_SAP_DEL_UTIL_REF_DATA.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/32_SAPDEL_UTILS/P_SAP_DEL_UTIL_WRITE_FILE.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/32_SAPDEL_UTILS/P_SAP_DEL_UTIL_BATCH_STATS.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/32_SAPDEL_UTILS/P_SAP_DEL_UTIL_MPDS.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#31 - SAPDEL_POD_CREATE
printf "\nRebuilding 31_SAPDEL_POD_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/31_SAPDEL_POD_CREATE/P_SAP_DEL_POD.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/31_SAPDEL_POD_CREATE/P_SAP_DEL_POD_MO.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#33 - SAPDEL_DEV_CREATE
printf "\nRebuilding 33_SAPDEL_DEV_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/33_SAPDEL_DEV_CREATE/P_SAP_DEL_DEV.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/33_SAPDEL_DEV_CREATE/P_SAP_DEL_DEVCHAR.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/33_SAPDEL_DEV_CREATE/P_SAP_DEL_DEVMO.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/33_SAPDEL_DEV_CREATE/P_SAP_DEL_DVLCRT.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/33_SAPDEL_DEV_CREATE/P_SAP_DEL_DVLUPDATE.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#34 - SAPDEL_CON_CHANGE
printf "\nRebuilding 34_SAPDEL_CON_CHANGE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/34_SAPDEL_CON_CHANGE/P_SAP_DEL_COB.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/34_SAPDEL_CON_CHANGE/P_SAP_DEL_COB_MO.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#36 - SAPDEL_PREM_CREATE
printf "\nRebuilding 36_SAPDEL_PREM_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/36_SAPDEL_PREM_CREATE/P_SAP_DEL_PREM.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#37 - SAPDEL_INST_CREATE
printf "\nRebuilding 37_SAPDEL_INST_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/37_SAPDEL_INST_CREATE/P_SAP_DEL_SCM.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/37_SAPDEL_INST_CREATE/P_SAP_DEL_SCM_MO.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/37_SAPDEL_INST_CREATE/P_SAP_DEL_SCM_TE.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/37_SAPDEL_INST_CREATE/P_SAP_DEL_SCM_TE_MO.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#38 - SAPDEL_PODSERV_CREATE
printf "\nRebuilding 38_SAPDEL_PODSERV_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/38_SAPDEL_PODSERV_CREATE/P_SAP_DEL_POD_SRV.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#39 - SAPDEL_REG_CREATE
printf "\nRebuilding 39_SAPDEL_REG_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/39_SAPDEL_REG_CREATE/P_SAP_DEL_REG.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#40 - SAPDEL_METER_READ_CREATE
printf "\nRebuilding 40_SAPDEL_METER_READ_CREATE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/40_SAPDEL_METER_READ_CREATE/P_SAP_DEL_METER_INSTALL.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/40_SAPDEL_METER_READ_CREATE/P_SAP_DEL_METER_READ.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#42 - SAPDEL_BP_CHANGE
printf "\nRebuilding 42_SAPDEL_BP_CHANGE procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/42_SAPDEL_BP_CHANGE/P_SAP_DEL_BP.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#SAPDEL VIEWS
printf "\nRebuilding SAP DEL VIews" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus SAPDEL/$1 @/recload/$1/SAP/SAPDEL/CREATE/02_CREATE_SAP_DEL_TRIGGERS.sql >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

#30 - SAPDEL_BATCH_CONTROL
printf "\nRebuilding 30_SAPDEL_BATCH_CONTROL procedures" | tee -a /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/30_SAPDEL_BATCH_CONTROL/P_MIG_BATCH.pks >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1
sqlplus -s SAPDEL/$1 @/recload/$1/SAP/CODE_STORE/30_SAPDEL_BATCH_CONTROL/P_MIG_BATCH.pkb >> /recload/$1/SAP/LOGS/PROC_BUILD/PROC_BUILD_$1_SAPDEL-$timestamp.log 2>&1

printf "\n\nPlease check the log\n"






























