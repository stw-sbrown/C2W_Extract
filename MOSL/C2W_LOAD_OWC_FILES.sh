#!/usr/bin/ksh
#
# Author	S.Badhan
# Subversion $Revision: 5458 $
# 
# 0.1		S.Badhan  -- Inital Build.
#  

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

printf "\nLogs will be placed in /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log"


##################################         START DROP TABLES                ################################################

#Drop RECEPTION tables
	printf "\nDropping RECEPTION OWC tables"   | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/DROP/02_DDL_DROP_OWC_TABLES.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

##################################               RECEPTION                 ################################################
#Create RECEPTION SAP tables
	printf "\nCreating RECEPTION OWC Tables" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/02_DDL_CREATE_OWC_TABLES.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1


##################################           ADD IMPORTED FILES            ################################################
#Add data to SAP tables

	printf "\n\nLoading data into the OWC tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	
  printf "\nAdding data into OWC_CALCULATED_DISCHARGE table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_CALCULATED_DISCHARGE_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_CALCULATED_DISCHARGE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_DISCHARGE_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_DISCHARGE_POINT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_METER_DISCHARGE_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_METER_DISCHARGE_POINT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_METER_NETWORK_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_METER_NETWORK-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_METER_SUPPLY_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_METER_SUPPLY_POINT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_METER_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_METER-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_METER_READING_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_METER_READING-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_SERVICE_COMPONENT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SERVICE_COMPONENT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_THAMES.ctl data=/recload/FILES/$1/FINIMPORTS/OWC/OWC_SUPPLY_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SUPPLY_POINT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1



#Display where the logs are stored.
	printf "\nPlease check the logs\n"
  
