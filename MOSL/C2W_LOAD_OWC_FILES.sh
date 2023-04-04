#!/usr/bin/ksh
#
# Author	S.Badhan
# Subversion $Revision: 5800 $
# 
# 0.1		S.Badhan  -- Inital Build.
#  

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
#ORACLE_SID=$1; export ORACLE_SID
#ORACLE_HOME=/oravwa/11.2.0.4 ; export ORACLE_HOME
#TNS_ADMIN=/oravwa/11.2.0.4/network/admin ; export TNS_ADMIN

# Get the current date and time into a variable
timestamp=`date '+%Y_%m_%d__%H_%M'`;

#printf "\nLogs will be placed in /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD-$1-$timestamp.log"

##################################           ADD SEWAGE OWC IMPORTED FILES            ################################################

##############	LOAD THAMES OWC	##############

  printf "\n\nLoading THAMES data into the OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-THAMES-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_THAMES.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_THAMES.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-THAMES-$1-$timestamp.log 2>&1

##############	LOAD SOUTHSTAFF OWC	##############

  printf "\n\nLoading data into the SOUTHSTAFF OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-SS-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_SS.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_SOUTHSTAFFS.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_SS.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_SOUTHSTAFFS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-SS-$1-$timestamp.log 2>&1

##############	LOAD YORKSHIRE OWC	##############

  printf "\n\nLoading data into the YORKSHIRE OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-YORKSHIRE-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_YORKSHIRE.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_YORKSHIRE.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-YORKSHIRE-$1-$timestamp.log 2>&1

##############	LOAD WESSEX OWC	##############

  printf "\n\nLoading data into the WESSEX OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WESSEX-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_WESSEX.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_WESSEX.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WESSEX-$1-$timestamp.log 2>&1

##############	LOAD WELSH OWC	##############

  printf "\n\nLoading data into the WELSH OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_CALCULATED_DISCHARGE table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_CALCULATED_DISCHARGE_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_CALCULATED_DISCHARGE_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_DISCHARGE_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_DISCHARGE_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_METER_DISCHARGE_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_DISCHARGE_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_METER_NETWORK_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_NETWORK_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_METER_TO_SUPPLY_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_METER_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_WELSH.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_METER_READING_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_READING_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-WELSH-$1-$timestamp.log 2>&1

##############	LOAD ANGLIAN OWC	##############

  printf "\n\nLoading data into the ANGLIAN OWC tables (WASTE)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-ANGLIAN-$1-$timestamp.log

  printf "\nAdding data into OWC_SERVICE_COMPONENT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_ANGLIAN.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SERVICE_COMPONENT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_SERVICE_COMPONENT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_ANGLIAN.ctl data=/recload/FILES/$1/OWCIMPORTS/SEWAGE/WASTE_SUPPLY_POINT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_SUPPLY_POINT_S-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_S-ANGLIAN-$1-$timestamp.log 2>&1

	printf "\nPlease check the logs\n"


##################################           ADD WATER OWC IMPORTED FILES            ################################################


##############	LOAD THAMES OWC	##############

  printf "\n\nLoading THAMES data into the OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log
	
  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORK_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_THAMES_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_THAMES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_THAMES_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W-THAMES-$1-$timestamp.log 2>&1

##############	LOAD SOUTHSTAFF OWC	##############

  printf "\n\nLoading data into the SOUTHSTAFF OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log
	
  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_POINT_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORKS_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_METER_SUPPLY_POINT_W_-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_SOUTHSTAFF.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_SOUTHSTAFF.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_SS_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_SOUTHSTAFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_SS_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_SS-$1-$timestamp.log 2>&1

##############	LOAD YORKSHIRE OWC	##############

  printf "\n\nLoading data into the YORKSHIRE OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log

  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_POINT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORKS_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_YORKSHIRE_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_YORKSHIRE.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_YORKSHIRE_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_YORKSHIRE-$1-$timestamp.log 2>&1

##############	LOAD WESSEX OWC	##############

  printf "\n\nLoading data into the WESSEX OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log

  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_POINT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORKS_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINTS_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_WESSEX_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_WESSEX.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WESSEX_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WESSEX-$1-$timestamp.log 2>&1

##############	LOAD WELSH OWC	##############

#  printf "\n\nLoading data into the WELSH OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log
	
#  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORK_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_TO_SUPPLY_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

#  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_WELSH_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_WELSH.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_WELSH_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_WELSH-$1-$timestamp.log 2>&1

##############	LOAD ANGLIAN OWC	##############

  printf "\n\nLoading data into the ANGLIAN OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log
  
  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORK_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_METER_READING_W_ANGLIAN-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_ANGLIAN_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_ANGLIAN.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_ANGLIAN_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_ANGLIAN-$1-$timestamp.log 2>&1

##############	LOAD UNITED OWC	##############

  printf "\n\nLoading data into the UNITED OWC tables (WATER)"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log
  
  printf "\nAdding data into OWC_CALCULATED_DISCHARGE_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_CALCULATED_DISCHARGE_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_CALCULATED_DISCHARGE_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_CALCULATED_DISCHARGE_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_DISCHARGE_POINT_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_DISCHARGE_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_METER_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_NETWORK_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_NETWORK_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_NETWORK_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_METER_NETWORK_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_SUPPLY_POINT_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_SUPPLY_POINT_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_METER_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_METER_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_METER_READING_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_METER_READING_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_METER_READING_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_METER_READING_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_DISCHARGE_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_DISCHARGE_POINT_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_DISCHARGE_POINT_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_DISCHARGE_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SERVICE_COMPONENT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SERVICE_COMPONENT_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SERVICE_COMPONENT_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_SERVICE_COMPONENT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1

  printf "\nAdding data into OWC_SUPPLY_POINT_W table"  | tee -a /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/OWC_SUPPLY_POINT_UNITED_W.ctl data=/recload/FILES/$1/OWCIMPORTS/WATER/OWC_SUPPLY_POINT_UNITED.dat ERRORS=100000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/OWC_UNITED_SUPPLY_POINT_W-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/DATA_LOADS/DATA_LOAD_W_UNITED-$1-$timestamp.log 2>&1


#Display where the logs are stored.
	printf "\nPlease check the logs\n"
