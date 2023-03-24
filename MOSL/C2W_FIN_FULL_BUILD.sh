#!/usr/bin/ksh
#
# Author	S.Badhan
# Subversion $Revision: 5391 $
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

printf "\nLogs will be placed in /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log"

##################################         START EXPORT STATS TABLES                ################################################
# Run an update statistics on all tables BEFORE the build
        printf "\nRunning an update stats first"
	printf "\nFINDEL"
        sqlplus FINDEL/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINDEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nFINTRAN"
        sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nRECEPTION"
        sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINSRECEPTION.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
        
  printf "\nExport existing stats from current build"
  printf "\nFINDEL"
        sqlplus FINDEL/$1 @/recload/$1/FIN/DBA_SCRIPTS/EXPORT_STATS_FINDEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nFINTRAN"
        sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/EXPORT_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	#printf "\nRECEPTION"
  #      sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/EXPORT_STATS_RECEPTION.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
        

##################################         START DROP TABLES                ################################################

	printf "\nDropping MO" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nDropping $2 tables" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/DROP/03_DDL_MOSL_TABLES_DROP_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nDropping $2 sequences"   | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/DROP/04_DDL_MOSL_DROP_SEQUENCES_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nDropping $2 procs"   | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/FIN/FINTRAN/DROP/01_DDL_DROP_FINTRAN_PROCS.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
#Drop MIG tables	
	printf "\nDropping MIG tables." | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @/recload/$1/FIN/MIG/DROP/01_DDL_DROP_MIG_TABLES.sql  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop LU tables
	printf "\nDropping LU tables"   | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @/recload/$1/FIN/LU/DROP/01_DDL_DROP_LU_TABLES.sql  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop BT tables
#	printf "\nDropping BT tables"   | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#	sqlplus FINTRAN/$1 @/recload/$1/FIN/BT/DROP/01_DDL_DROP_BT_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop RECEPTION tables
	printf "\nDropping RECEPTION SAP tables"   | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/FIN/FIN/RECEPTION/DROP/01_DDL_DROP_SAP_RECEPTION_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop DEL tables
	printf "\nDropping DELIVERY tables" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINDEL/$1 @/recload/$1/FIN/DEL/DROP/01_DDL_DROP_DEL_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nDrop complete\n\n"  

#Drop DEL MIG tables	
	printf "\nDropping DELIVERY MIG tables." | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINDEL/$1 @/recload/$1/FIN/DEL/DROP/02_DDL_DROP_DEL_MIG_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
 

##################################    CREATE TABLES AND APPLY PATCHES      ################################################

##################################                  BT                     ################################################
#Create BT tables
#	printf "\nCreating BT tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#	for bt in /recload/$1/FIN/BT/CREATE/*.sql
#	do
#	sqlplus FINTRAN/$1 @$bt >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	done

#Apply patches to BT tables
#	printf "\nApplying BT patches" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#	for bt in /recload/$1/FIN/PATCH/BT/*.sql
#	do
#	sqlplus FINTRAN/$1 @$bt >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	done


##################################               RECEPTION                 ################################################
#Create RECEPTION SAP tables
	printf "\nCreating RECEPTION SAP Tables" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/FIN/FIN/RECEPTION/CREATE/01_DDL_CREATE_SAP_RECEPTION_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1



##################################                  LU                     ################################################
#Create LU tables
	printf "\nCreating LU tables\n"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @/recload/$1/FIN/LU/CREATE/01_DDL_MOSL_LOOKUP_TABLES_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Apply patches for LU tables
	printf "\nApplying LU patches" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for lu in /recload/$1/FIN/PATCH/LU/*.sql
	do
	printf "\nApplying $lu" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @$lu >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	done


##################################                 MIG                     ################################################
#Create MIG tables in MO
#MIG tables are not re-created during a patch build

 printf "\nCreating MIG Tables" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
 sqlplus FINTRAN/$1 @/recload/$1/FIN/MIG/CREATE/01_DDL_CREATE_MIG_TABLES.sql  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Apply patches to MIG tables
       printf "\nApplying MIG patches" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       for m in /recload/$1/FIN/PATCH/MIG/*.sql
       do
	     printf "\nApplying $m" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       sqlplus FINTRAN/$1 @$m >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
       done

#Create views on MIG tables
	printf "\nCreating MIG views" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @/recload/$1/FIN/MIG/VIEWS/01_DDL_CREATE_MIG_VIEWS.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1 


##################################                 MO                      ################################################
#Create MO tables
	printf "\n\nCreating MO tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/01_DDL_MOSL_TABLES_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 primary keys"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/02_DDL_MOSL_PK_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 foreign keys" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/03_DDL_MOSL_FK_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 constraints and static data"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/04_DDL_MOSL_REF_DATA_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 comments on fields and tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/05_COMMENTS_TABLES_FIELDS_ALL.sql >>/recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 sequences\n"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  FINTRAN/$1 @/recload/$1/FIN/MO/CREATE/06_DDL_MOSL_SEQUENCES_ALL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1  

#Apply MO patches
	printf "\nApplying MOSL patches"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for p in /recload/$1/FIN/PATCH/MO/*.sql
	do
	printf "\nApplying $p" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @$p >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	done

#Apply MO patches specifically for FIN
	printf "\nApplying MOSL patches specific to FIN"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for p in /recload/$1/FIN/FIN/FINTRAN/PATCH/*.sql
	do
	printf "\nApplying $p" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINTRAN/$1 @$p >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	done
  
##################################               DELIVERY                  ################################################

#Rename Delivery area any MOUTRAN to FINTRAN
 printf "\nRename Delivery area any MOUTRAN to FINTRAN"  | tee -a /recload/$1/FIN/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
 /recload/$1/FIN/FIN/FINDEL/FINDEL_REPLACE_SCHEMA_NAME.sh >> /recload/$1/FIN/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Create the Delivery tables
	printf "\nCreating DELIVERY tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  FINDEL/$1 @/recload/$1/FIN/DEL/CREATE/01_DDL_CREATE_DEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create MIG tables in DELIVERY
	printf "\nCreating MIG tables in FINDEL" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINDEL/$1 @/recload/$1/FIN/MIG/CREATE/01_DDL_CREATE_MIG_TABLES.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Apply patches to MIG tables
       printf "\nApplying MIG patches" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       for m in /recload/$1/FIN/PATCH/MIG/*.sql
       do
	     printf "\nApplying $m" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       sqlplus FINDEL/$1 @$m >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
       done

#Create views on MIG tables
	printf "\nCreating MIG views" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINDEL/$1 @/recload/$1/FIN/MIG/VIEWS/01_DDL_CREATE_MIG_VIEWS.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1 

#Create Views
	printf "\nCreating DELIVERY views"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  FINDEL/$1 @/recload/$1/FIN/DEL/CREATE/02_DDL_CREATE_DEL_VIEW.sql  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create Functions
	printf "\nCreating DELIVERY functions"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  FINDEL/$1 @/recload/$1/FIN/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create Triggers
	printf "\nCreating DELIVERY triggers"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	for t in /recload/$1/FIN/DEL/CREATE/*TRG.sql
	do
	sqlplus FINDEL/$1 @$t >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	done

#Apply patches to Delivery tables
	printf "\nApplying DELIVERY patches" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for d in /recload/$1/FIN/PATCH/DEL/*.sql
	do
  printf "\nApplying $d" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus FINDEL/$1 @$d >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	done

##################################           ADD IMPORTED FILES            ################################################
#Add data to SAP tables

	printf "\n\nLoading data into the SAP tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	
  printf "\nAdding data into SAP_CALCULATED_DISCHARGE table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_CALCULATED_DISCHARGE.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_CALCULATED_DISCHARGE.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_CALCULATED_DISCHARGE-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_DISCHARGE_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_DISCHARGE_POINT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_DISCHARGE_POINT.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_DISCHARGE_POINT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_METER_DISCHARGE_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_DISCHARGE_POINT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_DISCHARGE_POINT.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_DISCHARGE_POINT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_METER_NETWORK table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_NETWORK.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_NETWORK.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_NETWORK-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_METER_SUPPLY_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_SUPPLY_POINT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_SUPPLY_POINT.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_SUPPLY_POINT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_METER table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_METER_READING table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_READING.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_READING.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_READING-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_SERVICE_COMPONENT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_SERVICE_COMPONENT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_SERVICE_COMPONENT.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_SERVICE_COMPONENT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into SAP_SUPPLY_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_SUPPLY_POINT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_SUPPLY_POINT.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_SUPPLY_POINT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#  printf "\nAdding data into DWRCYMRU - SAP_SUPPLY_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_SUPPLY_POINT_DWRCYMRU.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_SUPPLY_POINT_DWRCYMRU.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_SUPPLY_POINT_DWRCYMRU-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#  printf "\nAdding data into DWRCYMRU - SAP_METER_SUPPLY_POINT table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_SUPPLY_POINT_DWRCYMRU.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_SUPPLY_POINT_DWRCYMRU.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_SUPPLY_POINT_DWRCYMRU-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#  printf "\nAdding data into DWRCYMRU - SAP_METER table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log  
#  sqlldr RECEPTION/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/SAP_METER_DWRCYMRU.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/SAP_METER_DWRCYMRU.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/SAP_METER_DWRCYMRU-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1


##################################           ADD STATIC DATA               ################################################
##################################                 LU                      ################################################
#Add data to LU tables
	printf "\n\nLoading data into the LU tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nAdding data into LU_CONSTRUCTION_SITE table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_CONSTRUCTION_SITE.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_CONSTRUCTION_SITE.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-CONSTRUCTION-SITE-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_CROSSBORDER" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_CROSSBORDER.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_CROSSBORDER.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-CROSSBORDER-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_DATALOGGERS table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_DATALOGGERS_ALL.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_DATALOGGERS_ALL.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-DATALOGGERS-ALL-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
  #printf "\nAdding data into LU_LANDLORD" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
  #sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_LANDLORD.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_LANDLORD.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-LANDLORD-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_OUTREADER" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_OUTREADER_PROTOCOLS.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_OUTREADER_PROTOCOLS.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-OUTREADER-PROTOCOLS-$1-$timestamp.log  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_PUBHEALTHRESITE table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_PUBHEALTHRESITE.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_PUBHEALTHRESITE.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-PUBHEALTHRESITE-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SERVICE_CATEGORY" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_SERVICE_CATEGORY.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_SERVICE_CATEGORY_2016.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-SERVICE-CATEGORY-$1-$timestamp.log  >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_SPID_RANGE table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_SPID_RANGE.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_SPID_RANGE.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-SPID-RANGE-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_TARIFF_SPECIAL_AGREEMENTS" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-TARIFF-SPECIAL-AGREEMENTS-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SAP_FLOCA" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_SAP_FLOCA.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_SAP_TARGET_FLOC_MATCHES.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-SAP-FLOCA-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
  printf "\nAdding data into LU_SAP_EQUIPMENT" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_SAP_EQUIPMENT.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_SAP_TARGET_METER_MATCHES.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-SAP-EQUIPMENT-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SPID_OWC_RETAILER table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_SPID_OWC_RETAILER.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_SPID_OWC_RETAILER.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU-SPID-OWC-RETAILER-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_DISCHARGE_VOL_LIMITS" | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/LU_DISCHARGE_VOL_LIMITS.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/LU_DISCHARGE_VOL_LIMITS.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/LU_DISCHARGE_VOL_LIMITS-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

##################################                 MO                      ################################################
#Add data to MO tables
	printf "\nLoading data into the MO tables"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nAdding data into MO_ORG table"  | tee -a /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr FINTRAN/$1 control=/recload/$1/FIN/SQLLDR_FILES/CONTROL_FILES/MO_ORG.ctl data=/recload/$1/FIN/SQLLDR_FILES/DATA_FILES/MO_ORG.dat log=/recload/$1/FIN/LOGS/DATA_LOADS/MO-ORG-$1-$timestamp.log >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

##################################               STATS                     ################################################

# Run an update statistics on all tables before the drop
  printf "\nRunning an update stats first"
	printf "\nFINDEL"
  sqlplus FINDEL/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINDEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nFINTRAN"
  sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nRECEPTION tables used by FINTRAN"
  sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINSRECEPTION.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

##################################         START IMPORT STATS TABLES                ################################################
 # printf "\nImport stats from previous build"
 # printf "\nFINDEL"
 #       sqlplus FINDEL/$1 @/recload/$1/FIN/DBA_SCRIPTS/IMPORT_STATS_FINDEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nFINTRAN"
 #       sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/IMPORT_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nRECEPTION"
 #       sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/EXPORT_STATS_RECEPTION.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

# Run an update statistics on all tables AFTER the build
        printf "\nRunning an update stats first"
	printf "\nFINDEL"
        sqlplus FINDEL/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINDEL.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nFINTRAN"
        sqlplus FINTRAN/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINTRAN.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nRECEPTION"
        sqlplus RECEPTION/$1 @/recload/$1/FIN/DBA_SCRIPTS/UPDATE_STATS_FINSRECEPTION.sql >> /recload/$1/FIN/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1


#Display where the logs are stored.
	printf "\nPlease check the logs\n"
  
