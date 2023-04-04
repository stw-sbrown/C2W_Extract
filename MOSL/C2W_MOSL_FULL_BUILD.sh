#!/usr/bin/ksh
#
# 
# Author	N.Henderson
# Version	0.6
# $Revision: 6346 $
# Notes	Initial build
# 		Added scripts to drop and re-create all of the BT tables (12/04/2016), NJH
#  		Added the drop and create scripts for the delivery tables (14/04/2016), NJH
# 0.3		N.Henderson. -- Reordered the run oder.  Added some more logging features
# 0.4		N.Henderson  -- Copy of original file and added MIG related tables and views
# 0.5		M.Marron  -- Updated the SQLLDR commands to enable the data loads to work in all enviroments
# 0.6		M.Marron  -- Updated the include all changes made to the PATCH version and add subversion tag
# 0.7		S.Badhan  -- SAP CR_16. load of new table LU_DISCHARGE_VOL_LIMITS.
# 0.8   D.Cheung  -- Move Gather Stats to end of build
# 0.9   S.Badhan  -- I-320. Add create of MIG tables.
# 1.0   S.Badhan  -- I-320. Add drop of Delivery MIG tables.
# 1.1   D.Cheung  -- Add export and import previous stats
# 1.2   S.Badhan  -- I-320. Population of TVMNHHDTL now done in 2 scripts, create table then insert into table.
# 1.3   S.Badhan  -- I-320. Add create of Delivery MIG tables.
# 1.4   S.Badhan  -- I-320. Update source of lookup files.
# 1.5   K.Burton  -- Added LU_OWC_RECON_MEASURES for OWC files
# 1.6   S.Badhan  -- Add load of LU_LANDLORD
# 1.7   S.Badhan  -- Add load of LU_OWC_TARIFF
# 1.8   D.Cheung  -- Add running of PRECLONE_RECON scripts for target and teaccess counts
# 1.9   S.Badhan  -- Add load of OWC files.
# 1.10  S.Badhan  -- Add load of LU_OWC_TE_METERS and BT_OWC_TE_DPID_REF.
# 1.11  S.Badhan  -- LU_SPID_RANGE now populated from CIS.LU_SPID_RANGE
# 1.12  K.Burton  -- Added LU_SPID_RANGE_DWRCYMRU
# 1.13  S.Badhan  -- Add drop of OWC procs
# 1.14  L.Smith   -- Add drop and create views for TE
# 1.15  K.Burton  -- Added LU_SPID_RANGE_NOSPID
# 1.16  S.Badhan  -- Added LU_SS_LANDLORD
# 1.17  D.Cheung  -- Added LU_OTHER_METER_DPID
# 1.18  S.Badhan  -- Removed drop of MIG tables
# 1.19  K.Burton  -- Added LU_OWC_NOT_SENSITIVE
# 1.20  K.Burton  -- Added LU_OWC_SSW_SPIDS
# 1.21  D.Cheung  -- Added LU_TE_METER_DPID_EXCLUSION
# 1.22  L.Smith   -- Added LU_TE_METER_PAIRING
# 1.23  L.Smith   -- Move stats and preclone scripts to DAILY_MOSL_BATCH
# 1.24  K.Burton  -- Added LU_OWC_SAP_FLOCA
# 1.25  K.Burton  -- Added LU_NOSPID_EXCEPTIONS

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


printf "\nLogs will be placed in /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log"

##################################         START EXPORT STATS TABLES                ################################################
# Run an update statistics on all tables BEFORE the build

#  printf "\nRunning an update stats first"
#	printf "\nMOUDEL"
#        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nMOUTRAN"
#        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nRECEPTION"
#        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#        
#  printf "\nExport existing stats from current build"
#  printf "\nMOUDEL"
#        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/EXPORT_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nMOUTRAN"
#        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/EXPORT_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nRECEPTION"
#        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/EXPORT_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
        


##################################         START DROP TABLES                ################################################


	printf "\nDropping MO" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nDropping $2 tables" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/03_DDL_MOSL_TABLES_DROP_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nDropping $2 sequences"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/04_DDL_MOSL_DROP_SEQUENCES_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
#Drop materialized views
  printf "\nDropping $2 views"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/06_DDL_MOSL_DROP_MATERIALIZED_VIEWS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop OWC procs

	printf "\nDropping OWC procs" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/07_DDL_MOSL_DROP_OWC_PROCS.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
#Drop MIG tables	
  #printf "\nDropping MIG tables." | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/DROP/01_DDL_DROP_MIG_TABLES.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1


#Drop LU tables
	printf "\nDropping LU tables"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/DROP/01_DDL_DROP_LU_TABLES.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/DROP/02_DDL_DROP_OWC_LU_TABLES.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
#Drop BT tables
	printf "\nDropping BT tables"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/BT/DROP/01_DDL_DROP_BT_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Drop RECEPTION tables
	printf "\nDropping RECEPTION table"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/DROP/01_DDL_DROP_TVMNHHDTL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nDropping RECEPTION OWC tables"   | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/DROP/02_DDL_DROP_OWC_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  

#Drop DEL tables
	printf "\nDropping DELIVERY tables" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUDEL/$1 @/recload/$1/MOSL/DEL/DROP/01_DDL_DROP_DEL_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nDrop complete\n\n"  

#Drop DEL MIG tables	
	#printf "\nDropping DELIVERY MIG tables." | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlplus MOUDEL/$1 @/recload/$1/MOSL/DEL/DROP/02_DDL_DROP_DEL_MIG_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
##################################           END DROP TABLES               ################################################


##################################    CREATE TABLES AND APPLY PATCHES      ################################################
##################################                  BT                     ################################################
#Create BT tables
	printf "\nCreating BT tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for bt in /recload/$1/MOSL/BT/CREATE/*.sql
	do
	sqlplus MOUTRAN/$1 @$bt >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	done

#Apply patches to BT tables
	printf "\nApplying BT patches" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for bt in /recload/$1/MOSL/PATCH/BT/*.sql
	do
	printf "\nApplying $bt" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @$bt >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
 	done
##################################                  BT                     ################################################


##################################               RECEPTION                 ################################################
#Create RECEPTION tables

	printf "\nCreating RECEPTION Table" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/0_DDL_TVMNHHDTL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nPopulating RECEPTION Table" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/01_DDL_CREATE_TVMNHHDTL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nCreating RECEPTION OWC Tables" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/02_DDL_CREATE_OWC_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

##################################               RECEPTION                 ################################################


##################################                  LU                     ################################################
#Create LU tables
	printf "\nCreating LU tables\n"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/CREATE/01_DDL_MOSL_LOOKUP_TABLES_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/CREATE/02_DDL_CREATE_OWC_LU_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
#Apply patches for LU tables
	printf "\nApplying LU patches" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for lu in /recload/$1/MOSL/PATCH/LU/*.sql
	do
	printf "\nApplying $lu" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @$lu >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	done
##################################                  LU                     ################################################

##################################                 MIG                     ################################################
#Create MIG tables in MO
#MIG tables are re-created during a full build

 #printf "\nCreating MIG Tables" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
 #sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/CREATE/01_DDL_CREATE_MIG_TABLES.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Apply patches to MIG tables
       #printf "\nApplying MIG patches" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       #for m in /recload/$1/MOSL/PATCH/MIG/*.sql
       #do
       #sqlplus MOUTRAN/$1 @$m >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
       #done

#Create views on MIG tables
	#printf "\nCreating MIG views" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/VIEWS/01_DDL_CREATE_MIG_VIEWS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1 
##################################                 MIG                     ################################################

##################################                 MO                      ################################################
#Create MO tables
	printf "\n\nCreating MO tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/01_DDL_MOSL_TABLES_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 primary keys"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/02_DDL_MOSL_PK_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 foreign keys" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/03_DDL_MOSL_FK_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 constraints and static data"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/04_DDL_MOSL_REF_DATA_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 comments on fields and tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/05_COMMENTS_TABLES_FIELDS_ALL.sql >>/recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 sequences\n"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/06_DDL_MOSL_SEQUENCES_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1  
	printf "\nAdding $2 OWC views\n"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/08_DDL_OWC_VIEWS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1  

#Apply MO patches
	printf "\nApplying MOSL patches"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for p in /recload/$1/MOSL/PATCH/MO/*.sql
	do
	printf "\nApplying $p" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @$p >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	done
##################################                 MO                      ################################################

#	printf "\nCreating RECEPTION OWC Views" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/02_DDL_CREATE_RECEPTION_VIEWS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
  #Create MO views
	printf "\nAdding $2 views\n"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/07_DDL_MOSL_VIEWS_ALL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1  


##################################               DELIVERY                  ################################################
#Create the Delivery tables
	printf "\nCreating DELIVERY tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/DEL/CREATE/01_DDL_CREATE_DEL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create MIG tables in DELIVERY
#	printf "\nCreating MIG tables in MOUDEL" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlplus MOUDEL/$1 @/recload/$1/MOSL/MIG/CREATE/01_DDL_CREATE_MIG_TABLES.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Apply patches to MIG tables
       #printf "\nApplying MIG patches" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
       #for m in /recload/$1/MOSL/PATCH/MIG/*.sql
       #do
       #sqlplus MOUDEL/$1 @$m >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
       #done

#Create views on MIG tables
	#printf "\nCreating MIG views" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlplus MOUDEL/$1 @/recload/$1/MOSL/MIG/VIEWS/01_DDL_CREATE_MIG_VIEWS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1 
  
#Create Views
	printf "\nCreating DELIVERY views"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/DEL/CREATE/02_DDL_CREATE_DEL_VIEW.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create Functions
	printf "\nCreating DELIVERY functions"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#Create Triggers
	printf "\nCreating DELIVERY triggers"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 
	for t in /recload/$1/MOSL/DEL/CREATE/*TRG.sql
	do
	sqlplus MOUDEL/$1 @$t >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	done

#Apply patches to Delivery tables
	printf "\nApplying DELIVERY patches" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	for d in /recload/$1/MOSL/PATCH/DEL/*.sql
	do
	sqlplus MOUDEL/$1 @$d >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	done

##################################               DELIVERY                  ################################################


##################################           ADD STATIC DATA               ################################################
##################################                 LU                      ################################################
#Add data to LU tables
	printf "\n\nLoading data into the LU tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nAdding data into LU_CONSTRUCTION_SITE table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_CONSTRUCTION_SITE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_CONSTRUCTION_SITE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-CONSTRUCTION-SITE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_CROSSBORDER" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_CROSSBORDER.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_CROSSBORDER.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-CROSSBORDER-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_DATALOGGERS table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_DATALOGGERS_ALL.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_DATALOGGERS_ALL.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-DATALOGGERS-ALL-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
		
	printf "\nAdding data into LU_OUTREADER" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OUTREADER_PROTOCOLS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OUTREADER_PROTOCOLS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-OUTREADER-PROTOCOLS-$1-$timestamp.log  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_PUBHEALTHRESITE table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_PUBHEALTHRESITE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_PUBHEALTHRESITE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-PUBHEALTHRESITE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_SERVICE_CATEGORY" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SERVICE_CATEGORY.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SERVICE_CATEGORY_2016.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SERVICE-CATEGORY-$1-$timestamp.log  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
				
	printf "\nAdding data into LU_TARIFF_SPECIAL_AGREEMENTS" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-TARIFF-SPECIAL-AGREEMENTS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SAP_FLOCA" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SAP_FLOCA.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SAP_TARGET_FLOC_MATCHES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SAP-FLOCA-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
 	printf "\nAdding data into LU_SAP_EQUIPMENT" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SAP_EQUIPMENT.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SAP_TARGET_METER_MATCHES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SAP-EQUIPMENT-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SPID_OWC_RETAILER table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SPID_OWC_RETAILER.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SPID_OWC_RETAILER.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SPID-OWC-RETAILER-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_DISCHARGE_VOL_LIMITS" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_DISCHARGE_VOL_LIMITS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_DISCHARGE_VOL_LIMITS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_DISCHARGE_VOL_LIMITS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
			
 	printf "\nAdding data into LU_OWC_RECON_MEASURES" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_RECON_MEASURES.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_RECON_MEASURES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_RECON_MEASURES-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_LANDLORD" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_LANDLORD.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_LANDLORD.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_LANDLORD-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_OWC_TARIFF" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_TARIFF.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_TARIFF.dat ERRORS=1000 log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_TARIFF-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_OWC_TE_METERS" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_TE_METERS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_TE_METERS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_TE_METERS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
 	
  printf "\nAdding data into BT_OWC_TE_DPID_REF" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/BT_OWC_TE_DPID_REF.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/BT_OWC_TE_DPID_REF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/BT_OWC_TE_DPID_REF-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SPID_RANGE table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/DATA/01_INSERT_LU_SPID_RANGE.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SPID_RANGE_DWRCYMRU table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SPID_RANGE_DWRCYMRU.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SPID_RANGE_DWRCYMRU.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_SPID_RANGE_DWRCYMRU-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

	printf "\nAdding data into LU_SPID_RANGE_NOSPID table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SPID_RANGE_NOSPID.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SPID_RANGE_NOSPID.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_SPID_RANGE_NOSPID-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_SS_LANDLORD" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SS_LANDLORD.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SS_LANDLORD.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_SS_LANDLORD-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into LU_OTHER_METER_DPID" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OTHER_METER_DPID.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OTHER_METER_DPID.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OTHER_METER_DPID-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
	printf "\nAdding data into LU_TE_METER_DPID_EXCLUSION table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_TE_METER_DPID_EXCLUSION.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_TE_METER_DPID_EXCLUSION.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_TE_METER_DPID_EXCLUSION-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_OWC_NOT_SENSITIVE" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_NOT_SENSITIVE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_NOT_SENSITIVE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_NOT_SENSITIVE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

 	printf "\nAdding data into LU_OWC_SSW_SPIDS" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_SSW_SPIDS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_SSW_SPIDS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_SSW_SPIDS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  
  printf "\nAdding data into LU_TE_METER_PAIRING" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_TE_METER_PAIRING.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_TE_METER_PAIRING.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_TE_METER_PAIRING-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

  printf "\nAdding data into LU_OWC_SAP_FLOCA" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OWC_SAP_FLOCA.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OWC_SAP_FLOCA.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_OWC_SAP_FLOCA-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
			
  printf "\nAdding data into LU_NOSPID_EXCEPTIONS" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_NOSPID_EXCEPTIONS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_NOSPID_EXCEPTIONS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU_NOSPID_EXCEPTIONS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

##################################                 LU                      ################################################

##################################            load of OWC files            ################################################

	printf "\nLoading data into the OWC tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
  /recload/$1/MOSL/C2W_LOAD_OWC_FILES.sh $1 >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log

##################################                 MO                      ################################################
#Add data to MO tables
	printf "\nLoading data into the MO tables"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	printf "\nAdding data into MO_ORG table"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/MO_ORG.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/MO_ORG.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/MO-ORG-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
##################################                 MO                      ################################################

##################################         START IMPORT STATS TABLES                ################################################
#  printf "\nImport stats from previous build"
#  printf "\nMOUDEL"
#        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nMOUTRAN"
#        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#	printf "\nRECEPTION"
#        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/IMPORT_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1

#################################      RUN PRECLONE_RECON SCRIPTS         #################################################
#  printf "\nRUN PRECLONE RECON SCRIPTS\n"
#  printf "\nRunning PRECLONE_RECON_TARGET"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#      sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TARGET.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
#  printf "\nRunning PRECLONE_RECON_TEACCESS"  | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
#      sqlplus MOUTRAN/$1 @/recload/$1/MOSL/CODE_STORE/01D_AUDITINGREC/PRECLONE_RECON_TEACCESS.sql >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
  

#Display where the logs are stored.
	printf "\nPlease check the logs\n"



