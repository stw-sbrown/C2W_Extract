#!/usr/bin/ksh
#
# 
# Author	N.Henderson
# Version	0.6
# $Revision: 4023 $
# Notes	Initial build
# 		Added scripts to drop and re-create all of the BT tables (12/04/2016), NJH
#  		Added the drop and create scripts for the delivery tables (14/04/2016), NJH
# 0.3		N.Henderson. -- Reordered the run oder.  Added some more logging features
# 0.4		N.Henderson  -- Copy of original file and added MIG related tables and views
# 0.5		M.Marron  -- Updated the SQLLDR commands to enable the data loads to work in all enviroments
# 0.6		M.Marron  -- Updated the include all changes made to the PATCH version and add subversion tag


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


# Run an update statistics on all tables before the drop
        printf "\nRunning an update stats first"
	printf "\nMOUDEL"
        sqlplus MOUDEL/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_MOUDEL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nMOUTRAN"
        sqlplus MOUTRAN/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_MOUTRAN.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nRECEPTION"
        sqlplus RECEPTION/$1 @/recload/$1/MOSL/DBA_SCRIPTS/UPDATE_STATS_RECEPTION.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1


##################################         START DROP TABLES                ################################################

#	printf "\nDropping $2 static data" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$timestamp.log
#	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/DROP_MOSL_DB/01_DDL_MOSL_DROP_STATIC_DATA_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
#	printf "\nDropping $2 foreign keys"
#	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/DROP_MOSL_DB/02_DDL_MOSL_FK_DROP_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

	printf "\nDropping MO" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	printf "\nDropping $2 tables" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/03_DDL_MOSL_TABLES_DROP_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nDropping $2 sequences"   | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/DROP/04_DDL_MOSL_DROP_SEQUENCES_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Drop MIG tables	
	printf "\nDropping MIG tables." | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/DROP/01_DDL_DROP_MIG_TABLES.sql  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1


#Drop LU tables
	printf "\nDropping LU tables"   | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/DROP/01_DDL_DROP_LU_TABLES.sql  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Drop BT tables
	printf "\nDropping BT tables"   | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/BT/DROP/01_DDL_DROP_BT_TABLES.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Drop RECEPTION tables
	printf "\nDropping RECEPTION table"   | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/DROP/01_DDL_DROP_TVMNHHDTL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Drop DEL tables
	printf "\nDropping DELIVERY tables" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUDEL/$1 @/recload/$1/MOSL/DEL/DROP/01_DDL_DROP_DEL_TABLES.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nDrop complete\n\n"  
 
	
##################################           END DROP TABLES               ################################################


##################################    CREATE TABLES AND APPLY PATCHES      ################################################
##################################                  BT                     ################################################
#Create BT tables
	printf "\nCreating BT tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	for bt in /recload/$1/MOSL/BT/CREATE/*.sql
	do
	sqlplus MOUTRAN/$1 @$bt >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	done

#Apply patches to BT tables
	printf "\nApplying BT patches" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	for bt in /recload/$1/MOSL/PATCH/BT/*.sql
	do
	sqlplus MOUTRAN/$1 @$bt >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
 	done
##################################                  BT                     ################################################


##################################               RECEPTION                 ################################################
#Create RECEPTION tables
	printf "\nCreating RECEPTION and Populating Table" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus RECEPTION/$1 @/recload/$1/MOSL/RECEPTION/CREATE/01_DDL_CREATE_TVMNHHDTL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
##################################               RECEPTION                 ################################################


##################################                  LU                     ################################################
#Create LU tables
	printf "\nCreating LU tables\n"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/LU/CREATE/01_DDL_MOSL_LOOKUP_TABLES_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Apply patches for LU tables
	printf "\nApplying LU patches" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	for lu in /recload/$1/MOSL/PATCH/LU/*.sql
	do
	printf "\nApplying $lu" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @$lu >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	done
##################################                  LU                     ################################################

##################################                 MIG                     ################################################
#Create MIG tables in MO
#MIG tables are re-created during a patch build
 printf "\nCreating MIG Tables" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
 sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/DROP/01_DDL_DROP_MIG_TABLES.sql  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
#Apply patches to MIG tables
       printf "\nApplying MIG patches" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
       for m in /recload/$1/MOSL/PATCH/MIG/*.sql
       do
       sqlplus MOUTRAN/$1 @$m >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
       done

#Create views on MIG tables
	printf "\nCreating MIG views" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @/recload/$1/MOSL/MIG/VIEWS/01_DDL_CREATE_MIG_VIEWS.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1 
##################################                 MIG                     ################################################

##################################                 MO                      ################################################
#Create MO tables
	printf "\n\nCreating MO tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/01_DDL_MOSL_TABLES_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 primary keys"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/02_DDL_MOSL_PK_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 foreign keys" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/03_DDL_MOSL_FK_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 constraints and static data"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/04_DDL_MOSL_REF_DATA_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 comments on fields and tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/05_COMMENTS_TABLES_FIELDS_ALL.sql >>/recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nAdding $2 sequences\n"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus  MOUTRAN/$1 @/recload/$1/MOSL/MO/CREATE/06_DDL_MOSL_SEQUENCES_ALL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1  

#Apply MO patches
	printf "\nApplying MOSL patches"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	for p in /recload/$1/MOSL/PATCH/MO/*.sql
	do
	printf "\nApplying $p" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlplus MOUTRAN/$1 @$p >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	done
##################################                 MO                      ################################################

##################################               DELIVERY                  ################################################
#Create the Delivery tables
	printf "\nCreating DELIVERY tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/DEL/CREATE/01_DDL_CREATE_DEL.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Create MIG tables in DELIVERY
#	printf "\nCreating MIG tables in MOUDEL" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
#	sqlplus MOUDEL/$1 @/recload/$1/MOSL/MIG/CREATE/01_DDL_CREATE_MIG_TABLES.sql >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	printf "\nMIG tables are not dropped as part of a patch"  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Create Views
	printf "\nCreating DELIVERY views"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/DEL/CREATE/02_DDL_CREATE_DEL_VIEW.sql  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Create Functions
	printf "\nCreating DELIVERY functions"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	sqlplus  MOUDEL/$1 @/recload/$1/MOSL/CODE_STORE/10C_DEL_FUNCTIONS/FN_VALIDATE_POSTCODE.sql  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

#Create Triggers
	printf "\nCreating DELIVERY triggers"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 
	for t in /recload/$1/MOSL/DEL/CREATE/*TRG.sql
	do
	sqlplus MOUDEL/$1 @$t >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	done

#Apply patches to Delivery tables
	printf "\nApplying DELIVERY patches" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	for d in /recload/$1/MOSL/PATCH/DEL/*.sql
	do
	sqlplus MOUDEL/$1 @$d >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	done

##################################               DELIVERY                  ################################################


##################################           ADD STATIC DATA               ################################################
##################################                 LU                      ################################################
#Add data to LU tables
	printf "\n\nLoading data into the LU tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	printf "\nAdding data into LU_CONSTRUCTION_SITE table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_CONSTRUCTION_SITE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_CONSTRUCTION_SITE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-CONSTRUCTION-SITE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_CROSSBORDER" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_CROSSBORDER.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_CROSSBORDER.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-CROSSBORDER-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_DATALOGGERS table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_DATALOGGERS_ALL.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_DATALOGGERS_ALL.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-DATALOGGERS-ALL-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	#printf "\nAdding data into LU_LANDLORD" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	#sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_LANDLORD.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_LANDLORD.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-LANDLORD-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_OUTREADER" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_OUTREADER_PROTOCOLS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_OUTREADER_PROTOCOLS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-OUTREADER-PROTOCOLS-$1-$timestamp.log  >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_PUBHEALTHRESITE table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_PUBHEALTHRESITE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_PUBHEALTHRESITE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-PUBHEALTHRESITE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_SERVICE_CATEGORY" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SERVICE_CATEGORY.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SERVICE_CATEGORY.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SERVICE-CATEGORY-$1-$timestamp.log  >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_SERVICE_COMP_CHARGES" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SERVICE_COMP_CHARGES.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SERVICE_COMP_CHARGES.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SERVICE-COMP-CHARGES-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_SPID_RANGE table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_SPID_RANGE.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_SPID_RANGE.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-SPID-RANGE-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
	
	printf "\nAdding data into LU_TARIFF" | tee -a /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_TARIFF.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_TARIFF.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-TARIFF-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/FULL_BUILD/FULL-BUILD-$1-$timestamp.log 2>&1
		
	printf "\nAdding data into LU_TARIFF_SPECIAL_AGREEMENTS" | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/LU_TARIFF_SPECIAL_AGREEMENTS.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/LU-TARIFF-SPECIAL-AGREEMENTS-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1

##################################                 LU                      ################################################

##################################                 MO                      ################################################
#Add data to MO tables
	printf "\nLoading data into the MO tables"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	printf "\nAdding data into MO_ORG table"  | tee -a /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log
	sqlldr MOUTRAN/$1 control=/recload/$1/MOSL/SQLLDR_FILES/CONTROL_FILES/MO_ORG.ctl data=/recload/$1/MOSL/SQLLDR_FILES/DATA_FILES/MO_ORG.dat log=/recload/$1/MOSL/LOGS/DATA_LOADS/MO-ORG-$1-$timestamp.log >> /recload/$1/MOSL/LOGS/PATCH_BUILD/PATCH-BUILD-$1-$timestamp.log 2>&1
##################################                 MO                      ################################################
##################################                 MO                      ################################################


#Display where the logs are stored.
	printf "\nPlease check the logs\n"



