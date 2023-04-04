#!/bin/ksh
# Subversion  $Revision: 4064 $
# 0.1 Intial version Created by N.Henderson
# 0.2 M.Marron Updated to removed data from all deployment folders 25/05/2016
#               Added check on SID just to be sure user is in correct directory (i.e DOWD or DOWS or DOWP

# SID Check
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

umask 000

# Remove Databace Create/update/patch scripts
printf "\nRemoving BT Folder"
rm -r  BT
printf "\nRemoving CODE_STORE Folder"
rm -r  CODE_STORE
printf "\nRemoving DBA_SCRIPTS Folder"
rm -r  DBA_SCRIPTS
printf "\nRemoving DEL Folder"
rm -r  DEL
printf "\nRemoving LU Folder"
rm -r  LU
printf "\nRemoving MIG Folder"
rm -r  MIG
printf "\nRemoving MIG Folder"
rm -r  MO
printf "\nRemoving MO Folder"
rm -r  PATCH
printf "\nRemoving PATCH Folder"
rm -r  RECEPTION
printf "\nRemoving RECEPTION Folder"
rm -r  SQLLDR_FILES


# Remove MOSL Stored Procedure files
#rm -r /CODE_STORE/01A_BATCH_CONTROL
#rm -r /CODE_STORE/01B_KEY_GEN
#rm -r /CODE_STORE/01C_TARIFF_IMPORT
#rm -r /CODE_STORE/01D_AUDITINGREC
#rm -r /CODE_STORE/02_PROPERTY
#rm -r /CODE_STORE/03_CUSTOMER
#rm -r /CODE_STORE/04_ADDRESS
#rm -r /CODE_STORE/05_SUPPLY_POINT
#rm -r /CODE_STORE/06_DISCHARGE_POINT
#rm -r /CODE_STORE/07_SERVICE_COMPONENT
#rm -r /CODE_STORE/08_METER
#rm -r /CODE_STORE/09_METER_READINGS
#rm -r /CODE_STORE/10A_DEL_BATCH_CONTROL
#rm -r /CODE_STORE/10B_DEL_UTILITY
#rm -r /CODE_STORE/10C_DEL_FUNCTIONS
#rm -r /CODE_STORE/11_DEL_SUPPLY_POINT
#rm -r /CODE_STORE/12_DEL_SERVICE_COMPONENT
#rm -r /CODE_STORE/13_DEL_METER
#rm -r /CODE_STORE/14_DEL_METER_SUPPLY_POINT
#rm -r /CODE_STORE/15_DEL_METER_NETWORK
#rm -r /CODE_STORE/16_DEL_METER_READINGS
#rm -r /CODE_STORE/17_DEL_DISCHARGE_POINT
#rm -r /CODE_STORE/18_DEL_METER_DISCHARGE_POINT
#rm -r /CODE_STORE/19_DEL_TARIFF_EXPORT
#rm -r /CODE_STORE/20_DEL_CALC_DISCHARGE

# expand zip files, should only ever be three zipped files. if less, doesn't matter
printf "\nUnzipping File 01"
if [ ! -f 01.zip ]
then
	echo "file 01.zip does not exist!";
else
	unzip -a 01.zip
fi
printf "\nUnzipping File 02"
if [ ! -f 02.zip ]
then
	echo "file 02.zip does not exist!";
else
	unzip -a 02.zip
fi
printf "\nUnzipping File 03"
if [ ! -f 03.zip ]
then
	echo "file 03.zip does not exist!";
else
	unzip -a 03.zip
fi

# Make sure all files and folders have correct permissions assigned. We nay get errors on this.
printf "\nAppling correct permisions to all Files and folders"
for files in `ls -l | grep cis | awk '{ print $9 }'`
do
chmod -R 770 $files
done


# Remove zip files
printf "\nRemoving ZIP files"
for zips in `ls *.zip`
do
rm $zips
done

