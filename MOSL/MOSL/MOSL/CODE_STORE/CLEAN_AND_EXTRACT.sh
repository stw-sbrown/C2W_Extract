#!/bin/ksh

# remove all batch related folder

rm -r 01A_BATCH_CONTROL
rm -r 01B_KEY_GEN
rm -r 01C_TARIFF_IMPORT
rm -r 01D_AUDITINGREC
rm -r 02_PROPERTY
rm -r 03_CUSTOMER
rm -r 04_ADDRESS
rm -r 05_SUPPLY_POINT
rm -r 06_DISCHARGE_POINT
rm -r 07_SERVICE_COMPONENT
rm -r 08_METER
rm -r 09_METER_READINGS
rm -r 10A_DEL_BATCH_CONTROL
rm -r 10B_DEL_UTILITY
rm -r 10C_DEL_FUNCTIONS
rm -r 11_DEL_SUPPLY_POINT
rm -r 12_DEL_SERVICE_COMPONENT
rm -r 13_DEL_METER
rm -r 14_DEL_METER_SUPPLY_POINT
rm -r 15_DEL_METER_NETWORK
rm -r 16_DEL_METER_READINGS
rm -r 17_DEL_DISCHARGE_POINT
rm -r 18_DEL_METER_DISCHARGE_POINT
rm -r 19_DEL_TARIFF_EXPORT
rm -r 20_DEL_CALC_DISCHARGE

# expand zip files, should only ever be three zipped files. if less, doesn't matter

if [ ! -f 01.zip ]
then
	echo "file 01.zip does not exist!";
else
	unzip 01.zip
fi

if [ ! -f 02.zip ]
then
	echo "file 02.zip does not exist!";
else
	unzip 02.zip
fi

if [ ! -f 03.zip ]
then
	echo "file 03.zip does not exist!";
else
	unzip 03.zip
fi

# Make sure all files and folders have correct permissions assigned. We nay get errors on this.

for files in `ls -l | grep cis | awk '{ print $9 }'`
do
chmod -R 770 $files
done


# Remove zip files

for zips in `ls *.zip`
do
rm $zips
done

