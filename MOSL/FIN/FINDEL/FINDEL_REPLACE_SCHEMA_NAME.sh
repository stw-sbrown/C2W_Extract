##--
##-- Replace schema MOUTRAN to FINTRAN for delivery area
##--
##-- Subversion $Revision: 5391 $	
##--
##----------------------------------------------------------------------------------------
##-- Version     Date        Author     Description
##-- ---------   ----------  --------   --------------------------------------------------
##-- V 0.01      07/09/2016  S.Badhan   Intial Version.
##----------------------------------------------------------------------------------------

sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/DEL/CREATE/02_DDL_CREATE_DEL_VIEW.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/DEL/CREATE/ARCHIVE/02_DDL_CREATE_DEL_VIEW.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/10B_DEL_UTILITY/P_DEL_UTIL_REF_DATA.sql 
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/11_DEL_SUPPLY_POINT/P_MOU_DEL_SUPPLY_POINT.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/12_DEL_SERVICE_COMPONENT/P_MOU_DEL_SERVICE_COMPONENT.sql 
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/13_DEL_METER/P_MOU_DEL_METER.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/14_DEL_METER_SUPPLY_POINT/P_MOU_DEL_METER_SUPPLY_POINT.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/15_DEL_METER_NETWORK/P_MOU_DEL_METER_NETWORK.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/16_DEL_METER_READINGS/P_MOU_DEL_METER_READING.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/17_DEL_DISCHARGE_POINT/P_MOU_DEL_DISCHARGE_POINT.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/18_DEL_METER_DISCHARGE_POINT/P_MOU_DEL_METER_DISCHARGE.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/19_DEL_TARIFF_EXPORT/P_MOU_DEL_TARIFF_EXPORT.pkb
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/CODE_STORE/20_DEL_CALC_DISCHARGE/P_MOU_DEL_CALCULATED_DISCHARGE.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/PATCH/DEL/DEL_P0005.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/PATCH/DEL/DEL_P0007.sql
sed -i 's/MOUTRAN/FINTRAN/g' /recload/DOWD/FIN/PATCH/DEL/DEL_P0008.sql

