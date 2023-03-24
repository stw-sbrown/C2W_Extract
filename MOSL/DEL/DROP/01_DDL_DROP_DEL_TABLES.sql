--Statements to drop all objects associated with delivery tables
--N Henderson
--	
-- Subversion $Revision: 5178 $
--
--14/04/2016
--
----------------------------------------------------------------------------------------
-- Version     Date        Author     Description
-- ---------   ----------  --------   --------------------------------------------------
-- V 0.04      16/08/2016  S.Badhan   Add drop of FN_VALIDATE_GIS
-- V 0.03      16/08/2016  S.Badhan   Added drop of triggers.
-- V 0.02      15/08/2016  S.Badhan   Change order of drop of table to avoid constraint errors.
----------------------------------------------------------------------------------------

DROP TABLE BT_CROSSBORDER_CTRL;
DROP TABLE DEL_SERVICE_COMPONENT;
DROP TABLE DEL_CALCULATED_DISCHARGE;
DROP TABLE DEL_METER_SUPPLY_POINT;
DROP TABLE DEL_METER_NETWORK;
DROP TABLE DEL_METER_READING;
DROP TABLE DEL_METER_DISCHARGE_POINT;
DROP TABLE DEL_DISCHARGE_POINT;
DROP TABLE DEL_METER;
DROP TABLE DEL_SUPPLY_POINT;
commit;

DROP TABLE DEL_SERVICE_COMPONENT_ARC;
DROP TABLE DEL_METER_ARC;
DROP TABLE DEL_METER_SUPPLY_POINT_ARC;
DROP TABLE DEL_METER_NETWORK_ARC;
DROP TABLE DEL_METER_READING_ARC;
DROP TABLE DEL_DISCHARGE_POINT_ARC;
DROP TABLE DEL_METER_DISCHARGE_POINT_ARC;
DROP TABLE DEL_CALCULATED_DISCHARGE_ARC;
DROP TABLE DEL_SUPPLY_POINT_ARC;
commit;

--DROP VIEWS.
DROP VIEW DEL_SERVICE_COMPONENT_MPW_V;
DROP VIEW DEL_SERVICE_COMPONENT_MNPW_V;
DROP VIEW DEL_SERVICE_COMPONENT_UW_V;
DROP VIEW DEL_SERVICE_COMPONENT_WCA_V;
DROP VIEW DEL_SERVICE_COMPONENT_SCA_V;
DROP VIEW DEL_SERVICE_COMPONENT_MS_V;
DROP VIEW DEL_SERVICE_COMPONENT_US_V;
DROP VIEW DEL_SERVICE_COMPONENT_SW_V;
DROP VIEW DEL_SERVICE_COMPONENT_HD_V;
DROP VIEW DEL_SERVICE_COMPONENT_AS_V;
DROP VIEW DEL_SERVICE_COMPONENT_AW_V;
DROP VIEW DEL_DISCHARGE_POINT_TARIFF_V;
DROP VIEW DEL_SUPPLY_POINT_STW_V;
DROP VIEW DEL_SUPPLY_POINT_THW_V;
DROP VIEW DEL_SUPPLY_POINT_UUW_V;
DROP VIEW DEL_SUPPLY_POINT_YOW_V;
DROP VIEW DEL_SUPPLY_POINT_WEW_V;
DROP VIEW DEL_SUPPLY_POINT_WEL_V;
DROP VIEW DEL_SUPPLY_POINT_SSW_V;
DROP VIEW DEL_SUPPLY_POINT_ANW_V;
DROP VIEW DEL_SERVICE_COMPONENT_STW_V;
DROP VIEW DEL_SERVICE_COMPONENT_THW_V;
DROP VIEW DEL_SERVICE_COMPONENT_UUW_V;
DROP VIEW DEL_SERVICE_COMPONENT_YOW_V;
DROP VIEW DEL_SERVICE_COMPONENT_WEW_V;
DROP VIEW DEL_SERVICE_COMPONENT_WEL_V;
DROP VIEW DEL_SERVICE_COMPONENT_SSW_V;
DROP VIEW DEL_SERVICE_COMPONENT_ANW_V;
DROP VIEW DEL_METER_STW_V;
DROP VIEW DEL_METER_THW_V;
DROP VIEW DEL_METER_UUW_V;
DROP VIEW DEL_METER_YOW_V;
DROP VIEW DEL_METER_WEW_V;
DROP VIEW DEL_METER_WEL_V;
DROP VIEW DEL_METER_SSW_V;
DROP VIEW DEL_METER_ANW_V;
DROP VIEW DEL_METER_READING_STW_V;
DROP VIEW DEL_METER_READING_THW_V;
DROP VIEW DEL_METER_READING_UUW_V;
DROP VIEW DEL_METER_READING_YOW_V;
DROP VIEW DEL_METER_READING_WEW_V;
DROP VIEW DEL_METER_READING_WEL_V;
DROP VIEW DEL_METER_READING_SSW_V;
DROP VIEW DEL_METER_READING_ANW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_STW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_THW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_UUW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_YOW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_WEW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_WEL_V;
DROP VIEW DEL_METER_SUPPLY_POINT_SSW_V;
DROP VIEW DEL_METER_SUPPLY_POINT_ANW_V;
DROP VIEW DEL_METER_NETWORK_STW_V;
DROP VIEW DEL_METER_NETWORK_THW_V;
DROP VIEW DEL_METER_NETWORK_UUW_V;
DROP VIEW DEL_METER_NETWORK_YOW_V;
DROP VIEW DEL_METER_NETWORK_WEW_V;
DROP VIEW DEL_METER_NETWORK_WEL_V;
DROP VIEW DEL_METER_NETWORK_SSW_V;
DROP VIEW DEL_METER_NETWORK_ANW_V;
DROP VIEW DEL_DISCHARGE_POINT_STW_V;
DROP VIEW DEL_DISCHARGE_POINT_THW_V;
DROP VIEW DEL_DISCHARGE_POINT_UUW_V;
DROP VIEW DEL_DISCHARGE_POINT_YOW_V;
DROP VIEW DEL_DISCHARGE_POINT_WEW_V;
DROP VIEW DEL_DISCHARGE_POINT_WEL_V;
DROP VIEW DEL_DISCHARGE_POINT_SSW_V;
DROP VIEW DEL_DISCHARGE_POINT_ANW_V;
DROP VIEW DEL_METER_DISCHARGE_STW_V;
DROP VIEW DEL_METER_DISCHARGE_THW_V;
DROP VIEW DEL_METER_DISCHARGE_UUW_V;
DROP VIEW DEL_METER_DISCHARGE_YOW_V;
DROP VIEW DEL_METER_DISCHARGE_WEW_V;
DROP VIEW DEL_METER_DISCHARGE_WEL_V;
DROP VIEW DEL_METER_DISCHARGE_SSW_V;
DROP VIEW DEL_METER_DISCHARGE_ANW_V;
DROP VIEW DEL_CALC_DISCHARGE_STW_V;
DROP VIEW DEL_CALC_DISCHARGE_THW_V;
DROP VIEW DEL_CALC_DISCHARGE_UUW_V;
DROP VIEW DEL_CALC_DISCHARGE_YOW_V;
DROP VIEW DEL_CALC_DISCHARGE_WEW_V;
DROP VIEW DEL_CALC_DISCHARGE_WEL_V;
DROP VIEW DEL_CALC_DISCHARGE_SSW_V;
DROP VIEW DEL_CALC_DISCHARGE_ANW_V;
commit;

--DROP PROCEDURES
DROP PROCEDURE P_DEL_UTIL_ARCHIVE_TABLE;
DROP PROCEDURE P_DEL_UTIL_WRITE_FILE;
DROP PROCEDURE P_MOU_DEL_SUPPLY_POINT;
DROP PROCEDURE P_MOU_DEL_SERVICE_COMPONENT;
DROP PROCEDURE P_MOU_DEL_METER;
DROP PROCEDURE P_MOU_DEL_METER_SUPPLY_POINT;
DROP PROCEDURE P_MOU_DEL_METER_NETWORK;
DROP PROCEDURE P_MOU_DEL_METER_READING;
DROP PROCEDURE P_MOU_DEL_DISCHARGE_POINT;
DROP PROCEDURE P_MOU_DEL_METER_DISCHARGE;
DROP PROCEDURE P_MOU_DEL_CALCULATED_DISCHARGE;
commit;

--DROP FUNCTIONS
DROP FUNCTION FN_VALIDATE_POSTCODE;
DROP FUNCTION FN_VALIDATE_GIS;

--DROP PACKAGES
DROP PACKAGE P_MIG_BATCH;
DROP PACKAGE P_MOU_DEL_TARIFF_EXPORT;


commit;
exit;
