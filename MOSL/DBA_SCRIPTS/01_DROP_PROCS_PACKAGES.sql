----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Drop al procedures and/or packages in a databse
-- AUTHOR         : Nigel Henderson
-- CREATED        : 25/04/2016
-- DESCRIPTION    : 
-----------------------------------------------------------------------------------------  

DROP PROCEDURE P_MOU_DEL_SUPPLY_POINT;
DROP PROCEDURE P_MOU_DEL_SERVICE_COMPONENT;
DROP PROCEDURE P_MOU_DEL_METER;
DROP PROCEDURE P_MOU_DEL_METER_SUPPLY_POINT;
DROP PROCEDURE P_MOU_DEL_METER_NETWORK;
DROP PROCEDURE P_MOU_DEL_METER_READING;
DROP PROCEDURE P_MOU_DEL_DISCHARGE_POINT;
DROP PROCEDURE P_MOU_DEL_METER_DISCHARGE;

DROP PACKAGE P_MIG_BATCH;
DROP PACKAGE P_DEL_UTIL;
DROP PACKAGE P_MOU_DEL_TARIFF_EXPORT;

commit;
exit;
