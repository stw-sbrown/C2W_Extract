------------------------------------------------------------------------------
-- TASK				: 	DROP SAP RECEPTION TABLES  
--
-- AUTHOR         		: 	Kevin Burton
--
-- FILENAME       		: 	01_DDL_DROP_SAP_RECEPTION_TABLES.sql
--
-- CREATED        		: 	21/07/2016
--	
-- Subversion $Revision: 5083 $
--
-- DESCRIPTION 		   	: 	Creates all database tables for SAP file reception area
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         	Description
-- --------------------------------------------------------------------------------------------------------
-- V0.01	       	21/07/2016   	 	K.Burton      	Initial version
--------------------------------------------------------------------------------------------------------
DROP TABLE RECEPTION.SAP_DISCHARGE_POINT;
DROP TABLE RECEPTION.SAP_METER;
DROP TABLE RECEPTION.SAP_METER_DISCHARGE_POINT;
DROP TABLE RECEPTION.SAP_METER_NETWORK;
DROP TABLE RECEPTION.SAP_METER_READING;
DROP TABLE RECEPTION.SAP_METER_SUPPLY_POINT;
DROP TABLE RECEPTION.SAP_SERVICE_COMPONENT;
DROP TABLE RECEPTION.SAP_SUPPLY_POINT;
DROP TABLE RECEPTION.SAP_CALCULATED_DISCHARGE;

COMMIT;
show errors;
exit;  
