------------------------------------------------------------------------------
-- TASK				: 	DROP OWC RECEPTION TABLES  
--
-- AUTHOR         		: 	Surinder Badhan
--
-- FILENAME       		: 	02_DDL_DROP_OWC_TABLES.sql
--
-- CREATED        		: 	12/09/2016
--	
-- Subversion $Revision: 5666 $
--
-- DESCRIPTION 		   	: 	Creates all database tables for SAP file reception area
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date        	 	Author         	Description
-- --------------------------------------------------------------------------------------------------------
-- V0.01	       	12/09/2016   	 	S.Badhan      	Initial version
-- V0.02          28/09/2016      D.Cheung        Added Water files tables
--------------------------------------------------------------------------------------------------------
DROP TABLE RECEPTION.OWC_DISCHARGE_POINT;
DROP TABLE RECEPTION.OWC_METER;
DROP TABLE RECEPTION.OWC_METER_DISCHARGE_POINT;
DROP TABLE RECEPTION.OWC_METER_NETWORK;
DROP TABLE RECEPTION.OWC_METER_READING;
DROP TABLE RECEPTION.OWC_METER_SUPPLY_POINT;
DROP TABLE RECEPTION.OWC_SERVICE_COMPONENT;
DROP TABLE RECEPTION.OWC_SUPPLY_POINT;
DROP TABLE RECEPTION.OWC_CALCULATED_DISCHARGE;

DROP TABLE RECEPTION.OWC_DISCHARGE_POINT_W;
DROP TABLE RECEPTION.OWC_METER_W;
DROP TABLE RECEPTION.OWC_METER_DISCHARGE_POINT_W;
DROP TABLE RECEPTION.OWC_METER_NETWORK_W;
DROP TABLE RECEPTION.OWC_METER_READING_W;
DROP TABLE RECEPTION.OWC_METER_SUPPLY_POINT_W;
DROP TABLE RECEPTION.OWC_SERVICE_COMPONENT_W;
DROP TABLE RECEPTION.OWC_SUPPLY_POINT_W;
DROP TABLE RECEPTION.OWC_CALCULATED_DISCHARGE_W;

COMMIT;
show errors;
exit;  
