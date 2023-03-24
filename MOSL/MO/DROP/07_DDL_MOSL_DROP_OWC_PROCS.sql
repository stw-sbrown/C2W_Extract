------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS DROP 
--
-- AUTHOR         		: 	Surinder Badhan
--
-- FILENAME       		: 	07_DDL_MOSL_DROP_OWC_PROCS.sql
--
-- CREATED        		: 	13/10/2016
--	
-- Subversion $Revision: 5870 $
--
-- DESCRIPTION 		   	: 	Drops OWC procs
--
-- NOTES  				:
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	Date        Author       Description
-- ---------    ----------  -----------  ------------------------------------------------------------------
-- V0.02       	15/02/2016  S,Badhan     Initial version.
------------------------------------------------------------------------------------------------------------

DROP PROCEDURE P_OWC_TRAN_SUPPLY_POINT;
DROP PROCEDURE P_OWC_TRAN_SERVICE_COMPONENT;
DROP PROCEDURE P_OWC_TRAN_METER;
DROP PROCEDURE P_OWC_TRAN_METER_SPID_ASSOC;
DROP PROCEDURE P_OWC_TRAN_TE_METER_DPID_XREF;
DROP PROCEDURE P_OWC_TRAN_METER_READING;

DROP PACKAGE P_MIG_BATCH_OWC;


commit;
exit;

