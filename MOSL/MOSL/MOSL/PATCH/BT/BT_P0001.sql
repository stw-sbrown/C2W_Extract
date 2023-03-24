------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.CHEUNG
--
-- FILENAME       		: 	BT_P00001.sql
--
-- CREATED        		: 	21/04/2016
--
-- Subversion $Revision: 4023 $	
--
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL database
--
-- NOTES  			:	Place a summery at the top of the file with more detailed information 
--					where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ------------------------------------------------
-- V0.01		      21/04/2016	    D.CHEUNG        Add Indexes to BT_METER_FREQ_SCHED table to improve METER procedure performance
--							                                  Add Indexes to BT_METER_SPID table to improve METER procedure performance
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
------------------------------------------------------
--Add new index on BT_METER_FREQ_SCHED
--CREATE INDEX BT_METER_READ_FREQ_IDX2 ON BT_METER_READ_FREQ (NO_EQUIPMENT, NO_PROPERTY); 
--Add new index on BT_METER_SPID
--CREATE INDEX BT_METER_SPID_IDX3 ON BT_METER_SPID (CORESPID); 
/
commit;
exit;




