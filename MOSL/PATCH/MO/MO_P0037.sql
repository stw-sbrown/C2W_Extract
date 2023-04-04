------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.CHEUNG
--
-- FILENAME       		: 	MO_P00037.sql
--
-- CREATED        		: 	06/06/2016
--
-- Subversion $Revision: 5817 $	
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
-- V0.01		      06/06/2016	    D.CHEUNG        
--                                                Add field SAPEQUIPMENT NUMBER(10,0) to MO_METER
-- v0.02          06/06/2016      M.Marron        Altered the SAPEQUIPMENT FIELD VARCHAR2(25) and removed the BT_TVP054 new sap field
-- v1.01          13/06/2016      D.Cheung        Add index to SAPEQUIPMENT
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
------------------------------------------------------
--ADD NEW FIELDS
-- ALTER TABLE BT_TVP054 ADD SAPFLOCNUMBER	NUMBER(30,0); not required as field SAP_FLOC already exists and is used
ALTER TABLE MO_METER ADD SAPEQUIPMENT	VARCHAR2(25 BYTE);

--Add Comment
--COMMENT ON COLUMN BT_TVP054.SAPFLOCNUMBER IS 'SAP FLOC Number~~~STW015 - alternative internal key to enable SAP joins';
COMMENT ON COLUMN MO_METER.SAPEQUIPMENT IS 'SAP equipment number - alternative internal key to enable SAP joins';

--Add index on SAPEQUIPMENT
CREATE INDEX SAPEQUIPMENT_IDX ON MO_METER (SAPEQUIPMENT);

commit;
exit;




