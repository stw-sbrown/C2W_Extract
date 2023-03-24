------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0044.sql
--
-- Subversion $Revision: 4948 $	
--
-- CREATED        		: 	21/07/2016
--	
-- DESCRIPTION 		   	: 	Add DPID_TYPE field to MO_DISCHARGE_POINT
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.01       		21/07/2016     S.Badhan  SAP CR_020 - Add DPID_TYPE Field
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_DISCHARGE_POINT ADD DPID_TYPE VARCHAR2(9);

COMMENT ON COLUMN MO_DISCHARGE_POINT.DPID_TYPE IS 'Type of DPID';

ALTER TABLE MO_DISCHARGE_POINT ADD CONSTRAINT CH01_DPID_TYPE CHECK (DPID_TYPE IN ('Consent', 'Agreement', 'SVL', 'STDA'));

commit;
/
show errors;
exit;


