------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0047.sql
--
-- Subversion $Revision: 5706 $	
--
-- CREATED        		: 	04/10/2016
--	
-- DESCRIPTION 		   	: 	Add indexes for performance.
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author    Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.01       		04/10/2016     S.Badhan  Add indexes for performance.
------------------------------------------------------------------------------------------------------------

CREATE INDEX IDX_SUPPLY_POINT_SPIDEFFDATE ON MO_SUPPLY_POINT (SPID_PK, SUPPLYPOINTEFFECTIVEFROMDATE);

CREATE INDEX IDX_METER_REFTREATMENT ON MO_METER (METERREF, METERTREATMENT);

commit;
/
show errors;
exit;


