------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	DEL_P0012.sql
--
--
-- Subversion $Revision: 5194 $	
--
-- CREATED        		: 	11/08/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					            where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS :	
---------------------------- Modification History ----------------------------------------------------------
--
-- Version  Date        Author      Description
-- -------  ----------  ----------- ----------------------------------------------------------
-- V0.01	  11/08/2016	S.Badhan    I-331.Amend constraint for DEL_SUPPLY_POINT to allow null on SEWCHARGEABLEMETERSIZE if PRIVATETE.
------------------------------------------------------------------------------------------------------------

ALTER TABLE DEL_METER DROP CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE;
ALTER TABLE DEL_METER ADD CONSTRAINT CH01_SEWCHARGEABLEMETERSIZE CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATEWATER', 'CROSSBORDER')) AND SEWCHARGEABLEMETERSIZE IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATEWATER'))));

ALTER TABLE DEL_METER DROP CONSTRAINT CH01_RETURNTOSEWER;
ALTER TABLE DEL_METER ADD CONSTRAINT CH01_RETURNTOSEWER CHECK (((METERTREATMENT IN ('SEWERAGE', 'PRIVATEWATER', 'CROSSBORDER')) AND RETURNTOSEWER IS NOT NULL) OR ((METERTREATMENT NOT IN ('SEWERAGE', 'PRIVATEWATER'))));

commit;
/
show error;
exit;