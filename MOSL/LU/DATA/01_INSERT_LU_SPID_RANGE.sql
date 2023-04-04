------------------------------------------------------------------------------
-- TASK				: 	MOSL INSERT LU_SPID_RANGE
--
-- AUTHOR         		: 	Michael Marron
--
-- FILENAME       		: 	01_INSERT_LU_SPID_RANGE.sql
--
-- CREATED        		: 	10/10/2016
-- Subversion $Revision: 5774 $
--	
-- DESCRIPTION 		   	: 	Adds data to LU_SPID_RANGE
--
-- NOTES  			:	
	
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	Date          Author        Description
-- -------      ----------    ------------  ----------------------------------------------------------------
-- v0.01        10/10/2016    S.Badhan      Initial version.
------------------------------------------------------------------------------------------------------------

INSERT INTO LU_SPID_RANGE SELECT * FROM CIS.LU_SPID_RANGE;

commit;
exit;



  
  
