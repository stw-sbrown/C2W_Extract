--
-- CREATED        		: 	26/05/2016
--	
-- DESCRIPTION 		   	: 	Add constraint for SERVICECOMPONENT and SPID
--
--
-- Subversion $Revision: 4284 $	
--							
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version   Date        Author    Description
-- --------  ----------  --------  --------------------------------------------------------------
-- V0.01		 26/05/2016  S.Badhan  Add unique constraint for SERVICECOMPONENT and SPID
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_SERVICE_COMPONENT ADD CONSTRAINT CH02_SPID_COMTYPE UNIQUE (SERVICECOMPONENTTYPE, SPID_PK);

commit;
/
show errors;

exit;