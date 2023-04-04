------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	P00040.sql
--
-- CREATED        		: 	22/06/2016
--	
-- DESCRIPTION 		   	: 	This file recreates index and constraint SYS_C00106150 as composite on CUSTOMERNUMBER_PK, STWPROPERTYNUMBER_PK
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author   Description
-- ---------      ----------     -------   ----------------------------------------------------------------
-- V0.01       		22/06/2016     S.Badhan  I-247. Add FK constraint on MO_DISCHARGE_POINT.TARRIFCODE
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_DISCHARGE_POINT ADD CONSTRAINT FK_TARIFFCODE FOREIGN KEY (TARRIFCODE) REFERENCES MO_TARIFF (TARIFFCODE_PK);

commit;
/
show errors;
exit;
