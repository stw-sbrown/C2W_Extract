------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	BT_P0004.sql
--
-- Subversion $Revision: 4621 $	
--
-- CREATED        		: 	29/06/2016
--	
-- DESCRIPTION 		   	: 	Add new column to BT_SC_UW.
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------- ----------------------------------------------------------------
-- V0.01       		29/06/2016     S.Badhan Add new columns to BT_SC_UW
------------------------------------------------------------------------------------------------------------

ALTER TABLE BT_SC_UW ADD VOLUMETRICRATE	NUMBER;

commit;
/
show errors;
exit;


