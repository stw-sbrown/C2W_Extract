------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	BT_P0004.sql
--
-- Subversion $Revision: 5611 $	
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
-- V0.02          22/09/2016     D.Cheung Add index to BT_CLOCKOVER table
------------------------------------------------------------------------------------------------------------

ALTER TABLE BT_SC_UW ADD VOLUMETRICRATE	NUMBER;

CREATE INDEX IDX_BT_CLOCKOVER_PK ON BT_CLOCKOVER(STWMETERREF_PK,METERREADDATE);

commit;
/
show errors;
exit;


