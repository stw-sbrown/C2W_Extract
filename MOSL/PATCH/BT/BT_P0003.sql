------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	BT_P0003.sql
--
-- Subversion $Revision: 4596 $	
--
-- CREATED        		: 	28/06/2016
--	
-- DESCRIPTION 		   	: 	Add new columns to BT_TVP054 and BT_TVP163.
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------- ----------------------------------------------------------------
-- V0.01       		28/06/2016     S.Badhan Add new columns to BT_TVP054 and BT_TVP163.
------------------------------------------------------------------------------------------------------------

ALTER TABLE BT_TVP054 ADD FG_TE CHAR(1);	
ALTER TABLE BT_TVP054 ADD FG_MECOMS_RDY CHAR(1);
ALTER TABLE BT_TVP054 ADD NO_PROPERTY_MASTER	NUMBER;
ALTER TABLE BT_TVP054 ADD FG_NMM CHAR(1);

ALTER TABLE BT_TVP163 ADD FG_TE CHAR(1);	
ALTER TABLE BT_TVP163 ADD FG_MECOMS_RDY CHAR(1);
ALTER TABLE BT_TVP163 ADD NO_PROPERTY_MASTER	NUMBER;
ALTER TABLE BT_TVP163 ADD FG_NMM CHAR(1);

commit;
/
show errors;
exit;


