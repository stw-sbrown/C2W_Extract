------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0042.sql
--
-- Subversion $Revision: 5194 $	
--
-- CREATED        		: 	13/07/2016
--	
-- DESCRIPTION 		   	: 	Add VOLUME_LIMIT field to MO_DISCHARGE_POINT
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.01       		13/07/2016     S.Badhan  SAP CR_016 - Add VOLUME_LIMIT Field
-------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_DISCHARGE_POINT ADD VOLUME_LIMIT NUMBER(5,0);

comment on column MO_DISCHARGE_POINT.VOLUME_LIMIT is 'SAP only field - Daily Volume limit per m3';
ALTER TABLE MO_ADDRESS ADD FOREIGN_ADDRESS VARCHAR2 (10);

commit;
/
show errors;
exit;


