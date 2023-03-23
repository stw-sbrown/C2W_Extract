------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00010.sql
--
--
-- Subversion $Revision: 4023 $	
--
--Date 						11/03/2016
--Issue: 					Address fields are not large enough
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- 
--
--
-- 
------------------------------------------------------------------------------------------------------------
-- Changes applied
--Increasing addressline sizes in MO_ADDRESS field to accommodate actual data sizes which are more than 60 bytes
----------------------------------------------------------------------------------------------------------------------------------------------
ALTER TABLE MO_ADDRESS MODIFY ( ADDRESSLINE01 VARCHAR2(255));
ALTER TABLE MO_ADDRESS MODIFY ( ADDRESSLINE02 VARCHAR2(255));
ALTER TABLE MO_ADDRESS MODIFY ( ADDRESSLINE03 VARCHAR2(255));
ALTER TABLE MO_ADDRESS MODIFY ( ADDRESSLINE04 VARCHAR2(255));
ALTER TABLE MO_ADDRESS MODIFY ( ADDRESSLINE05 VARCHAR2(255));

commit;
exit;

