------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Ola BADMUS
--
-- FILENAME       		: 	P00025.sql
--
-- CREATED        		: 	26/04/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL database
--
-- NOTES  			:	Place a summery at the top of the file with more detailed information 
--					where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author       Description
-- ---------      ----------      -------      ------------------------------------------------------------
-- V0.02       		13/10/2016      S.Badhan     Remove drop of FK_SERVICECOMPONENTREF_PK01, does not exist
-- V0.01       		26/04/2016      M.Marron     Initial version 
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_METER_ADDRESS ADD MANUFACTURER_PK VARCHAR2(32);
ALTER TABLE MO_METER_ADDRESS ADD CONSTRAINT FK_METER_ADDRESS_MAN_COMP FOREIGN KEY (MANUFACTURER_PK, METERSERIALNUMBER_PK) REFERENCES MO_METER(MANUFACTURER_PK, MANUFACTURERSERIALNUM_PK);
--added on 15-06-2016
--ALTER TABLE MO_CUST_ADDRESS DROP CONSTRAINT SYS_C00112289;
--ADDED ON 22/06/2016
CREATE INDEX IND_CUSTOMERID ON MO_ELIGIBLE_PREMISES (CUSTOMERID_PK);

commit;
exit;

