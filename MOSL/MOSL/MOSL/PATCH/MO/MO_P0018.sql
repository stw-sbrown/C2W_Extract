------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	M.Marron
--
-- FILENAME       		: 	P00018.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	14/04/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL database
--
-- NOTES  			:	This patch drops redundant cloumns on the Service Component Table
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  		:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01       		24/02/2016      N.Henderson     Initial version 
-- 
------------------------------------------------------------------------------------------------------------

--CHANGES
--DROP Columns
------------------------------------------------------
alter table MO_SERVICE_COMPONENT drop column METEREDFSTARIFFCODE;
alter table MO_SERVICE_COMPONENT drop column METEREDNPWTARIFFCODE;
alter table MO_SERVICE_COMPONENT drop column METEREDPWTARIFFCODE;
alter table MO_SERVICE_COMPONENT drop column HWAYDRAINAGETARIFFCODE;
alter table MO_SERVICE_COMPONENT drop column ASSESSEDTARIFFCODE;
alter table MO_SERVICE_COMPONENT drop column SRFCWATERTARRIFCODE;
alter table MO_SERVICE_COMPONENT drop column UNMEASUREDTARIFFCODE;
commit;
exit;
