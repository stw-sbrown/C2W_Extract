------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	DEL_P0010.sql
--
--
-- Subversion $Revision: 5178 $	
--
-- CREATED        		: 	15/06/2016
--	
-- DESCRIPTION 		   	: 	This file contains all ongoing patches that are made to the MOSL DEL database
--
-- NOTES  			      :	Place a summary at the top of the file with more detailed information 
--					where the patch is applied in this script (if needed)
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS :	
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     	  Date            Author          Description
-- ---------      ----------      -------         ----------------------------------------------------------
-- V0.01	        15/06/2016	    D.Cheung        Increase field length of Delivery Special Agreement Factor fields on Service Component
-- V0.02          15/08/2016      S.Badhan        I-320. Remove schema name from table.
-- V0.03          16/08/2016      S.Badhan        I-320. Add Compile of trigger DEL_SERVICE_COMPONENT_TRG.
------------------------------------------------------------------------------------------------------------
-- CHANGES

ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY MPWSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY MNPWSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY AWSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY UWSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY MFSSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY ASSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY USSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY SWSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TABLE DEL_SERVICE_COMPONENT  MODIFY HDSPECIALAGREEMENTFACTOR  NUMBER(5,2);
ALTER TRIGGER DEL_SERVICE_COMPONENT_TRG COMPILE;
/

commit;
/
/
show errors;
exit;