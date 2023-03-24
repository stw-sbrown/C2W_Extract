------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	DEL_P0009.sql
--
--
-- Subversion $Revision: 5194 $	
--
-- CREATED        		: 	01/06/2016
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
-- V0.01	        01/06/2016	    D.Cheung        Increase field length of Delivery <METERREF> fields FOR TE
-- V0.02          15/08/2016      S.Badhan        I-320. Remove schema name from table.
-- V0.03          16/08/2016      S.Badhan        I-320. Add Compile of trigger DEL_METER_READING_TRG.
------------------------------------------------------------------------------------------------------------
-- CHANGES

ALTER TABLE DEL_METER_READING  MODIFY METERREF  NUMBER(15);
ALTER TABLE DEL_METER_READING  MODIFY PREVMETERREF  NUMBER(15);
ALTER TABLE DEL_CALCULATED_DISCHARGE  MODIFY CALCDISCHARGEID_PK  VARCHAR2(15);
ALTER TRIGGER DEL_METER_READING_TRG COMPILE;
/

commit;
/
/
show errors;
exit;