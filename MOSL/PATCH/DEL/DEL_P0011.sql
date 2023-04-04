------------------------------------------------------------------------------
-- TASK		    		    : 	MOSL DEL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	DEL_P0011.sql
--
--
-- Subversion $Revision: 5544 $	
--
-- CREATED        		: 	23/06/2016
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
-- V0.01	        23/06/2016	    D.Cheung        Amend constraint for DEL_SUPPLY_POINT
-- V0.02          01/07/2016      K.Burton        I-267 - Patch no longer required
-- V0.03          07/07/2016      D.Cheung        Amend del meter network trigger to check SPID
-- v0.04          19/09/2016      D.Cheung        MOSL change - main spid cannot be NULL, even for two-level non-market scenario
------------------------------------------------------------------------------------------------------------
-- CHANGES

-- V0.02 
--ALTER TABLE DEL_SUPPLY_POINT DROP CONSTRAINT CH01_PAIRING_WHOLESALER;
--ALTER TABLE DEL_SUPPLY_POINT ADD CONSTRAINT CH01_PAIRING_WHOLESALER CHECK ((PAIRINGREFREASONCODE = 'NOSPID' AND OTHERWHOLESALERID IS NULL) OR (PAIRINGREFREASONCODE = 'NOTELIGIBLE' AND OTHERWHOLESALERID IS NOT NULL AND OTHERWHOLESALERID <> 'SEVERN-W') OR (PAIRINGREFREASONCODE IS NULL AND OTHERWHOLESALERID = 'SEVERN-W'));

-- V0.03
ALTER TABLE DEL_METER_NETWORK ADD MAINNONMARKETFLAG NUMBER(1,0);
ALTER TABLE DEL_METER_NETWORK DROP CONSTRAINT CH01_MNSPIDPK;
--ALTER TABLE DEL_METER_NETWORK ADD CONSTRAINT CH01_MNSPIDPK CHECK ((SPID_PK IS NOT NULL) OR (MAINNONMARKETFLAG = 1));
ALTER TABLE DEL_METER_NETWORK ADD CONSTRAINT CH01_MNSPIDPK CHECK (SPID_PK IS NOT NULL);

commit;
/
/
show errors;
exit;