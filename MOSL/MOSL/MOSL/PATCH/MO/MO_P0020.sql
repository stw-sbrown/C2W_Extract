------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.CHEUNG
--
-- FILENAME       		: 	MO_P00020.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	20/04/2016
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
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ------------------------------------------------
-- V0.01		      20/04/2016	    D.CHEUNG        Drop Constraint RF01_REMOTEREADTYPE on MO_METER
--							                                  ADD constraint RF01_REMOTEREADTYPE MO_METER_READING check (REMOTEREADTYPE IN ('TOUCH', '1WAYRAD', '2WRAD', 'GPRS', 'OTHER'))
--                                                ADD new field PIPESIZE to MO_SERVICE_COMPONENT
--
--
--
--
-- 
------------------------------------------------------------------------------------------------------------
--CHANGES
------------------------------------------------------
--DROP CONSTRAINT RF01_REMOTEREADTYPE
ALTER TABLE MO_METER DROP CONSTRAINT RF01_REMOTEREADTYPE;
--RECREATE CONSTRAINT WITH NEW VALUES
ALTER TABLE MO_METER ADD CONSTRAINT RF01_REMOTEREADTYPE CHECK (REMOTEREADTYPE IN ('TOUCH', '1WAYRAD', '2WRAD', 'GPRS', 'OTHER'));

--ADD PIPESIZE COLUMN TO MO_SERVICE_COMPONENT
ALTER TABLE MO_SERVICE_COMPONENT ADD PIPESIZE NUMBER(4,0);
COMMENT ON COLUMN MO_SERVICE_COMPONENT.PIPESIZE IS 'PIPESIZE~~~D2071 - Pipe Size - value in mm';

commit;
exit;




