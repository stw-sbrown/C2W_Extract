--
-- CREATED        		: 	24/05/2016
--	
-- DESCRIPTION 		   	: 	Add columns to MO_DISCHARGE_POINT
--
--
-- Subversion $Revision: 4284 $	
--							
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version   Date        Author    Description
-- --------  ----------  --------  --------------------------------------------------------------
-- V0.02		 25/05/2016  S.Badhan  Increase length of NO_IWCS.
-- V0.01		 24/05/2016  S.Badhan  Add columns to MO_DISCHARGE_POINT for TE.
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_DISCHARGE_POINT  MODIFY DPID_PK  VARCHAR2(32);

ALTER TABLE MO_DISCHARGE_POINT  ADD  (NO_IWCS           NUMBER(10),
                                      NO_SAMPLE_POINT   NUMBER(8),
                                      CONSENT_NO        VARCHAR2(7),
                                      NO_ACCOUNT        NUMBER(9) );
                                      
COMMENT ON COLUMN MO_DISCHARGE_POINT.NO_IWCS IS 'IWCS Number';
COMMENT ON COLUMN MO_DISCHARGE_POINT.NO_SAMPLE_POINT IS 'Sample Point';
COMMENT ON COLUMN MO_DISCHARGE_POINT.CONSENT_NO IS 'Consent Number';
COMMENT ON COLUMN MO_DISCHARGE_POINT.NO_ACCOUNT IS 'Account Number';

ALTER TABLE MO_DISCHARGE_POINT  MODIFY NO_IWCS  NUMBER(12);

COMMIT;

exit;