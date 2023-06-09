--
-- CREATED        		: 	26/05/2016
--	
-- DESCRIPTION 		   	: 	Amend METERREF fields on all MO METER tables to NUMBER(15,0) for TE
--                        Add fields to MO_CALCULATED_DISCHARGE for TE
--
--
-- Subversion $Revision: 5449 $	
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
-- V0.04     13/09/2016  D.Cheung  I-357 - Add constraint on MO_METER_DPIDXREF manufactuer, serialnum, dpid unique
-- V0.03     27/05/2016  D.Cheung  Increase length of DPID_PK on MO_CALCULATED_DISCHARGE for TE
-- V0.02     26/05/2016  D.Cheung  Add fields to MO_CALCULATED_DISCHARGE for TE
--                                 Increase length of CALCDISCHARGEID_PK on MO_CALCULATED_DISCHARGE for TE
-- V0.01		 26/05/2016  D.Cheung  Increase length of METERREF.
--                                 Increase length of DPID_PK ON METER_DPIDXREF
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_METER  MODIFY METERREF  NUMBER(15);
ALTER TABLE MO_METER_DPIDXREF MODIFY METERDPIDXREF_PK NUMBER(15);
ALTER TABLE MO_METER_NETWORK MODIFY MAIN_METERREF NUMBER(15);
ALTER TABLE MO_METER_NETWORK MODIFY SUB_METERREF NUMBER(15);
ALTER TABLE MO_METER_READING  MODIFY METERREF  NUMBER(15);
ALTER TABLE MO_METER_SPID_ASSOC  MODIFY METERREF  NUMBER(15);

ALTER TABLE MO_METER_DPIDXREF MODIFY DPID_PK VARCHAR2(32);
ALTER TABLE MO_METER_DPIDXREF ADD CONSTRAINT PK_02_MAN_SERIAL_DPID UNIQUE (MANUFACTURER_PK,MANUFACTURERSERIALNUM_PK,DPID_PK);

ALTER TABLE MO_CALCULATED_DISCHARGE ADD STWPROPERTYNUMBER_PK	NUMBER(9) CONSTRAINT CH01_STWPROPERTYNUMBER_PK NOT NULL;
ALTER TABLE MO_CALCULATED_DISCHARGE ADD STWACCOUNTNUMBER	NUMBER(10) CONSTRAINT CH01_STWACCOUNTNUMBER NOT NULL;
ALTER TABLE MO_CALCULATED_DISCHARGE ADD STWIWCS	NUMBER(12) CONSTRAINT CH01_STWIWCS NOT NULL;
ALTER TABLE MO_CALCULATED_DISCHARGE ADD REFDESC	VARCHAR2(250) CONSTRAINT CH01_REFDESC NOT NULL;
ALTER TABLE MO_CALCULATED_DISCHARGE ADD TETARIFFBAND CHAR(5);
ALTER TABLE MO_CALCULATED_DISCHARGE ADD TECATEGORY	VARCHAR2(250);

ALTER TABLE MO_CALCULATED_DISCHARGE  MODIFY CALCDISCHARGEID_PK  VARCHAR2(15);
ALTER TABLE MO_CALCULATED_DISCHARGE  MODIFY DPID_PK  VARCHAR2(32);

COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.STWPROPERTYNUMBER_PK IS 'Property Number';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.STWACCOUNTNUMBER IS 'Account Number';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.STWIWCS IS 'IWCS Number';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.REFDESC IS 'Internal Description';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.TETARIFFBAND IS 'Trade Effluent Tariff Band';
COMMENT ON COLUMN MO_CALCULATED_DISCHARGE.TECATEGORY IS 'STW TE Category';

commit;
/
/
show errors;

exit;