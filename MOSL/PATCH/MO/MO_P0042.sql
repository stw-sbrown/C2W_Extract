------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	D.Cheung
--
-- FILENAME       		: 	MO_P0042.sql
--
-- Subversion $Revision: 4841 $	
--
-- CREATED        		: 	04/07/2016
--	
-- DESCRIPTION 		   	: 	Add MASTER_PROPERTY field to MO_METER_NETWORK AND MO_METER
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author   Description
-- ---------      ----------     -------   ----------------------------------------------------------------
-- V0.01       		04/07/2016     D.Cheung  CR_021 - Add MASTER_PROPERTY Field
--                                         ALTER MAIN_SPID AND SUB_SPID set NULLABLE
-- v0.02          06/07/2016     D.Cheung  Add flag to indicate if AGGREGATED (Pseudo) network
-- v0.03          07/07/2016     D.Cheung  (SAP) CR_007 - Add METER_MODEL to MO_METER (for SAP only)
-- v0.04          11/07/2016     D.Cheung  (SAP) CR_014 - Add UNITOFMEASURE to MO_METER (for SAP only)
------------------------------------------------------------------------------------------------------------
ALTER TABLE MO_METER_NETWORK ADD MASTER_PROPERTY NUMBER(9,0) CONSTRAINT CH02_MASTERPROPERTY NOT NULL;
ALTER TABLE MO_METER_NETWORK DROP CONSTRAINT CH01_MAINSPIDNWRK;
ALTER TABLE MO_METER_NETWORK DROP CONSTRAINT CH01_SUBSPIDNWRK;

COMMENT ON COLUMN MO_METER_NETWORK.MASTER_PROPERTY IS 'Top Master Property Number at Head of Network';

ALTER TABLE MO_METER ADD MASTER_PROPERTY NUMBER(9,0);
COMMENT ON COLUMN MO_METER.MASTER_PROPERTY IS 'For Aggregated Properties - Master Head Property';

ALTER TABLE MO_METER_NETWORK ADD AGG_NET_FLAG NUMBER(1,0);
COMMENT ON COLUMN MO_METER_NETWORK.AGG_NET_FLAG IS 'Flag to indicate if AGGREGATED (Pseudo) network';

ALTER TABLE MO_METER ADD METER_MODEL VARCHAR2(15);
COMMENT ON COLUMN MO_METER.METER_MODEL IS 'SAP ONLY - Meter Model';

ALTER TABLE MO_METER ADD UNITOFMEASURE VARCHAR2(6);
COMMENT ON COLUMN MO_METER.METER_MODEL IS 'SAP ONLY - Pre-Transform CD_UNIT_OF_MEASURE Target code value';

COMMENT ON COLUMN MO_METER.METERLOCATIONDESC IS 'SAP ONLY - Pre-Transform LOC_STD_EQUIP_41 Target code value - Maps to first part of concatenated FreeDescriptor';


commit;
/
show errors;
exit;


