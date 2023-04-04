------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	K.Burton
--
-- FILENAME       		: 	MO_P0048.sql
--
-- Subversion $Revision: 5817 $	
--
-- CREATED        		: 	05/10/2016
--	
-- DESCRIPTION 		   	: 	Table alterations for MO_METER_DPIDXREF for OWC TE meters
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author    Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.01       		05/10/2016     K.Burton  Initial version
-- V0.02          05/10/2016     K.Burton  Added check on MO_SUPPLY_POINT for OWC updates
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_METER_DPIDXREF DROP CONSTRAINT FK_METER_READING_MANUFACT01;

ALTER TABLE MO_METER_DPIDXREF ADD OWC VARCHAR2(32);


create or replace
TRIGGER MO_METER_DPIDXREF_TRG
  BEFORE INSERT OR UPDATE ON MO_METER_DPIDXREF
  FOR EACH ROW
DECLARE
  l_count NUMBER;
BEGIN
  IF :NEW.OWC IS NULL THEN
    SELECT COUNT(*) INTO l_count
    FROM MO_METER
    WHERE MANUFACTURER_PK = :NEW.MANUFACTURER_PK
    AND MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK;
    
    IF l_count = 0 THEN
      RAISE_APPLICATION_ERROR( -20001, 'Meter does not exist in MO_METER table');
    END IF;
  ELSE
    SELECT COUNT(*) INTO l_count
    FROM MO_SUPPLY_POINT
    WHERE SPID_PK = :NEW.SPID;
    
    IF l_count = 0 THEN
      RAISE_APPLICATION_ERROR( -20002, 'SPID does not exist in MO_SUPPLY_POINT table');
    END IF;
  END IF;
END;
/

commit;
/
show errors;
exit;


