----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Meter Reading Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 19/04/2016
-- Subversion $Revision: 4023 $
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter reading delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_READING_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER_READING
  FOR EACH ROW
DECLARE
  l_init_reading DATE;
  l_last_reading NUMBER(12,0);
BEGIN
  IF( :NEW.METERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Meter Read Date: METEREADDATE cannot be a future date');
  END IF;
  
  BEGIN
    SELECT M.INITIALMETERREADDATE
    INTO l_init_reading
    FROM DEL_METER M
    WHERE M.MANUFACTURER_PK = :NEW.MANUFACTURER_PK
    AND M.MANUFACTURERSERIALNUM_PK = :NEW.MANUFACTURERSERIALNUM_PK;
    
    IF (l_init_reading > :NEW.METERREADDATE) THEN
    RAISE_APPLICATION_ERROR( -20003, 'Invalid Meter Read Date: METEREADDATE cannot be earlier than INITIALMETERREADDATE');
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      RAISE_APPLICATION_ERROR( -20002, 'Missing Initial Meter Read Date: INITIALMETERREADDATE cannot be found');
  END;
  
  BEGIN
    IF (:NEW.METERREAD < (NVL(:NEW.PREVIOUSMETERREADING,0)) AND (:NEW.ROLLOVERINDICATOR = 0)) THEN
      RAISE_APPLICATION_ERROR( -20004, 'Invalid Meter Reading: METERREAD must be > previous METERREAD value if ROLLOVERINDICATOR = 0');
    END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      NULL;
  END;
END;
/
exit;
