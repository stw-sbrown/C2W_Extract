----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Meter Delivery Extract 
-- AUTHOR         : Dominic Cheung
-- CREATED        : 20/04/2016
-- Subversion $Revision: 4023 $
-- DESCRIPTION    : Trigger to enforce complex validation rules for meter delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_METER_TRG
  BEFORE INSERT OR UPDATE ON DEL_METER
  FOR EACH ROW
BEGIN
  --INITIAL METER READ DATE SHOULD BE BEFORE MARKET OPERATION DA, I.E. LESS THAN TODAY DATE
  IF( :NEW.INITIALMETERREADDATE >= SYSDATE ) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Initial Meter Read Date: INITIALMETERREADDATE should be before the Market Operation day');
  END IF;

  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.POSTCODE);
  END IF;
END;
/
exit;
