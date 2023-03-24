----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Discharge Point Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 20/04/2016
-- DESCRIPTION    : Trigger to enforce complex validation rules for discharge point delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_DISCHARGE_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_DISCHARGE_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.POSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.POSTCODE);
  END IF;
END;