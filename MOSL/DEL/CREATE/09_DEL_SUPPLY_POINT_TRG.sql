----------------------------------------------------------------------------------------
-- TRIGGER SPECIFICATION: Supply Point Delivery Extract 
-- AUTHOR         : Kevin Burton
-- CREATED        : 20/04/2016
--	
-- Subversion $Revision: 4023 $
--
-- DESCRIPTION    : Trigger to enforce complex validation rules for supply point delivery extract
-----------------------------------------------------------------------------------------
CREATE OR REPLACE TRIGGER DEL_SUPPLY_POINT_TRG
  BEFORE INSERT OR UPDATE ON DEL_SUPPLY_POINT
  FOR EACH ROW
BEGIN
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.PREMPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.PREMPOSTCODE);
  END IF;
  
  IF NOT FN_VALIDATE_POSTCODE(UPPER(:NEW.CUSTPOSTCODE)) THEN
    RAISE_APPLICATION_ERROR( -20001, 'Invalid Postcode ' || :NEW.PREMPOSTCODE);
  END IF;
END;
/
exit;
