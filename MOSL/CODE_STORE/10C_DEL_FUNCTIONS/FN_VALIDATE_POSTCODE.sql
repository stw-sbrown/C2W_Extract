----------------------------------------------------------------------------------------
-- FUNCTION SPECIFICATION: Postcode Validtion
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : FN_VALIDATE_POSTCODE.sql
--
-- Subversion $Revision: 5870 $
--
-- CREATED        : 20/04/2016
--
-- DESCRIPTION    : Function to validate postcodes according to MOSL specification
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      20/04/2016  K.Burton   Initial Draft
-- V 0.02      03/05/2016  K.Burton   Added 'I' to list of excluded letters for 3rd alpha
--                                    character as per MOSL spec
-- V 0.03      05/05/2016  K.Burton   Added code to pre-format postcodes into CCCC CCC format
--                                    Return type also changed to VARCHAR2	
-- V 0.04      07/06/2016  K.Burton   Changes to validate only outbound postcode. Checks if inbound
--                                    postcode is 3 characters long - if not inbound part is
--                                    dropped and only outbound part is validated and returned
-- V 0.05      27/06/2016  K.Burton   Change to setting of l_outbound variable for issue when TRIM function
--                                    returned NULL causing valid postcodes to be rejected in Delivery
-- V 0.06      07/10/2016  K.Burton   Fixed regular expression for outbound postcode
-- V 0.07      14/10/2016  K.Burton   Return Invalid postcode if any exception errors
-----------------------------------------------------------------------------------------
create or replace
FUNCTION FN_VALIDATE_POSTCODE(PA_PCODE IN VARCHAR2) RETURN VARCHAR2 AS
  l_postcode VARCHAR2(8);
  l_outbound VARCHAR2(4);
  l_inbound VARCHAR2(4);
BEGIN
  l_postcode := PA_PCODE;
  -- V.0.05
  IF TRIM(SUBSTR(l_postcode,1,INSTR(l_postcode,' ')-1)) IS NULL THEN
    l_outbound := TRIM(l_postcode);
  ELSE
    l_outbound := TRIM(SUBSTR(l_postcode,1,INSTR(l_postcode,' ')-1));
  END IF;
  l_inbound := TRIM(SUBSTR(l_postcode,INSTR(l_postcode,' ')+1));

  -- whole postcode validations
  IF L_POSTCODE = 'GIR 0AA' THEN
    RETURN l_postcode;
  END IF;

  IF LENGTH(l_postcode) > 8 THEN
    RETURN 'INVALID';
  END IF;

  -- validate inbound postcode - only check length here - must be 3 characters to be valid
  -- if it's invalid set it to null but do not reject postcode
  IF LENGTH(l_inbound) <> 3 THEN
    l_inbound := NULL;
  END IF;
  
  -- validate outbound postcode
  IF SUBSTR(l_outbound,1,1) IN ('Q','V','X') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_outbound,2,1) IN ('I','J','Z') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_outbound,3,1) IN ('L','M','N','O','Q','R','V','X','Y','Z') THEN
    RETURN 'INVALID';
  END IF;

  IF SUBSTR(l_outbound,4,1) IN ('C','D','F','G','I','J','K','L','O','Q','S','T','U','Z') THEN
    RETURN 'INVALID';
  END IF;

--  IF REGEXP_LIKE(l_outbound, '^([[:alpha:]]{1,2}[[:digit:]]{1,2})$') THEN
  IF REGEXP_LIKE(l_outbound, '^[[:alpha:]]{1,2}[[:digit:]]{1,2}[[:alpha:]]{0,1}$') THEN
    IF l_inbound IS NOT NULL THEN
      l_postcode := l_outbound || ' ' || l_inbound;
    ELSE
      l_postcode := l_outbound;
    END IF;
    RETURN l_postcode;
  END IF;
  
  RETURN 'INVALID';
EXCEPTION
WHEN OTHERS THEN
    RETURN 'INVALID';
END;
/
exit;