----------------------------------------------------------------------------------------
-- FUNCTION SPECIFICATION: GIS Coordinate Validtion
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : FN_VALIDATE_GIS.sql
--
-- Subversion $Revision: 5194 $
--
-- CREATED        : 29/06/2016
--
-- DESCRIPTION    : Function to validate GIS coordinates according to MOSL specification
---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.02      09/08/2016  S.Badhan   I-330. Return error if either GIS X or Y is null.
-- V 0.01      29/06/2016  K.Burton   Initial Draft
-----------------------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION FN_VALIDATE_GIS (P_GIS_CODE IN VARCHAR2) RETURN VARCHAR2 AS 
  l_gisx NUMBER(7,1);
  l_gisy NUMBER(7,1);
  l_gis_code VARCHAR2(60);
BEGIN
  l_gis_code := TRIM(P_GIS_CODE); -- get rid of any leading or trailing spaces
  
  IF l_gis_code IS NULL THEN -- if no GIS code then set to MOSL defined defaults
    l_gis_code := '82644;5186';
  END IF;
  
  -- basic transformations to try to ignore separator format issues
  l_gis_code := REPLACE(l_gis_code,' ',';'); -- if there is a space within the data replace with ;
  l_gis_code := REPLACE(l_gis_code,',',';'); -- if there is a , within the data replace with ;
  l_gis_code := REPLACE(l_gis_code,'-',';'); -- if there is a - within the data replace with ;
  l_gis_code := REPLACE(l_gis_code,':',';'); -- if there is a : within the data replace with ;
  l_gis_code := REPLACE(l_gis_code,'.',';'); -- if there is a . within the data replace with ;
  l_gis_code := REPLACE(l_gis_code,'/',';'); -- if there is a / within the data replace with ;

  IF INSTR(l_gis_code,';') = 0 OR REGEXP_COUNT(l_gis_code,';') > 1 THEN -- input data must be in general format xxxxx;yyyyy
    RETURN 'Invalid GIS code input format';
  ELSE -- input has the ; separator - now split into x (eastings) and y (northings)
    BEGIN
      l_gisx := TO_NUMBER(TRIM(SUBSTR(l_gis_code,1,INSTR(l_gis_code,';')-1)));
    EXCEPTION -- invalid if the x co-ordinate is not a number or is too large
      WHEN OTHERS THEN
        RETURN 'Invalid GISX value - non-numeric or number too large';
    END;
    BEGIN
      l_gisy := TO_NUMBER(TRIM(SUBSTR(l_gis_code,INSTR(l_gis_code,';')+1)));
    EXCEPTION -- invalid if the y co-ordinate is not a number or is too large
      WHEN OTHERS THEN
        RETURN 'Invalid GISY value - non-numeric or number too large';
    END;
    
    -- if we get here we have 2 numbers for x and y co-ordinates - now check if they are in the correct range
    IF l_gisx < 82644.0 OR l_gisx > 655612.0 THEN
        RETURN 'Invalid GISX value - out of range';
    END IF;
    
    IF l_gisy < 5186.0 OR l_gisy > 657421.0 THEN
        RETURN 'Invalid GISY value - out of range';
    END IF;
  END IF;
  
  -- if we get here we have 2 numbers for x and y co-ordinates - now check if they are in the correct range
  IF l_gisx IS NULL THEN
     RETURN 'Invalid GISX value - is null';
  END IF;
  
  IF l_gisy IS NULL THEN
     RETURN 'Invalid GISY value - is null';
  END IF;

  -- if we get this far then we have 2 numbers for x and y co-ordinates which are within the correct range as per MOSL spec
  -- return new GIS code in xxxxx;yyyy format
  l_gis_code := TO_CHAR(l_gisx) || ';' || TO_CHAR(l_gisy);
  RETURN l_gis_code;
END FN_VALIDATE_GIS;
/
exit;