create or replace
PROCEDURE P_DEL_UTIL_REF_DATA AS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Reference Data Utility
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_MOU_DEL_REF_DATA.sql
--
-- Subversion $Revision: 5927 $
--
-- CREATED        : 30/08/2016
--
-- DESCRIPTION    : Utility proc to create CSV files of reference data
--
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      30/08/2016  K.Burton   Initial Draft
-- V 0.02      20/10/2016  K.Burton   Adjusted Properties file to exclude 'invented' properties
-----------------------------------------------------------------------------------------    
  l_text VARCHAR2(2000);
  l_rows_written VARCHAR2(10);
  l_delimiter VARCHAR2(1) := ',';
  l_filehandle UTL_FILE.FILE_TYPE;
  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(200);
  l_timestamp VARCHAR2(20);
  
  CURSOR prop_cur IS
    SELECT DISTINCT MSP.STWPROPERTYNUMBER_PK, BATCH.TS_UPDATE TS_MIGRATION_MOSL
    FROM MOUTRAN.MO_SUPPLY_POINT MSP,
      DEL_SUPPLY_POINT DSP,
      MIG_BATCHSTATUS BATCH
    WHERE MSP.SPID_PK = DSP.SPID_PK
    AND BATCH.NO_BATCH = (SELECT MAX(NO_BATCH) FROM MIG_BATCHSTATUS)
    AND NOT EXISTS (SELECT 1 FROM RECEPTION.OWC_SUPPLY_POINT WHERE SPID_PK = MSP.SPID_PK AND STWPROPERTYNUMBER IS NULL);
BEGIN
   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;
   
  -- create propeties CSV file
  l_timestamp := TO_CHAR(SYSDATE,'YYYYMMDD_HH24MI');
  l_filename := 'MOSL_PROPERTY_' || l_timestamp || '.csv';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  


  UTL_FILE.PUT_LINE(l_filehandle, 'PROPERTY_NUMBER,TS_MIGRATION_MOSL');
  FOR t IN prop_cur
  LOOP
    l_text := t.STWPROPERTYNUMBER_PK || ',' || t.TS_MIGRATION_MOSL;
    UTL_FILE.PUT_LINE(l_filehandle, l_text);    
  END LOOP;
  
  UTL_FILE.FCLOSE(l_filehandle);  
  
END P_DEL_UTIL_REF_DATA;
/
exit;