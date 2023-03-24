create or replace
PROCEDURE P_SAP_DEL_UTIL_REF_DATA AS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Reference Data Utility
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_REF_DATA.sql
--
-- Subversion $Revision: 5291 $
--
-- CREATED        : 14/06/2016
--
-- DESCRIPTION    : Utility proc to create CSV files of reference data for SAP
--                  Lists Trading Party and Tariff Details
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      13/05/2016  K.Burton   Initial Draft
-- V 0.02      11/07/2016  K.Burton   CR_010 - new csv files for all properties and meters
--                                    included in the SAP upload files
--                                    CR_012 - additional column ORGDESCRIPTION added to 
--                                    SAP_TRADING_PARTIES csv file
-- V 0.03      27/07/2016  K.Burton   Added POSTCODE to SAP_PROPERTY csv file
-- V 0.04      30/08/2016  K.Burton   Added TS_MIGRATION_SAP to SAP_PROPERTY csv file
-----------------------------------------------------------------------------------------    
  l_text VARCHAR2(2000);
  l_rows_written VARCHAR2(10);
  l_delimiter VARCHAR2(1) := ',';
  l_filehandle UTL_FILE.FILE_TYPE;
  l_filepath VARCHAR2(30) := 'SAPDELEXPORT';
  l_filename VARCHAR2(200);
  l_timestamp VARCHAR2(20);
  
  CURSOR tp_cur IS
    SELECT ORGID_PK, ORGNAME, ORGTYPE, ORGDESCRIPTION FROM SAPTRAN.MO_ORG;
    
  CURSOR tf_cur IS
    SELECT MT.TARIFFCODE_PK,
      MT.TARIFFEFFECTIVEFROMDATE,
      NULL EFFECTIVETODATE,
      MT.TARIFFNAME,
      MTV.APPLICABLESERVICECOMPONENT,
      MTV.TARIFFSTATUS
    FROM SAPTRAN.MO_TARIFF_VERSION MTV, SAPTRAN.MO_TARIFF MT
    WHERE MT.TARIFFCODE_PK = MTV.TARIFFCODE_PK;

  CURSOR prop_cur IS
    SELECT PRM.STWPROPERTYNUMBER,
           PRM.SAPFLOCNUMBER,
           COB.POSTCODE, 
           BATCH.TS_UPDATE TS_MIGRATION_SAP
    FROM SAP_DEL_PREM PRM, 
         SAP_DEL_COB COB,
         MIG_BATCHSTATUS BATCH
    WHERE PRM.SAPFLOCNUMBER = COB.SAPFLOCNUMBER
    AND BATCH.NO_BATCH = (SELECT MAX(NO_BATCH) FROM MIG_BATCHSTATUS);
    
  CURSOR m_cur IS
    SELECT STWMETERREF,SAPEQUIPMENT FROM SAP_DEL_DEV;  
        
BEGIN
  -- create trading parties CSV file
  l_timestamp := TO_CHAR(SYSDATE,'YYMMDDHH24MI');
  l_filename := 'SAP_TRADING_PARTIES_' || l_timestamp || '.csv';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  UTL_FILE.PUT_LINE(l_filehandle, 'TRADING_PARTY_ID,TRADING_PARTY_NAME,TRADING_PARTY_ROLE,TRADING_PARTY_DESC'); -- header
  FOR t IN tp_cur
  LOOP
    l_text := t.ORGID_PK || ',' || t.ORGNAME || ',' || t.ORGTYPE || ',' || t.ORGDESCRIPTION;
    UTL_FILE.PUT_LINE(l_filehandle, l_text);    
  END LOOP;
  
  UTL_FILE.FCLOSE(l_filehandle);
  
  -- create tariffs CSV file
  l_filename := 'SAP_TARIFFS_' || l_timestamp || '.csv';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');
  
  UTL_FILE.PUT_LINE(l_filehandle, 'TARIFF_CODE,EFFECTIVE_FROM_DATE,EFFECTIVE_TO_DATE,TARIFF_NAME,SERVICE_COMPONENT_TYPE,TARIFF_STATUS'); -- header
  FOR t IN tf_cur
  LOOP
    l_text := t.TARIFFCODE_PK || ',' || t.TARIFFEFFECTIVEFROMDATE || ',' || t.EFFECTIVETODATE || ',' || t.TARIFFNAME || ',' || t.APPLICABLESERVICECOMPONENT || ',' || t.TARIFFSTATUS;
    UTL_FILE.PUT_LINE(l_filehandle, l_text);    
  END LOOP;
  
  UTL_FILE.FCLOSE(l_filehandle);  
  
  -- **** V 0.02 ****
  -- create propeties CSV file
  l_filename := 'SAP_PROPERTY_' || l_timestamp || '.csv';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  


  UTL_FILE.PUT_LINE(l_filehandle, 'PROPERTY_NUMBER,FLOC_NUMBER,POSTCODE,TS_MIGRATION_SAP');
  FOR t IN prop_cur
  LOOP
    l_text := t.STWPROPERTYNUMBER || ',' || t.SAPFLOCNUMBER || ',' || t.POSTCODE || ',' || t.TS_MIGRATION_SAP;
    UTL_FILE.PUT_LINE(l_filehandle, l_text);    
  END LOOP;
  
  UTL_FILE.FCLOSE(l_filehandle);  
  
  -- create meters CSV file
  l_filename := 'SAP_METER_' || l_timestamp || '.csv';
  l_filehandle := UTL_FILE.FOPEN(l_filepath, l_filename, 'A');  


  UTL_FILE.PUT_LINE(l_filehandle, 'TARGET_EQUIP_NO,SAP_EQUIP_NO');
  FOR t IN m_cur
  LOOP
    l_text := t.STWMETERREF || ',' || t.SAPEQUIPMENT;
    UTL_FILE.PUT_LINE(l_filehandle, l_text);    
  END LOOP;
  
  UTL_FILE.FCLOSE(l_filehandle);   -- **** V 0.02 ****
  
END P_SAP_DEL_UTIL_REF_DATA;
/
exit;