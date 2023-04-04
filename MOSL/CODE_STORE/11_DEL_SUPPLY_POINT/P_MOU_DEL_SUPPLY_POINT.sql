create or replace
PROCEDURE P_MOU_DEL_SUPPLY_POINT(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                               no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                               return_code IN OUT NUMBER ) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Supply Point Delivery
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_MOU_DEL_SUPPLY_POINT.sql
--
-- Subversion $Revision: 5440 $
--
-- CREATED        : 07/04/2016
--
-- DESCRIPTION    : Procedure to create the Supply Point MOSL Upload file
--                  Queries Transform tables and populates table DEL_SUPPLY_POINT
--                  Writes to file SUPPLY_POINT_SEVERN-W_<date/timestamp>.dat
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      07/04/2016  K.Burton   Initial Draft
-- V 0.02      22/04/2016  K.Burton   Removed TRIM from PREMADDRESSLINE01 and
--                                    CUSTADDRESSLINE01 to work around issue I-112
-- V 0.03      26/04/2016  K.Burton   Changed call to ArchiveDeliveryTable procedure
--                                    now calls P_DEL_UTIL_ARCHIVE_TABLE stored procedure
-- V 0.04      28/04/2016  K.Burton   Changes to cur_supply_point 
--                                    Output format change to RATEABLEVALUE (Defect 15)
-- V 1.01      13/05/2016  K.Burton   Changes to file write logic to accomodate cross border
--                                    files
-- V 1.02      16/08/2916  K.Burton   SAP Defect 124 - SIC codes need to be 5 characters for SAP 
--                                    - left pad with leading 0's where required.
-- V 1.03      25/08/2016  S.Badhan   I-320. If user FINDEL use directory FINEXPORT.
-- V 1.04      01/09/2016  K.Burton   Updates for splitting STW data into 3 batches
-----------------------------------------------------------------------------------------

  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_DEL_SUPPLY_POINT';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  l_progress                    VARCHAR2(100);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE;
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_written              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_rows_written                MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- rows written to each file
  l_no_row_dropped_cb           MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  -- count of cross border rows NOT output to any file
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;
  l_count                       NUMBER;

  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(200);
  l_tablename VARCHAR2(100) := 'DEL_SUPPLY_POINT';

  l_sql VARCHAR2(2000);
  
  -- Cross Border Control cursor
  CURSOR cb_cur IS
    SELECT WHOLESALER_ID, RUN_FLAG
    FROM BT_CROSSBORDER_CTRL;
--    WHERE RUN_FLAG = 1;
    
  -- Cursor updated following changes to MO_SUPPLY_POINT and MO_CUST_ADDRESS to link
  -- to property numbers - V0.04
  CURSOR cur_supply_point IS
    SELECT MSP.SPID_PK, 
           MSP.WHOLESALERID_PK WHOLESALERID,
           MSP.RETAILERID_PK RETAILERID,
           MSP.SERVICECATEGORY,
           TO_DATE(TO_CHAR(NVL(MSP.SUPPLYPOINTEFFECTIVEFROMDATE,TO_DATE('2016-04-01','YYYY-MM-DD')),'YYYY-MM-DD'),'YYYY-MM_DD') SUPPLYPOINTEFFECTIVEFROMDATE,
           MSP.PAIRINGREFREASONCODE,
           MSP.OTHERWHOLESALERID,  -- Must be populated with valid wholesaler id if PAIRINGREFREASONCODE is NULL
           MSP.MULTIPLEWHOLESALERFLAG,
           NVL(MSP.DISCONRECONDEREGSTATUS,'REC') DISCONRECONDEREGSTATUS,
           MEP.VOABAREFERENCE,  -- Must be populated only if VOABAREFRSNCODE is NULL
           MEP.VOABAREFRSNCODE, -- Must be populated only if VOABAREFERENCE is NULL
           MEP.UPRN,  -- Must be populated only if UPRNREASONCODE is NULL
           MEP.UPRNREASONCODE,  -- Must be populated only if UPRN is NULL
           NVL(MC.CUSTOMERCLASSIFICATION,'NA') CUSTOMERCLASSIFICATION,
           MEP.PUBHEALTHRELSITEARR,
           MEP.NONPUBHEALTHRELSITE,
           MEP.NONPUBHEALTHRELSITEDSC,  -- Must be populated if NONPUBHEALTHRELSITE = 1
--           MC.STDINDUSTRYCLASSCODE,
           TO_CHAR(MC.STDINDUSTRYCLASSCODE,'00000') STDINDUSTRYCLASSCODE,  -- V 1.02 
           MC.STDINDUSTRYCLASSCODETYPE,  -- Must be populated if STDINDUSTRYCLASSCODE is not NULL
           MEP.RATEABLEVALUE,
           MEP.OCCUPENCYSTATUS,
           MEP.BUILDINGWATERSTATUS,
           MSP.LANDLORDSPID,  -- Must be valid MOSL supplied SPID if populated
           MEP.SECTION154 ,
           MC.CUSTOMERNAME,
           MC.CUSTOMERBANNERNAME,
           PMA.LOCATIONFREETEXTDESCRIPTOR PREMLOCATIONFREETEXTDESCRIPTOR,
           PMA.SECONDADDRESABLEOBJECT PREMSECONDADDRESABLEOBJECT,
           PMA.PRIMARYADDRESSABLEOBJECT PREMPRIMARYADDRESSABLEOBJECT,
--           TRIM(PMA.ADDRESSLINE01) PREMADDRESSLINE01, -- V0.02
           PMA.ADDRESSLINE01 PREMADDRESSLINE01,
           TRIM(PMA.ADDRESSLINE02) PREMADDRESSLINE02,
           TRIM(PMA.ADDRESSLINE03) PREMADDRESSLINE03,
           TRIM(PMA.ADDRESSLINE04) PREMADDRESSLINE04,
           TRIM(PMA.ADDRESSLINE05) PREMADDRESSLINE05,
           TRIM(PMA.POSTCODE) PREMPOSTCODE,
           TRIM(PMA.PAFADDRESSKEY) PREMPAFADDRESSKEY,
           TRIM(CMA.LOCATIONFREETEXTDESCRIPTOR) CUSTLOCATIONFREETEXTDESCRIPTOR,
           TRIM(CMA.SECONDADDRESABLEOBJECT) CUSTSECONDADDRESABLEOBJECT,
           TRIM(CMA.PRIMARYADDRESSABLEOBJECT) CUSTPRIMARYADDRESSABLEOBJECT,
--           TRIM(CMA.ADDRESSLINE01) CUSADDRESSLINE01, -- V0.02
           CMA.ADDRESSLINE01 CUSADDRESSLINE01,
           TRIM(CMA.ADDRESSLINE02) CUSADDRESSLINE02,
           TRIM(CMA.ADDRESSLINE03) CUSADDRESSLINE03,
           TRIM(CMA.ADDRESSLINE04) CUSADDRESSLINE04,
           TRIM(CMA.ADDRESSLINE05) CUSADDRESSLINE05,
           TRIM(CMA.POSTCODE) CUSTPOSTCODE,
           TRIM(CMA.COUNTRY) CUSTCOUNTRY,
           TRIM(CMA.PAFADDRESSKEY) CUSTPAFADDRESSKEY
    FROM MOUTRAN.MO_SUPPLY_POINT MSP,
         MOUTRAN.MO_ELIGIBLE_PREMISES MEP,
         MOUTRAN.MO_PROPERTY_ADDRESS MPA,
         MOUTRAN.MO_CUSTOMER MC,
         MOUTRAN.MO_CUST_ADDRESS MCA,
         MOUTRAN.MO_ADDRESS CMA, -- CUSTOMER ADDRESS
         MOUTRAN.MO_ADDRESS PMA -- PROPERTY ADDRESS
    WHERE MSP.STWPROPERTYNUMBER_PK = MEP.STWPROPERTYNUMBER_PK
    AND MEP.STWPROPERTYNUMBER_PK = MPA.STWPROPERTYNUMBER_PK 
    AND MEP.STWPROPERTYNUMBER_PK = MCA.STWPROPERTYNUMBER_PK 
    AND MC.STWPROPERTYNUMBER_PK = MCA.STWPROPERTYNUMBER_PK
    AND MC.CUSTOMERNUMBER_PK = MSP.CUSTOMERNUMBER_PK
    AND MCA.CUSTOMERNUMBER_PK = MSP.CUSTOMERNUMBER_PK
    AND MPA.ADDRESS_PK = PMA.ADDRESS_PK
    AND MCA.ADDRESS_PK = CMA.ADDRESS_PK
    ORDER BY MSP.SPID_PK;
    
  TYPE tab_supply_point IS TABLE OF cur_supply_point%ROWTYPE INDEX BY PLS_INTEGER;
  t_supply_point  tab_supply_point;

BEGIN
   -- initial variables
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_dropped_cb := 0;
   l_no_row_written := 0;
   l_no_row_war := 10;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_job.IND_STATUS := 'RUN';

   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;


   -- get job no and start job
   P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name,
                         l_job.NO_INSTANCE,
                         l_job.ERR_TOLERANCE,
                         l_job.EXP_TOLERANCE,
                         l_job.WAR_TOLERANCE,
                         l_job.NO_COMMIT,
                         l_job.NO_STREAM,
                         l_job.NO_RANGE_MIN,
                         l_job.NO_RANGE_MAX,
                         l_job.IND_STATUS);

   COMMIT;

   l_progress := 'processing ';
--   l_filename := 'SUPPLY_POINT_SEVERN-W_' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || '.dat';

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- start processing all records for range supplied
--  fHandle := UTL_FILE.FOPEN('DELEXPORT', l_filename, 'w');

  OPEN cur_supply_point;

  l_progress := 'loop processing ';

  LOOP
    FETCH cur_supply_point BULK COLLECT INTO t_supply_point LIMIT l_job.NO_COMMIT;

    FOR i IN 1..t_supply_point.COUNT
    LOOP
      l_no_row_read := l_no_row_read + 1;
      l_err.TXT_KEY := t_supply_point(i).SPID_PK;
        l_rec_written := TRUE;
        BEGIN
          -- write the data to the delivery table
          l_progress := 'insert row into DEL_SUPPLY_POINT ';
          INSERT INTO DEL_SUPPLY_POINT VALUES t_supply_point(i);
          COMMIT;
        EXCEPTION
        WHEN OTHERS THEN
             l_no_row_dropped := l_no_row_dropped + 1;
             l_rec_written := FALSE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;

             IF instr(l_error_message,'Postcode') > 0 THEN
               l_error_message := 'Invalid Postcode Error';
             END IF;

             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
             l_no_row_exp := l_no_row_exp + 1;

             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)
             THEN
                 CLOSE cur_supply_point;
                 l_job.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
              END IF;
        END;

        -- keep count of records written
        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        END IF;

    END LOOP;
-- NOW WRITE THE FILE
   FOR w IN cb_cur
   LOOP
    CASE w.WHOLESALER_ID 
      WHEN 'ANGLIAN-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_ANW_V';
      WHEN 'DWRCYMRU-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_WEL_V';
      WHEN 'SEVERN-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_STW_V';
      WHEN 'SEVERN-A' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_STWA_V';
      WHEN 'SEVERN-B' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_STWB_V';
      WHEN 'THAMES-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_THW_V';
      WHEN 'WESSEX-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_WEW_V';
      WHEN 'YORKSHIRE-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_YOW_V';
      WHEN 'UNITED-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_UUW_V';
      WHEN 'SOUTHSTAFF-W' THEN
        l_sql := 'SELECT * FROM DEL_SUPPLY_POINT_SSW_V';
    END CASE;
    IF w.RUN_FLAG = 1 THEN
      l_filename := 'SUPPLY_POINT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
      P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      l_no_row_written := l_no_row_written + l_rows_written; -- add rows written to total

      IF w.WHOLESALER_ID NOT LIKE 'SEVERN%' THEN
        l_filename := 'OWC_SUPPLY_POINT_' || w.WHOLESALER_ID || '_' || TO_CHAR(SYSDATE,'YYMMDDHH24MI') || '.dat';
        l_sql := 'SELECT  STW.*,MSP.SPID_PK SPID_S 
                  FROM DEL_SUPPLY_POINT_STW_V STW, MOUTRAN.MO_SUPPLY_POINT MSP 
                  WHERE STW.OTHERWHOLESALERID = ''' || w.WHOLESALER_ID || ''' 
                  AND SUBSTR(STW.SPID_PK,0,10) = MSP.CORESPID_PK 
                  AND MSP.SERVICECATEGORY = ''S''';
        P_DEL_UTIL_WRITE_FILE(l_sql,l_filepath,l_filename,l_rows_written);
      END IF;
    ELSE
      l_sql := 'SELECT COUNT(*) FROM DEL_SUPPLY_POINT WHERE WHOLESALERID = :wholesaler';
      EXECUTE IMMEDIATE l_sql INTO l_count USING w.WHOLESALER_ID;
      l_no_row_dropped_cb := l_no_row_dropped_cb + l_count;
    END IF;
  END LOOP;
  
    IF t_supply_point.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE cur_supply_point;

  -- archive the latest batch
  P_DEL_UTIL_ARCHIVE_TABLE(p_tablename => l_tablename,
                           p_batch_no => no_batch,
                           p_filename => l_filename);

  -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP36', 2000, l_no_row_read,    'Distinct Supply Points read in from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP36', 2010, l_no_row_dropped, 'Supply Points dropped during extract from Transform tables');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP36', 2015, l_no_row_dropped_cb, 'Cross Border Supply Points not written to any file');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP36', 2020, l_no_row_insert,  'Supply Points written to ' || l_tablename || ' from extract');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP36', 2030, l_no_row_written, 'Supply Points written to file(s) from extract');

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';

  COMMIT;

EXCEPTION
  WHEN OTHERS THEN
     l_error_number := SQLCODE;
     l_error_message := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY,  substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     return_code := -1;
END P_MOU_DEL_SUPPLY_POINT;
/
exit;