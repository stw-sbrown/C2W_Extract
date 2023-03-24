create or replace
PROCEDURE P_MOU_TRAN_CUSTOMER(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                                no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                                return_code       IN OUT NUMBER )
IS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Property Transform MO Extract
--
-- AUTHOR         : Ola Badmus
--
-- FILENAME       : P_MOU_TRAN_CUSTOMER.sql
--
-- Subversion $Revision: 4023 $
--
-- CREATED        : 01/03/2016
--
-- DESCRIPTION    : Procedure to create the Customer MO Extract
--                 Will read from key gen and target tables, apply any transformationn
--                 rules and write to normalised tables.
-- NOTES  :
-- This package must be run each time the transform batch is run.
---------------------------- Modification History --------------------------------------
--
-- Version     Date                Author         Description
-- ---------   ---------------     -------             ----------------------------------
-- V 0.01      01/03/2016          O.Badmus        Initial Draft
-- V 0.02      09/03/2016          O.Badmus        Updated variable names, SQL query to pick up appropriate T/A names : I-002
-- V 0.03      10/03/2016          O.Badmus        Writing data into TEMP_2_MO_CUSTOMER to analayse instances of 2 or more LEs
--                                                 attached to a property and vice versal
--V 0.04       11/03/2016          O.Badmus        Amended query to filter out properties with 3 or more distinct LEs :  I-003
--v 1.00       14/03/2016          M.Marron        Published Code (noting that I-003 resolution is under investigation.). Renamed
--                                                 Temp_TVP054 to BT_TVP054 as defined in the Updated Control document
--v 1.01       25/04/2016          O.Badmus        Issue no I-114 SIC CODE classfication code type CHECK and when Invalid change to Null and write out a warning
--v 1.02       27/04/2016          O.Badmus        Issue no I-196 linking cursor to MO_SUPPLY_POINT to ensure only 1 customer per suppy point
-----------------------------------------------------------------------------------------
--prerequsite
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_CUSTOMER';  -- modify
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);
  L_PROGRESS                    VARCHAR2(100);
  l_prev_prp                    BT_TVP054.NO_LEGAL_ENTITY%TYPE; --modify
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  L_ERR                         MIG_ERRORLOG%ROWTYPE;
  L_MO                          MO_CUSTOMER%ROWTYPE; --modify
  L_CTY                         MO_CUSTOMER%ROWTYPE;
  --L_SITE                        LU_CONSTUCTION_SITE%ROWTYPE; --look up table
  l_hlt                         LU_PUBHEALTHRESITE%ROWTYPE; --look up table
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  L_NO_ROW_INSERT               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;

cursor CUR_CUST (P_NO_LEGAL_ENTITY_START   BT_TVP054.NO_LEGAL_ENTITY%type,
                 P_NO_LEGAL_ENTITY_END     BT_TVP054.NO_LEGAL_ENTITY%type)
    IS
     SELECT /*+ PARALLEL(T054,12) PARALLEL(T2,12)  PARALLEL(T3,12) PARALLEL(T4,12) */
     DISTINCT t054.NO_LEGAL_ENTITY,
            T054.NO_PROPERTY,
            T2.ID_TAX,
            T2.NM_PREFERRED,
            CASE WHEN T2.IND_LEGAL_ENTITY = 'B'
                  THEN T3.NM_LEGAL_ENTITY
                  ELSE ''
            END NM_LEGAL_ENTITY,
            T2.CD_SIC
     FROM   BT_TVP054 T054,
            CIS.TVP036LEGALENTITY T2,
            CIS.TVP064LENAME T3,
            MO_SUPPLY_POINT T4
    WHERE   T054.NO_LEGAL_ENTITY BETWEEN p_no_legal_entity_start AND  p_no_legal_entity_end
      AND    T054.CD_COMPANY_SYSTEM = T2.CD_COMPANY_SYSTEM
      AND    T4.CUSTOMERNUMBER_PK = T054.NO_LEGAL_ENTITY(+)
      AND    T4.STWPROPERTYNUMBER_PK = T054.NO_PROPERTY (+)
      AND    T054.NO_LEGAL_ENTITY = T2.NO_LEGAL_ENTITY
      AND    T3.NO_LEGAL_ENTITY(+) = T2.NO_LEGAL_ENTITY
--    AND    T2.IND_LEGAL_ENTITY ='B'
      AND    T3.NO_SEQ(+)  = 2
      AND    T054.TP_CUST_ACCT_ROLE = 'P'
     order by t054.NO_LEGAL_ENTITY, t054.NO_PROPERTY desc;

TYPE tab_customer IS TABLE OF cur_cust%ROWTYPE INDEX BY PLS_INTEGER;
t_cust tab_customer;

BEGIN

   l_PROGRESS := 'Start';
   l_ERR.TXT_DATA := C_MODULE_NAME;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   L_NO_ROW_INSERT := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_prev_prp := 0;
   l_job.IND_STATUS := 'RUN';

   -- get job no

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

   l_progress := 'processing ';

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- process all records for range supplied
  OPEN cur_cust (l_job.NO_RANGE_MIN, l_job.NO_RANGE_MAX); -- modify

  l_progress := 'loop processing ';

  LOOP

    FETCH cur_cust BULK COLLECT INTO t_cust LIMIT l_job.NO_COMMIT;

    l_no_row_read := l_no_row_read + t_cust.COUNT;

    FOR i IN 1..t_cust.COUNT
    LOOP

      L_ERR.TXT_KEY := t_cust(I).NO_LEGAL_ENTITY; -- modify
      l_mo := NULL;

         IF t_cust(I).CD_SIC =1
          THEN L_MO.STDINDUSTRYCLASSCODE := '00000';
         ELSIF
            t_cust(I).CD_SIC = 19999
          THEN L_MO.STDINDUSTRYCLASSCODE := NULL;
         ELSif
            t_cust(I).CD_SIC IN (99 ,10099, 20099, 30000, 30099, 40099
            , 50099, 60099, 70099, 80099, 90099)
           THEN L_MO.STDINDUSTRYCLASSCODE := 'INVALID';
         ELSE L_MO.STDINDUSTRYCLASSCODE := t_cust(I).CD_SIC;
         END IF;
        -- dbms_output.put_line('test1');
l_progress := 'loop processing SELECTING FROM SENSITIVE';

         BEGIN
            SELECT SENSITIVE
            INTO   l_hlt.SENSITIVE
            FROM   LU_PUBHEALTHRESITE
            WHERE  STWPROPERTYNUMBER_PK = t_cust(i).NO_PROPERTY;
         EXCEPTION
         WHEN NO_DATA_FOUND THEN
              l_hlt.SENSITIVE := NULL;
         END;

         IF
         L_HLT.SENSITIVE IS NOT NULL
           THEN L_HLT.SENSITIVE := 'SEMDV';
         ELSE
             L_HLT.SENSITIVE := 'NA';
         END IF;
         --SIC CODE classfication code type CHECK
         --and when it is Invalid change to Null and write out a warning
         IF 
          L_MO.STDINDUSTRYCLASSCODE IS NULL
          THEN L_CTY.STDINDUSTRYCLASSCODETYPE := NULL;
          ELSIF L_MO.STDINDUSTRYCLASSCODE = 'INVALID' 
          THEN L_MO.STDINDUSTRYCLASSCODE := NULL;
             L_CTY.STDINDUSTRYCLASSCODETYPE := NULL;
             P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'W', SUBSTR('Invalid  Sic Code',1,100),  L_ERR.TXT_KEY, SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100));
          else L_CTY.STDINDUSTRYCLASSCODETYPE := 1980;
          END IF;

        L_REC_WRITTEN := true;
        l_progress := 'loop processing ABOUT TO INSERT INTO TABLE';
        begin
          INSERT INTO MO_CUSTOMER --MODIFY
          (CUSTOMERNUMBER_PK,STWPROPERTYNUMBER_PK,COMPANIESHOUSEREFNUM,CUSTOMERCLASSIFICATION,CUSTOMERNAME
          ,CUSTOMERBANNERNAME,STDINDUSTRYCLASSCODE,STDINDUSTRYCLASSCODETYPE,SERVICECATEGORY)
          VALUES
          (T_CUST(I).NO_LEGAL_ENTITY,T_CUST(I).NO_PROPERTY,T_CUST(I).ID_TAX,L_HLT.SENSITIVE,TRIM(T_CUST(I).NM_PREFERRED)
          ,TRIM(t_cust(i).NM_LEGAL_ENTITY),L_MO.STDINDUSTRYCLASSCODE,L_CTY.STDINDUSTRYCLASSCODETYPE,NULL);
        EXCEPTION
        WHEN OTHERS THEN
             L_NO_ROW_DROPPED := L_NO_ROW_DROPPED + 1;
             l_rec_written := TRUE;
             l_error_number := SQLCODE;
             l_error_message := SQLERRM;

             P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_ERR.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,100));
             l_no_row_exp := l_no_row_exp + 1;

             -- if tolearance limit has een exceeded, set error message and exit out
             IF (   l_no_row_exp > l_job.EXP_TOLERANCE
                 OR l_no_row_err > l_job.ERR_TOLERANCE
                 OR l_no_row_war > l_job.WAR_TOLERANCE)
             THEN
                 CLOSE cur_cust;
                 L_JOB.IND_STATUS := 'ERR';
                 P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Error tolerance level exceeded',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,100));
                 P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
                 COMMIT;
                 return_code := -1;
                 RETURN;
              END IF;
        END;

        IF l_rec_written THEN
           l_no_row_insert := l_no_row_insert + 1;
        END IF;

        l_prev_prp := t_cust(i).NO_LEGAL_ENTITY;

     -- END IF;

    END LOOP;

    IF t_cust.COUNT < l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;

  END LOOP;

  CLOSE cur_cust;
    -- write counts
  l_progress := 'Writing Counts';

  --  the recon key numbers used will be specific to each procedure

  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP27', 940, L_NO_ROW_READ,    'Distinct Eligible Customers read during Transform');
  P_MIG_BATCH.FN_RECONLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'CP27', 950, L_NO_ROW_DROPPED, 'Eligible Customers  dropped during Transform');
  P_MIG_BATCH.FN_RECONLOG(no_batch, l_job.NO_INSTANCE, 'CP27', 960, l_no_row_insert,  'Eligible Customers written to MO_CUSTOMER during Transform');

  --  check counts match

  IF l_no_row_read <> l_no_row_insert THEN
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'X', 'Reconciliation counts do not match',  l_no_row_read || ',' || l_no_row_insert, substr(l_ERR.TXT_DATA || ',' || l_progress,100));
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     COMMIT;
     return_code := -1;
  ELSE
     l_job.IND_STATUS := 'END';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
  END IF;

  l_progress := 'End';

  COMMIT;

EXCEPTION
WHEN OTHERS THEN
     L_ERROR_NUMBER := SQLCODE;
     L_ERROR_MESSAGE := SQLERRM;
     P_MIG_BATCH.FN_ERRORLOG(NO_BATCH, L_JOB.NO_INSTANCE, 'E', SUBSTR(L_ERROR_MESSAGE,1,100),  L_ERR.TXT_KEY,  SUBSTR(l_ERR.TXT_DATA || ',' || L_PROGRESS,100));
     P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  l_err.TXT_KEY, substr(l_ERR.TXT_DATA || ',' || l_progress,100));
     l_job.IND_STATUS := 'ERR';
     P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
     RETURN_CODE := -1;
END P_MOU_TRAN_CUSTOMER;
/
exit;
