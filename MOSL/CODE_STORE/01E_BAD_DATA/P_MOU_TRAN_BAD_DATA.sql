----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Bad Data Reporting
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_MOU_TRAN_BAD_DATA.sql
--
-- Subversion $Revision: 4309 $
--
-- CREATED        : 11/05/2016
--
-- DESCRIPTION    : Procedure to search for bad data characters that can cause corruptions
--                  in the output files e.g. '|'
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      11/05/2016  K.Burton   Initial Draft
-----------------------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE P_MOU_TRAN_BAD_DATA (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                               no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                               return_code IN OUT NUMBER,
                                               p_bad_char VARCHAR2) AS 
  CURSOR bad_char_cur (p_bad_char VARCHAR2) IS
    SELECT DISTINCT p_bad_char SEARCH_TERM,
                    table_name,
                    column_name
    FROM cols,
     TABLE (xmlsequence (dbms_xmlgen.getxmltype ('SELECT ' || column_name || ' FROM ' || table_name ||
                                                ' WHERE UPPER(' || column_name || ') LIKE UPPER(''%' || p_bad_char || '%'')' 
                                                ).extract ('ROWSET/ROW/*') ) ) t
    WHERE cols.TABLE_NAME LIKE 'MO_%'
    ORDER BY TABLE_NAME;
    
  CURSOR bad_data_cur IS
    SELECT ROW_ID,
           SEARCH_TERM,
           TABLE_NAME,
           COLUMN_NAME,
           VALUE
    FROM BT_BAD_DATA
    WHERE STATUS = 'N';
    
  l_sql VARCHAR2(2000);
  
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MOU_TRAN_BAD_DATA';
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
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_rec_written                 BOOLEAN;  

BEGIN
   -- initial variables
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_written := 0;
   l_no_row_war := 10;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_job.IND_STATUS := 'RUN';

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

   -- any errors set return code and exit out
   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;
  
  l_progress := 'Creating BT_BAD_DATA table'; 
  -- drop the table - if it doesn't exist this will error so trap the error and carry on
  BEGIN
    EXECUTE IMMEDIATE 'DROP TABLE BT_BAD_DATA';
  EXCEPTION 
    WHEN OTHERS THEN
      NULL;
  END;
  
  l_sql := 'CREATE TABLE BT_BAD_DATA (ROW_ID ROWID, SEARCH_TERM VARCHAR2(30), TABLE_NAME VARCHAR2(30), COLUMN_NAME VARCHAR2(30), VALUE VARCHAR2(2000), STATUS VARCHAR2(1))';
  -- create the table - quit if some error occurs
  BEGIN
    EXECUTE IMMEDIATE l_sql;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SQLERRM;
      
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_no_row_exp := l_no_row_exp + 1;
    
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
  END;
  
  FOR b IN bad_char_cur(p_bad_char)
  LOOP
    -- insert a record showing the table, column and value containing the bad data
    l_sql := NULL;
    l_sql := 'INSERT INTO BT_BAD_DATA(ROW_ID,SEARCH_TERM,TABLE_NAME,COLUMN_NAME,VALUE,STATUS) ';
    
    l_sql := l_sql || 'SELECT ROWID ROW_ID,''' || b.SEARCH_TERM || ''',''' || b.TABLE_NAME || ''' TABLE_NAME, ' 
           || '''' || b.COLUMN_NAME || ''' COLUMN_NAME, '
           || b.COLUMN_NAME || ' VALUE,''N'''
           || ' FROM ' || b.TABLE_NAME 
           || ' WHERE ' || b.COLUMN_NAME || ' LIKE ''%' || b.SEARCH_TERM || '%''';
  
    BEGIN
      EXECUTE IMMEDIATE l_sql;  
    EXCEPTION
      WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SQLERRM;
      
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_no_row_exp := l_no_row_exp + 1;
    
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
    END;
  END LOOP;
  
  FOR r IN bad_data_cur 
  LOOP
    -- update the bad data records in TRANSFORM tables so records can be processed by delivery
    l_sql := 'UPDATE ' || r.TABLE_NAME || ' SET ' || r.COLUMN_NAME || ' = REPLACE(' || r.COLUMN_NAME || ',''' || r.SEARCH_TERM || ''','''') '
            || 'WHERE ROWID = ''' || r.ROW_ID || '''';
    BEGIN
      EXECUTE IMMEDIATE l_sql;  
    EXCEPTION
      WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SQLERRM;
      
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_no_row_exp := l_no_row_exp + 1;
    
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
    END;
    
    -- update the status of the record in bad data table to show it has been processed
    l_sql := 'UPDATE BT_BAD_DATA SET STATUS = ''P'' WHERE ROW_ID = ''' || r.ROW_ID || '''';
    BEGIN
      EXECUTE IMMEDIATE l_sql;  
    EXCEPTION
      WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SQLERRM;
      
      P_MIG_BATCH.FN_ERRORLOG(no_batch, l_job.NO_INSTANCE, 'E', substr(l_error_message,1,100),  l_err.TXT_KEY, substr(l_err.TXT_DATA || ',' || l_progress,1,100));
      l_no_row_exp := l_no_row_exp + 1;
    
      l_job.IND_STATUS := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);
      return_code := -1;
      RETURN;
    END;
  END LOOP;

  l_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS);

  l_progress := 'End';
  
END P_MOU_TRAN_BAD_DATA;
/
exit;