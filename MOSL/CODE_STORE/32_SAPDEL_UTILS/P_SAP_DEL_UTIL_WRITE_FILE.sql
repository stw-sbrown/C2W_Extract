create or replace
PROCEDURE P_SAP_DEL_UTIL_WRITE_FILE(p_query IN VARCHAR2,
                                p_filehandle IN UTL_FILE.FILE_TYPE,
                                p_delimiter IN VARCHAR2,
                                p_keys_written OUT VARCHAR2,
                                p_lines_written OUT VARCHAR2) AS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Write File Utility
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_UTIL_WRITE_FILE.sql
--
-- Subversion $Revision: 5042 $
--
-- CREATED        : 13/05/2016
--
-- DESCRIPTION    : Utility proc to write delimited SAP files from query results
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      13/05/2016  K.Burton   Initial Draft
-- V 0.02      24/05/2016  K.Burton   Updates to accommodate NULL values within the output data
-----------------------------------------------------------------------------------------                                
  v_finaltxt  VARCHAR2(4000);
  v_v_val     VARCHAR2(4000);
  v_n_val     NUMBER;
  v_c_val     CHAR(20);
  v_d_val     DATE;
  v_ret       NUMBER;
  c           NUMBER;
  d           NUMBER;
  col_cnt     INTEGER;
  f           BOOLEAN;
  rec_tab     DBMS_SQL.DESC_TAB;
  l_num_cols      NUMBER;
  l_lines_written NUMBER;
  l_delimiter VARCHAR2(1);
  l_key_count NUMBER;
  l_prev_key VARCHAR2(30);
BEGIN
--  l_delimiter := p_delimiter;
  l_delimiter := chr(9); -- work-around to set TAB delimiter for all procs in one go
  l_lines_written := 0;
  l_prev_key := 'DUMMY';
  l_key_count := 0;
  c := DBMS_SQL.OPEN_CURSOR;
  DBMS_SQL.PARSE(c, p_query, DBMS_SQL.NATIVE);
  d := DBMS_SQL.EXECUTE(c);
  DBMS_SQL.DESCRIBE_COLUMNS(c, col_cnt, rec_tab);
  
  FOR j in 1..col_cnt
  LOOP
    CASE rec_tab(j).col_type
      WHEN 1 THEN DBMS_SQL.DEFINE_COLUMN(c,j,v_v_val,2000);
      WHEN 2 THEN DBMS_SQL.DEFINE_COLUMN(c,j,v_n_val);
      WHEN 12 THEN DBMS_SQL.DEFINE_COLUMN(c,j,v_d_val);
      WHEN 96 THEN DBMS_SQL.DEFINE_COLUMN(c,j,v_c_val,20);
    ELSE
      DBMS_SQL.DEFINE_COLUMN(c,j,v_v_val,2000);
    END CASE;
  END LOOP;

  LOOP
    v_ret := DBMS_SQL.FETCH_ROWS(c);
    EXIT WHEN v_ret = 0;
    v_finaltxt := NULL;
    FOR j in 1..col_cnt
    LOOP
      IF rec_tab(j).col_name = 'COL_COUNT' THEN
        DBMS_SQL.COLUMN_VALUE(c,j,l_num_cols); -- gets the maximum number of output columns from table for current row
      ELSE
        IF j <= l_num_cols THEN -- if we have not reached the last column for this row add the value to the output string
          DBMS_SQL.COLUMN_VALUE(c,j,v_v_val);
          IF rec_tab(j).col_name = 'SECTION_ID' THEN
            v_finaltxt := v_finaltxt || l_delimiter || TRIM(SUBSTR(v_v_val,3)); -- work around to get sections is sequence
          ELSE
            v_finaltxt := v_finaltxt || l_delimiter || TRIM(v_v_val);
            IF rec_tab(j).col_name = 'KEY_COL' THEN
              IF TRIM(v_v_val) <> l_prev_key THEN
                l_key_count := l_key_count + 1;
                l_prev_key := TRIM(v_v_val);
              END IF;
            END IF;
          END IF;
        END IF;
      END IF;
    END LOOP;
    BEGIN
      UTL_FILE.PUT_LINE(p_filehandle, SUBSTR(v_finaltxt,length(l_delimiter)+1));  -- strip out leading delimiter
      l_lines_written := l_lines_written + 1;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END LOOP;
  DBMS_SQL.CLOSE_CURSOR(c);
  p_keys_written := l_key_count;
  p_lines_written := l_lines_written;
END P_SAP_DEL_UTIL_WRITE_FILE;
/
exit;