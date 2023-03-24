create or replace
PROCEDURE P_DEL_UTIL_WRITE_FILE(p_query IN VARCHAR2,
                                p_directory IN VARCHAR2,
                                p_filename IN VARCHAR2,
                                p_lines_written OUT VARCHAR2) AS 
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Write File Utility
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_DEL_UTIL_WRITE_FILE.sql
--
-- Subversion $Revision: 5965 $
--
-- CREATED        : 13/05/2016
--
-- DESCRIPTION    : Utility proc to write delimited files from query results
-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      13/05/2016  K.Burton   Initial Draft
-- V 0.02      20/10/2016  K.Burton   Change to append to existing file
-----------------------------------------------------------------------------------------                                
  v_finaltxt  VARCHAR2(4000);
  v_v_val     VARCHAR2(4000);
  v_n_val     NUMBER;
  v_d_val     DATE;
  v_ret       NUMBER;
  c           NUMBER;
  d           NUMBER;
  col_cnt     INTEGER;
  f           BOOLEAN;
  rec_tab     DBMS_SQL.DESC_TAB;
  col_num     NUMBER;
  v_fh        UTL_FILE.FILE_TYPE;
  l_lines_written NUMBER;
  l_static_filename VARCHAR2(100);
BEGIN
  l_static_filename := SUBSTR(p_filename,1,INSTR(p_filename,'_',-1)-1) || '.dat';
  l_lines_written := 0;
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
    ELSE
      DBMS_SQL.DEFINE_COLUMN(c,j,v_v_val,2000);
    END CASE;
  END LOOP;

--    v_fh := UTL_FILE.FOPEN(p_directory, p_filename, 'w');
    v_fh := UTL_FILE.FOPEN(p_directory, l_static_filename, 'a');

  LOOP
    v_ret := DBMS_SQL.FETCH_ROWS(c);
    EXIT WHEN v_ret = 0;
    v_finaltxt := NULL;
    FOR j in 1..col_cnt
    LOOP
      CASE rec_tab(j).col_type
        WHEN 1 THEN DBMS_SQL.COLUMN_VALUE(c,j,v_v_val);
                    v_finaltxt := v_finaltxt || '|' || TRIM(v_v_val);
        WHEN 2 THEN DBMS_SQL.COLUMN_VALUE(c,j,v_n_val);
                    CASE rec_tab(j).col_scale
                     WHEN 1 THEN
                      v_finaltxt := v_finaltxt || '|' || TRIM(TO_CHAR(v_n_val,'99999990.9'));
                     WHEN 2 THEN
                      v_finaltxt := v_finaltxt || '|' || TRIM(TO_CHAR(v_n_val,'99999990.99'));
                     ELSE
                      v_finaltxt := v_finaltxt || '|' || TRIM(v_n_val);
                    END CASE;
        WHEN 12 THEN DBMS_SQL.COLUMN_VALUE(c,j,v_d_val);
                    v_finaltxt := TRIM(v_finaltxt || '|' || TRIM(to_char(v_d_val,'YYYY-MM-DD')));
      ELSE
        v_finaltxt := v_finaltxt || '|' || TRIM(v_v_val);
      END CASE;
    END LOOP;
    BEGIN
      UTL_FILE.PUT_LINE(v_fh, SUBSTR(v_finaltxt,2));  -- strip out leading delimiter
      l_lines_written := l_lines_written + 1;
    EXCEPTION
      WHEN OTHERS THEN
        NULL;
    END;
  END LOOP;
  UTL_FILE.FCLOSE(v_fh);
  DBMS_SQL.CLOSE_CURSOR(c);
  p_lines_written := l_lines_written;

  -- rename file
  UTL_FILE.FRENAME(p_directory, l_static_filename, p_directory, p_filename, TRUE);
END P_DEL_UTIL_WRITE_FILE;
/

show error;

exit;