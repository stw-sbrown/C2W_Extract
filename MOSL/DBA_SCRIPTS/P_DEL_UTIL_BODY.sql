create or replace
PACKAGE BODY P_DEL_UTIL AS

  PROCEDURE ArchiveDeliveryTable(p_tablename IN VARCHAR2, 
                                 p_batch_no IN MIG_BATCHSTATUS.NO_BATCH%TYPE, 
                                 p_filename IN VARCHAR2) AS
    l_error_number  VARCHAR2(255);
    l_error_message VARCHAR2(512);  
    l_sql           VARCHAR2(1000);
    l_archive_table VARCHAR2(100);
    l_err_txt MIG_ERRORLOG.TXT_KEY%TYPE := 'ArchiveTable';    
    BEGIN
      l_archive_table := p_tablename || '_ARC';
      BEGIN
        l_sql := 'DROP TABLE ' || l_archive_table;
        EXECUTE IMMEDIATE l_sql;
      EXCEPTION
        WHEN OTHERS THEN
           l_error_number := SQLCODE;
           l_error_message := SQLERRM;
           P_MIG_BATCH.FN_ERRORLOG(p_batch_no, p_job_instance, 'W', substr(l_error_message,1,100),  l_err_txt,  substr(p_err_data || ',' || 'drop table ' || p_tablename,1,100));
      END;
      
      l_sql := 'CREATE TABLE ' || l_archive_table || ' AS ';
      l_sql := l_sql || 'SELECT T.*, MB.NO_BATCH, ''' || p_filename || ''' FILENAME ';
      l_sql := l_sql || 'FROM ' || p_tablename || ' T, MOUDEL.MIG_BATCHSTATUS MB ';
      l_sql := l_sql || 'WHERE MB.NO_BATCH = ' || p_batch_no;

--      dbms_output.put_line(l_sql);
  
      EXECUTE IMMEDIATE l_sql;
    EXCEPTION
        WHEN OTHERS THEN
           l_error_number := SQLCODE;
           l_error_message := SQLERRM;
          dbms_output.put_line('ArchiveDeliveryTable error: ' ||  l_error_message);
           P_MIG_BATCH.FN_ERRORLOG(p_batch_no, p_job_instance, 'W', substr(l_error_message,1,100),  l_err_txt,  substr(p_err_data || ',' || 'create table ' || p_tablename,1,100));
  END ArchiveDeliveryTable;

  PROCEDURE TruncateDeliveryTable(p_tablename IN VARCHAR2,
                                  p_batch_no IN MIG_BATCHSTATUS.NO_BATCH%TYPE, 
                                  p_job_instance IN MIG_JOBSTATUS.NO_INSTANCE%TYPE,
                                  p_err_data IN MIG_ERRORLOG.TXT_DATA%TYPE,
                                  p_job_status IN OUT MIG_JOBSTATUS.IND_STATUS%TYPE,
                                  p_return_code IN OUT NUMBER) AS
    l_error_number  VARCHAR2(255);
    l_error_message VARCHAR2(512);   
    l_sql VARCHAR2(1000);
    l_err_txt MIG_ERRORLOG.TXT_KEY%TYPE := 'TruncateTable';
  BEGIN
    DisableConstraints;
    
    l_sql := 'TRUNCATE TABLE ' || p_tablename;
--    dbms_output.put_line(l_sql);
    EXECUTE IMMEDIATE l_sql;
    
    EnableConstraints;
  EXCEPTION
    WHEN OTHERS THEN
      l_error_number := SQLCODE;
      l_error_message := SQLERRM;
      dbms_output.put_line('TruncateDeliveryTable error: ' ||  l_error_message);
      P_MIG_BATCH.FN_ERRORLOG(p_batch_no, p_job_instance, 'E', substr(l_error_message,1,100),  l_err_txt,  substr(p_err_data || ',' || 'truncate table ' || p_tablename,1,100));
      P_MIG_BATCH.FN_ERRORLOG(p_batch_no, p_job_instance, 'E', 'Job Ended - Unexpected Error',  l_err_txt,  substr(p_err_data || ',' || 'truncate table ' || p_tablename,1,100));
      p_job_status := 'ERR';
      P_MIG_BATCH.FN_UPDATEJOB(p_batch_no, p_job_instance, p_job_status);
      p_return_code := -1;  
  END TruncateDeliveryTable;
  
  PROCEDURE EnableConstraints AS
  BEGIN
    FOR c IN
    (SELECT c.owner, c.table_name, c.constraint_name
     FROM user_constraints c, user_tables t
     WHERE c.table_name = t.table_name
     AND c.table_name LIKE 'DEL_%'
     AND c.status = 'DISABLED'
     ORDER BY c.constraint_type)
    LOOP
    
      dbms_utility.exec_ddl_statement('alter table "' || c.owner || '"."' || c.table_name || '" enable constraint ' || c.constraint_name);
    END LOOP;  
  END EnableConstraints;
  
  PROCEDURE DisableConstraints AS
  BEGIN
    FOR c IN
    (SELECT c.owner, c.table_name, c.constraint_name
     FROM user_constraints c, user_tables t
     WHERE c.table_name = t.table_name
     AND c.status = 'ENABLED'
     AND c.table_name LIKE 'DEL_%'
     AND NOT (t.iot_type IS NOT NULL AND c.constraint_type = 'P')
     ORDER BY c.constraint_type DESC)
    LOOP
      dbms_utility.exec_ddl_statement('alter table "' || c.owner || '"."' || c.table_name || '" disable constraint ' || c.constraint_name);
    END LOOP;  
  END DisableConstraints;  

END P_DEL_UTIL;
/
exit;