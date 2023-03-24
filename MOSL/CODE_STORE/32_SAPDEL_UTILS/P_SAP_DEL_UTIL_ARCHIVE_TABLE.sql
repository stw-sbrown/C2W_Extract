create or replace
PROCEDURE P_SAP_DEL_UTIL_ARCHIVE_TABLE (p_tablename IN VARCHAR2,
                                                      p_batch_no IN NUMBER, 
                                                      p_filename IN VARCHAR2) AS
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Archive Table Utility
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_SAP_DEL_UTIL_ARCHIVE.sql
--
-- Subversion $Revision: 4031 $
--
-- CREATED        : 23/05/2016
--
-- DESCRIPTION    : Utility proc to archive delivery tables

-- NOTES  :
-- This package must be run each time the delivery batch is run.
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      23/05/2016  K.Burton   Initial Draft
-----------------------------------------------------------------------------------------   
                                                   
    l_error_number  VARCHAR2(255);
    l_error_message VARCHAR2(512);  
    l_sql           VARCHAR2(1000);
    l_archive_table VARCHAR2(100);
BEGIN
  l_archive_table := p_tablename || '_ARC';
  BEGIN
    l_sql := 'DROP TABLE ' || l_archive_table;
  
    EXECUTE IMMEDIATE l_sql;
      EXCEPTION
        WHEN OTHERS THEN
         l_error_number := SQLCODE;
         l_error_message := SQLERRM;
         dbms_output.put_line('ArchiveDeliveryTable error: ' ||  l_error_message);
  END;
    
  BEGIN    
    l_sql := 'CREATE TABLE ' || l_archive_table || ' AS ';
    l_sql := l_sql || 'SELECT T.*, MB.NO_BATCH, ''' || p_filename || ''' FILENAME ';
    l_sql := l_sql || 'FROM ' || p_tablename || ' T, SAPDEL.MIG_BATCHSTATUS MB ';
    l_sql := l_sql || 'WHERE MB.NO_BATCH = ' || p_batch_no;
  
    EXECUTE IMMEDIATE l_sql;
  EXCEPTION
      WHEN OTHERS THEN
         l_error_number := SQLCODE;
         l_error_message := SQLERRM;
        dbms_output.put_line('ArchiveDeliveryTable error: ' ||  l_error_message);
  END;
END P_SAP_DEL_UTIL_ARCHIVE_TABLE;
/
exit;