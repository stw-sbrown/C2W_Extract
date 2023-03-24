DECLARE
/*------------------------------------------------------------------------------
|| PROCEDURE  : PRECLONE_RECON_TEACCESS
|| DESCRIPTION: Pre data load reconciliation
||              This script runs on the source database prior to migration.
||              The schema must have permissions to access tables in TEACCESS
||              Requires permissions to the FILES directory.
||      Subversion $Revision: 4023 $
|| Author     : Lee Smith
|| Amendments : L.Smith      29/4/2016   Don't use item types.
||                                       
|| Date       : 6 April 2016
||----------------------------------------------------------------------------*/

  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_table_name             VARCHAR2(30);
  l_no_row_read                 NUMBER(10);
  l_no_row_insert               NUMBER(10);
  l_no_row_dropped              NUMBER(10);  
  l_no_row_war                  NUMBER(5);
  l_no_row_err                  NUMBER(5);
  l_no_row_exp                  NUMBER(5);
  l_rec_written                 BOOLEAN;
  l_no_tables_processed         NUMBER(10);
  l_no_files_written            NUMBER(10);
  l_no_rows                     INTEGER;
  l_process_start               VARCHAR2(15);
  l_process_end                 VARCHAR2(15);

  fHandle                       UTL_FILE.FILE_TYPE;
  l_dir                         VARCHAR2(2000):='FILES';
  l_data_string                 VARCHAR2(2000);

  l_recon_measure_no            NUMBER;
  l_recon_measure_tab           VARCHAR2(30);
  l_recon_measure_cp            VARCHAR2(30);
  l_recon_measure_own           VARCHAR2(30);
  l_recon_measure_desc          Varchar2(2000);

-- CREATE A RECONCILIATION MEASURES LOOKUP ARRAY.
-- WHEN ADDING OR REMOVING TABLES FROM THE ARRAY ALSO AMEND CURSOR cur_mig_tables BELOW.
--
-- Array Data


-- TEACCESS tables
l_measure_own23  VARCHAR2(30)  := 'TEACCESS';
l_measure_no23   NUMBER        := 160;
l_measure_tab23  VARCHAR2(30)  := 'METER_DATA';
l_measure_desc23 VARCHAR2(2000):= 'TEACCESS table METER_DATA row counts before loading into the Reception Area';
l_measure_cp23   VARCHAR2(30)  := 'CP7';

l_measure_own24  VARCHAR2(30)  := 'TEACCESS';
l_measure_no24   NUMBER        := 161;
l_measure_tab24  VARCHAR2(30)  := 'NOTES';
l_measure_desc24 VARCHAR2(2000):= 'TEACCESS table NOTES row counts before loading into the Reception Area';
l_measure_cp24   VARCHAR2(30)  := 'CP7';


l_measure_own25  VARCHAR2(30)  := 'TEACCESS';
l_measure_no25   NUMBER        := 162;
l_measure_tab25  VARCHAR2(30)  := 'AUDIT_DATA';
l_measure_desc25 VARCHAR2(2000):= 'TEACCESS table AUDIT_DATA row counts before loading into the Reception Area';
l_measure_cp25   VARCHAR2(30)  := 'CP7';


l_measure_own26  VARCHAR2(30)  := 'TEACCESS';
l_measure_no26   NUMBER        := 163;
l_measure_tab26  VARCHAR2(30)  := 'CUS_DATA';
l_measure_desc26 VARCHAR2(2000):= 'TEACCESS table CUS_DATA row counts before loading into the Reception Area';
l_measure_cp26   VARCHAR2(30)  := 'CP7';


l_measure_own27  VARCHAR2(30)  := 'TEACCESS';
l_measure_no27   NUMBER        := 164;
l_measure_tab27  VARCHAR2(30)  := 'LETTERS';
l_measure_desc27 VARCHAR2(2000):= 'TEACCESS table LETTERS row counts before loading into the Reception Area';
l_measure_cp27   VARCHAR2(30)  := 'CP7';


l_measure_own28  VARCHAR2(30)  := 'TEACCESS';
l_measure_no28   NUMBER        := 165;
l_measure_tab28  VARCHAR2(30)  := 'SITE_DATA';
l_measure_desc28 VARCHAR2(2000):= 'TEACCESS table SITE_DATA row counts before loading into the Reception Area';
l_measure_cp28   VARCHAR2(30)  := 'CP7';

-- COMPASS tables
l_measure_own29  VARCHAR2(30)  := 'CIS';
l_measure_no29   NUMBER        := 200;
l_measure_tab29  VARCHAR2(30)  := 'COMPASS_CONSENTS';
l_measure_desc29 VARCHAR2(2000):= 'COMPASS table COMPASS_CONSENTS row counts before loading into the Reception Area';
l_measure_cp29   VARCHAR2(30)  := 'CP9';

-- RECORD OF THE RECONCILIATION MEASURES ARRAY
TYPE measures_type IS RECORD (measure_own   VARCHAR2(30),
                              measure_no    NUMBER(5),
                              measure_desc  VARCHAR2(2000),
                              measure_cp    VARCHAR2(30)
                             );
-- INDEXED BY TABLE OF THE RECONCILIATION MEASURES
TYPE measures_tab IS TABLE OF measures_type INDEX BY VARCHAR2(30);
t_measures measures_tab;


-- SELECT REQUIRED TABLES FROM THE DICTIONARY
CURSOR cur_mig_tables
    IS
    SELECT owner,
           table_name
      FROM all_tables
     WHERE owner IN ('TEACCESS')
       AND table_name IN ('METER_DATA',
                          'NOTES',
                          'AUDIT_DATA',
                          'CUS_DATA',
                          'LETTERS',
                          'SITE_DATA'
                         )
     ORDER BY owner, table_name;
TYPE tab_mig_tables IS TABLE OF cur_mig_tables%ROWTYPE INDEX BY PLS_INTEGER;
t_mig_tables  tab_mig_tables;


BEGIN
  SELECT TO_CHAR(sysdate,'yyyymmdd_hh24miss') 
    INTO l_process_start
    FROM dual;
  DBMS_OUTPUT.PUT_LINE('preclone_recon_teaccess started at:- ' || l_process_start);
 
   -- initialise variables 
  l_progress := 'Start';
  l_no_tables_processed := 0;
  l_no_files_written    := 0;
  l_no_rows := 0;
  l_no_row_insert := 0;
  l_no_row_dropped := 0;
  l_no_row_war := 0;
  l_no_row_err := 0;
  l_no_row_exp := 0;
  l_prev_table_name := 'NONE';

-- Populate lookup Table.
  l_progress := 'Load measures array ';
-- TEACCESS tables
t_measures(l_measure_tab23).measure_own    := l_measure_own23;
t_measures(l_measure_tab23).measure_no     := l_measure_no23;
t_measures(l_measure_tab23).measure_desc   := l_measure_desc23;
t_measures(l_measure_tab23).measure_cp     := l_measure_cp23;

t_measures(l_measure_tab24).measure_own    := l_measure_own24;
t_measures(l_measure_tab24).measure_no     := l_measure_no24;
t_measures(l_measure_tab24).measure_desc   := l_measure_desc24;
t_measures(l_measure_tab24).measure_cp     := l_measure_cp24;

t_measures(l_measure_tab25).measure_own    := l_measure_own25;
t_measures(l_measure_tab25).measure_no     := l_measure_no25;
t_measures(l_measure_tab25).measure_desc   := l_measure_desc25;
t_measures(l_measure_tab25).measure_cp     := l_measure_cp25;

t_measures(l_measure_tab26).measure_own    := l_measure_own26;
t_measures(l_measure_tab26).measure_no     := l_measure_no26;
t_measures(l_measure_tab26).measure_desc   := l_measure_desc26;
t_measures(l_measure_tab26).measure_cp     := l_measure_cp26;

t_measures(l_measure_tab27).measure_own    := l_measure_own27;
t_measures(l_measure_tab27).measure_no     := l_measure_no27;
t_measures(l_measure_tab27).measure_desc   := l_measure_desc27;
t_measures(l_measure_tab27).measure_cp     := l_measure_cp27;

t_measures(l_measure_tab28).measure_own    := l_measure_own28;
t_measures(l_measure_tab28).measure_no     := l_measure_no28;
t_measures(l_measure_tab28).measure_desc   := l_measure_desc28;
t_measures(l_measure_tab28).measure_cp     := l_measure_cp28;

      

fHandle := UTL_FILE.FOPEN(l_dir, 'preclone_recon_teaccess', 'w');

  OPEN cur_mig_tables;

  l_progress := 'loop processing cloned tables ';

  LOOP
  
    FETCH cur_mig_tables BULK COLLECT INTO t_mig_tables LIMIT 99999999;
 
    FOR i IN 1..t_mig_tables.COUNT
    LOOP
    
      
      IF l_prev_table_name <> t_mig_tables(i).table_name THEN

          -- Dynamically count the number of rows
         EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || t_mig_tables(i).owner || ' .' 
--                           || t_mig_tables(i).table_name || ' WHERE ROWNUM < 101 ' INTO l_no_rows;  -- Limit to 100 rows
                           || t_mig_tables(i).table_name INTO l_no_rows;                              -- All rows


          -- Lookup measure number and control points from the t_measures array
         BEGIN
            l_recon_measure_own  := t_measures(t_mig_tables(i).table_name).measure_own;
            l_recon_measure_tab  := t_mig_tables(i).table_name;
            l_recon_measure_no   := t_measures(t_mig_tables(i).table_name).measure_no;
            l_recon_measure_cp   := t_measures(t_mig_tables(i).table_name).measure_cp;
            l_recon_measure_desc := t_measures(t_mig_tables(i).table_name).measure_desc;
--            DBMS_OUTPUT.PUT_LINE(l_recon_measure_no);
--            DBMS_OUTPUT.PUT_LINE(t_mig_tables(i).owner);
--            DBMS_OUTPUT.PUT_LINE(t_mig_tables(i).table_name);
--            DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_recon_measure_cp));
--            DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_recon_measure_desc));
--            DBMS_OUTPUT.PUT_LINE('Count='||to_char(l_no_rows));

            l_data_string := l_recon_measure_own
                             ||','
                             ||l_recon_measure_tab
                             ||','
                             ||TO_CHAR(l_recon_measure_no)
                             ||','
                             ||l_recon_measure_cp
                             ||','
                             ||l_no_rows
                             ||','
                             ||l_recon_measure_desc;

            UTL_FILE.PUT_LINE(fHandle, l_data_string);

         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
              DBMS_OUTPUT.PUT_LINE(TO_CHAR(t_mig_tables(i).table_name) || ' NOT FOUND');
         END;

          -- keep count of number of tables processed
         l_no_tables_processed := l_no_tables_processed + 1;
         l_prev_table_name := t_mig_tables(i).table_name;
      
      END IF;
      
    END LOOP;

    IF t_mig_tables.COUNT < 999999 THEN
       EXIT;
    ELSE
       NULL;
    END IF;
     
  END LOOP;
  CLOSE cur_mig_tables;
  UTL_FILE.FCLOSE(fHandle);
  l_no_files_written := l_no_files_written + 1;
 
  l_progress := 'End'; 
  DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_no_tables_processed) || ' Tables processed.');
  DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_no_files_written) || ' File(s) written to.');
 
  SELECT TO_CHAR(sysdate,'yyyymmdd_hh24miss') 
    INTO l_process_end
    FROM dual;
  DBMS_OUTPUT.PUT_LINE('preclone_recon_teaccess ended at:- ' || l_process_end);
  
END;

/
show error;
exit;


