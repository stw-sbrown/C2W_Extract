DECLARE
/*------------------------------------------------------------------------------
|| PROCEDURE  : PRECLONE_RECON_TARGET
|| DESCRIPTION: Pre data load reconciliation
||              This script runs on the source database prior to migration.
||              The schema must have permissions to access tables in TARGET.
||              Requires permissions to the FILES directory.
|| OUTPUT     : Single flat file written to the FILES directory.
|| Subversion $Revision: 4023 $
|| Author     : Lee Smith
|| Amendments : L.Smith      29/4/2016   Tables exist in schema CIS
||                                       Don't use item types.
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

-- CIS tables
l_measure_own1  VARCHAR2(30)  := 'CIS';
l_measure_no1   NUMBER        := 110;
l_measure_tab1  VARCHAR2(30)  := 'TADADDRESS';
l_measure_desc1 VARCHAR2(2000):= 'Targets table TADADDRESS row counts before loading into the Reception Area';
l_measure_cp1   VARCHAR2(30)  := 'CP1';

l_measure_own2  VARCHAR2(30)  := 'CIS';
l_measure_no2   NUMBER        := 111;
l_measure_tab2  VARCHAR2(30)  := 'TADADDRFAST';
l_measure_desc2 VARCHAR2(2000):= 'Targets table TADADDRFAST row counts before loading into the Reception Area';
l_measure_cp2   VARCHAR2(30)  := 'CP1';

l_measure_own3  VARCHAR2(30)  := 'CIS';
l_measure_no3   NUMBER        := 112;
l_measure_tab3  VARCHAR2(30)  := 'TVP009APPLIEDCONFG';
l_measure_desc3 VARCHAR2(2000):= 'Targets table TVP009APPLIEDCONFG row counts before loading into the Reception Area';
l_measure_cp3   VARCHAR2(30)  := 'CP1';

l_measure_own4  VARCHAR2(30)  := 'CIS';
l_measure_no4   NUMBER        := 113;
l_measure_tab4  VARCHAR2(30)  := 'TVP036LEGALENTITY';
l_measure_desc4 VARCHAR2(2000):= 'Targets table TVP036LEGALENTITY row counts before loading into the Reception Area';
l_measure_cp4   VARCHAR2(30)  := 'CP1';

l_measure_own5  VARCHAR2(30)  := 'CIS';
l_measure_no5   NUMBER        := 114;
l_measure_tab5  VARCHAR2(30)  := 'TVP046PROPERTY';
l_measure_desc5 VARCHAR2(2000):= 'Targets table TVP046PROPERTY row counts before loading into the Reception Area';
l_measure_cp5   VARCHAR2(30)  := 'CP1';

l_measure_own6  VARCHAR2(30)  := 'CIS';
l_measure_no6   NUMBER        := 115;
l_measure_tab6  VARCHAR2(30)  := 'TVP052REGCONFIG';
l_measure_desc6 VARCHAR2(2000):= 'Targets table TVP052REGCONFIG row counts before loading into the Reception Area';
l_measure_cp6   VARCHAR2(30)  := 'CP1';

l_measure_own7  VARCHAR2(30)  := 'CIS';
l_measure_no7   NUMBER        := 116;
l_measure_tab7  VARCHAR2(30)  := 'TVP053REGSPEC';
l_measure_desc7 VARCHAR2(2000):= 'Targets table TVP053REGSPEC row counts before loading into the Reception Area';
l_measure_cp7   VARCHAR2(30)  := 'CP1';

l_measure_own8  VARCHAR2(30)  := 'CIS';
l_measure_no8   NUMBER        := 117;
l_measure_tab8  VARCHAR2(30)  := 'TVP054SERVPROVRESP';
l_measure_desc8 VARCHAR2(2000):= 'Targets table TVP054SERVPROVRESP row counts before loading into the Reception Area';
l_measure_cp8   VARCHAR2(30)  := 'CP1';

l_measure_own9  VARCHAR2(30)  := 'CIS';
l_measure_no9   NUMBER        := 118;
l_measure_tab9  VARCHAR2(30)  := 'TVP056SERVPROV';
l_measure_desc9 VARCHAR2(2000):= 'Targets table TVP056SERVPROV row counts before loading into the Reception Area';
l_measure_cp9   VARCHAR2(30)  := 'CP1';

l_measure_own10  VARCHAR2(30)  := 'CIS';
l_measure_no10   NUMBER        := 119;
l_measure_tab10  VARCHAR2(30)  := 'TVP057TARIFF';
l_measure_desc10 VARCHAR2(2000):= 'Targets table TVP057TARIFF row counts before loading into the Reception Area';
l_measure_CP10   VARCHAR2(30)  := 'CP1';

l_measure_own11  VARCHAR2(30)  := 'CIS';
l_measure_no11   NUMBER        := 120;
l_measure_tab11  VARCHAR2(30)  := 'TVP058TARIFFASSGN';
l_measure_desc11 VARCHAR2(2000):= 'Targets table TVP058TARIFFASSGN row counts before loading into the Reception Area';
l_measure_cp11   VARCHAR2(30)  := 'CP1';

l_measure_own12  VARCHAR2(30)  := 'CIS';
l_measure_no12   NUMBER        := 121;
l_measure_tab12  VARCHAR2(30)  := 'TVP063EQUIPMENT';
l_measure_desc12 VARCHAR2(2000):= 'Targets table TVP063EQUIPMENT row counts before loading into the Reception Area';
l_measure_cp12   VARCHAR2(30)  := 'CP1';

l_measure_own13  VARCHAR2(30)  := 'CIS';
l_measure_no13   NUMBER        := 122;
l_measure_tab13  VARCHAR2(30)  := 'TVP064LENAME';
l_measure_desc13 VARCHAR2(2000):= 'Targets table TVP064LENAME row counts before loading into the Reception Area';
l_measure_cp13   VARCHAR2(30)  := 'CP1';

l_measure_own14  VARCHAR2(30)  := 'CIS';
l_measure_no14   NUMBER        := 123;
l_measure_tab14  VARCHAR2(30)  := 'TVP097PROPSCALERT';
l_measure_desc14 VARCHAR2(2000):= 'Targets table TVP097PROPSCALERT row counts before loading into the Reception Area';
l_measure_cp14   VARCHAR2(30)  := 'CP1';

l_measure_own15  VARCHAR2(30)  := 'CIS';
l_measure_no15   NUMBER        := 124;
l_measure_tab15  VARCHAR2(30)  := 'TVP163EQUIPINST';
l_measure_desc15 VARCHAR2(2000):= 'Targets table TVP163EQUIPINST row counts before loading into the Reception Area';
l_measure_cp15   VARCHAR2(30)  := 'CP1';

l_measure_own16  VARCHAR2(30)  := 'CIS';
l_measure_no16   NUMBER        := 125;
l_measure_tab16  VARCHAR2(30)  := 'TVP195READING';
l_measure_desc16 VARCHAR2(2000):= 'Targets table TVP195READING row counts before loading into the Reception Area';
l_measure_cp16   VARCHAR2(30)  := 'CP1';

l_measure_own17  VARCHAR2(30)  := 'CIS';
l_measure_no17   NUMBER        := 126;
l_measure_tab17  VARCHAR2(30)  := 'TVP225WATERMTR';
l_measure_desc17 VARCHAR2(2000):= 'Targets table TVP225WATERMTR row counts before loading into the Reception Area';
l_measure_cp17   VARCHAR2(30)  := 'CP1';

l_measure_own18  VARCHAR2(30)  := 'CIS';
l_measure_no18   NUMBER        := 127;
l_measure_tab18  VARCHAR2(30)  := 'TVP249BILLINGCYCLE';
l_measure_desc18 VARCHAR2(2000):= 'Targets table TVP249BILLINGCYCLE row counts before loading into the Reception Area';
l_measure_cp18   VARCHAR2(30)  := 'CP1';

l_measure_own19  VARCHAR2(30)  := 'CIS';
l_measure_no19   NUMBER        := 128;
l_measure_tab19  VARCHAR2(30)  := 'TVP310SERPROVWATER';
l_measure_desc19 VARCHAR2(2000):= 'Targets table TVP310SERPROVWATER row counts before loading into the Reception Area';
l_measure_cp19   VARCHAR2(30)  := 'CP1';

l_measure_own20  VARCHAR2(30)  := 'CIS';
l_measure_no20   NUMBER        := 129;
l_measure_tab20  VARCHAR2(30)  := 'TVP703EXTERNREFDET';
l_measure_desc20 VARCHAR2(2000):= 'Targets table TVP703EXTERNREFDET row counts before loading into the Reception Area';
l_measure_cp20   VARCHAR2(30)  := 'CP1';

l_measure_own21  VARCHAR2(30)  := 'CIS';
l_measure_no21   NUMBER        := 130;
l_measure_tab21  VARCHAR2(30)  := 'TVP771SPRBLALGITEM';
l_measure_desc21 VARCHAR2(2000):= 'Targets table TVP771SPRBLALGITEM row counts before loading into the Reception Area';
l_measure_cp21   VARCHAR2(30)  := 'CP1';


-- RECEPTION TABLES
l_measure_own22  VARCHAR2(30)  := 'RECEPTION';
l_measure_no22   NUMBER        := 180;
l_measure_tab22  VARCHAR2(30)  := 'ELIGIBILITY_CONTROL_TABLE';
l_measure_desc22 VARCHAR2(2000):= 'Targets table ELIGIBILITY_CONTROL_TABLE row counts before loading into the Reception Area';
l_measure_cp22   VARCHAR2(30)  := 'CP4';

-- COMPASS tables
--l_measure_own29  VARCHAR2(30)  := 'CIS';
--l_measure_no29   NUMBER        := 200;
--l_measure_tab29  VARCHAR2(30)  := 'COMPASS_CONSENTS';
--l_measure_desc29 VARCHAR2(2000):= 'COMPASS table COMPASS_CONSENTS row counts before loading into the Reception Area';
--l_measure_cp29   VARCHAR2(30)  := 'CP9';

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
     WHERE owner IN ('CIS','RECEPTION') 
       AND table_name IN ('TADADDRESS',
                          'TADADDRFAST',
                          'TVP009APPLIEDCONFG',
                          'TVP036LEGALENTITY',
                          'TVP046PROPERTY',
                          'TVP052REGCONFIG',
                          'TVP053REGSPEC',
                          'TVP054SERVPROVRESP',
                          'TVP056SERVPROV',
                          'TVP057TARIFF',
                          'TVP058TARIFFASSGN',
                          'TVP063EQUIPMENT',
                          'TVP064LENAME',
                          'TVP097PROPSCALERT',
                          'TVP163EQUIPINST',
                          'TVP195READING',
                          'TVP225WATERMTR',
                          'TVP249BILLINGCYCLE',
                          'TVP310SERPROVWATER',
                          'TVP703EXTERNREFDET',
                          'TVP771SPRBLALGITEM',
                          'ELIGIBILITY_CONTROL_TABLE'
--                          'COMPASS_CONSENTS'
                         )
     ORDER BY owner, table_name;
TYPE tab_mig_tables IS TABLE OF cur_mig_tables%ROWTYPE INDEX BY PLS_INTEGER;
t_mig_tables  tab_mig_tables;


BEGIN
  SELECT TO_CHAR(sysdate,'yyyymmdd_hh24miss') 
    INTO l_process_start
    FROM dual;
  DBMS_OUTPUT.PUT_LINE('preclone_recon_target started at:- ' || l_process_start);
 
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
-- CIS tables
t_measures(l_measure_tab1).measure_own    := l_measure_own1;
t_measures(l_measure_tab1).measure_no     := l_measure_no1;
t_measures(l_measure_tab1).measure_desc   := l_measure_desc1;
t_measures(l_measure_tab1).measure_cp     := l_measure_cp1;

t_measures(l_measure_tab2).measure_own    := l_measure_own2;
t_measures(l_measure_tab2).measure_no     := l_measure_no2;
t_measures(l_measure_tab2).measure_desc   := l_measure_desc2;
t_measures(l_measure_tab2).measure_cp     := l_measure_cp2;

t_measures(l_measure_tab3).measure_own    := l_measure_own3;
t_measures(l_measure_tab3).measure_no     := l_measure_no3;
t_measures(l_measure_tab3).measure_desc   := l_measure_desc3;
t_measures(l_measure_tab3).measure_cp     := l_measure_cp3;

t_measures(l_measure_tab4).measure_own    := l_measure_own4;
t_measures(l_measure_tab4).measure_no     := l_measure_no4;
t_measures(l_measure_tab4).measure_desc   := l_measure_desc4;
t_measures(l_measure_tab4).measure_cp     := l_measure_cp4;

t_measures(l_measure_tab5).measure_own    := l_measure_own5;
t_measures(l_measure_tab5).measure_no     := l_measure_no5;
t_measures(l_measure_tab5).measure_desc   := l_measure_desc5;
t_measures(l_measure_tab5).measure_cp     := l_measure_cp5;

t_measures(l_measure_tab6).measure_own    := l_measure_own6;
t_measures(l_measure_tab6).measure_no     := l_measure_no6;
t_measures(l_measure_tab6).measure_desc   := l_measure_desc6;
t_measures(l_measure_tab6).measure_cp     := l_measure_cp6;

t_measures(l_measure_tab7).measure_own    := l_measure_own7;
t_measures(l_measure_tab7).measure_no     := l_measure_no7;
t_measures(l_measure_tab7).measure_desc   := l_measure_desc7;
t_measures(l_measure_tab7).measure_cp     := l_measure_cp7;

t_measures(l_measure_tab8).measure_own    := l_measure_own8;
t_measures(l_measure_tab8).measure_no     := l_measure_no8;
t_measures(l_measure_tab8).measure_desc   := l_measure_desc8;
t_measures(l_measure_tab8).measure_cp     := l_measure_cp8;

t_measures(l_measure_tab9).measure_own    := l_measure_own9;
t_measures(l_measure_tab9).measure_no     := l_measure_no9;
t_measures(l_measure_tab9).measure_desc   := l_measure_desc9;
t_measures(l_measure_tab9).measure_cp     := l_measure_cp9;

t_measures(l_measure_tab10).measure_own    := l_measure_own10;
t_measures(l_measure_tab10).measure_no     := l_measure_no10;
t_measures(l_measure_tab10).measure_desc   := l_measure_desc10;
t_measures(l_measure_tab10).measure_cp     := l_measure_cp10;

t_measures(l_measure_tab11).measure_own    := l_measure_own11;
t_measures(l_measure_tab11).measure_no     := l_measure_no11;
t_measures(l_measure_tab11).measure_desc   := l_measure_desc11;
t_measures(l_measure_tab11).measure_cp     := l_measure_cp11;

t_measures(l_measure_tab12).measure_own    := l_measure_own12;
t_measures(l_measure_tab12).measure_no     := l_measure_no12;
t_measures(l_measure_tab12).measure_desc   := l_measure_desc12;
t_measures(l_measure_tab12).measure_cp     := l_measure_cp12;

t_measures(l_measure_tab13).measure_own    := l_measure_own13;
t_measures(l_measure_tab13).measure_no     := l_measure_no13;
t_measures(l_measure_tab13).measure_desc   := l_measure_desc13;
t_measures(l_measure_tab13).measure_cp     := l_measure_cp13;

t_measures(l_measure_tab14).measure_own    := l_measure_own14;
t_measures(l_measure_tab14).measure_no     := l_measure_no14;
t_measures(l_measure_tab14).measure_desc   := l_measure_desc14;
t_measures(l_measure_tab14).measure_cp     := l_measure_cp14;

t_measures(l_measure_tab15).measure_own    := l_measure_own15;
t_measures(l_measure_tab15).measure_no     := l_measure_no15;
t_measures(l_measure_tab15).measure_desc   := l_measure_desc15;
t_measures(l_measure_tab15).measure_cp     := l_measure_cp15;

t_measures(l_measure_tab16).measure_own    := l_measure_own16;
t_measures(l_measure_tab16).measure_no     := l_measure_no16;
t_measures(l_measure_tab16).measure_desc   := l_measure_desc16;
t_measures(l_measure_tab16).measure_cp     := l_measure_cp16;

t_measures(l_measure_tab17).measure_own    := l_measure_own17;
t_measures(l_measure_tab17).measure_no     := l_measure_no17;
t_measures(l_measure_tab17).measure_desc   := l_measure_desc17;
t_measures(l_measure_tab17).measure_cp     := l_measure_cp17;

t_measures(l_measure_tab18).measure_own    := l_measure_own18;
t_measures(l_measure_tab18).measure_no     := l_measure_no18;
t_measures(l_measure_tab18).measure_desc   := l_measure_desc18;
t_measures(l_measure_tab18).measure_cp     := l_measure_cp18;

t_measures(l_measure_tab19).measure_own    := l_measure_own19;
t_measures(l_measure_tab19).measure_no     := l_measure_no19;
t_measures(l_measure_tab19).measure_desc   := l_measure_desc19;
t_measures(l_measure_tab19).measure_cp     := l_measure_cp19;

t_measures(l_measure_tab20).measure_own    := l_measure_own20;
t_measures(l_measure_tab20).measure_no     := l_measure_no20;
t_measures(l_measure_tab20).measure_desc   := l_measure_desc20;
t_measures(l_measure_tab20).measure_cp     := l_measure_cp20;

t_measures(l_measure_tab21).measure_own    := l_measure_own21;
t_measures(l_measure_tab21).measure_no     := l_measure_no21;
t_measures(l_measure_tab21).measure_desc   := l_measure_desc21;
t_measures(l_measure_tab21).measure_cp     := l_measure_cp21;

-- RECEPTION tables
t_measures(l_measure_tab22).measure_own    := l_measure_own22;
t_measures(l_measure_tab22).measure_no     := l_measure_no22;
t_measures(l_measure_tab22).measure_desc   := l_measure_desc22;
t_measures(l_measure_tab22).measure_cp     := l_measure_cp22;

-- COMPASS tables
--t_measures(l_measure_tab29).measure_own    := l_measure_own29;
--t_measures(l_measure_tab29).measure_no     := l_measure_no29;
--t_measures(l_measure_tab29).measure_desc   := l_measure_desc29;
--t_measures(l_measure_tab29).measure_cp     := l_measure_cp29;
      

fHandle := UTL_FILE.FOPEN(l_dir, 'preclone_recon_target', 'w');

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
  DBMS_OUTPUT.PUT_LINE('preclone_recon_target ended at:- ' || l_process_end);
  
END;

/
show error;
exit;



