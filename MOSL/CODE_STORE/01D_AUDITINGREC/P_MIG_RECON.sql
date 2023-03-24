CREATE OR REPLACE PROCEDURE P_MIG_RECON(no_batch          IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                        no_job            IN MIG_JOBREF.NO_JOB%TYPE,
                                        return_code       IN OUT NUMBER )
IS

/*------------------------------------------------------------------------------
-- AUTHOR         : Lee Smith
--
-- FILENAME       : P_MIG_RECON.sql
--
-- Subversion $Revision: 6479 $
--
-- CREATED        : 08/04/2016
--
-- Amendment History:
-- Changed By        Date          Description
-- Lee Smith         14/04/2016    'CP2', 150, 'Targets rows dropped during loading' Remove
--                                    Create CP2, 110-130 for each table as per CP1 and CP3
--
--                                 'CP8', 170, 'TEACCESS rows dropped during load'
--                                    Create CP8, 160-165 for each table as per CP7 and CP8
--
--                                 'CP5', 190, 'Eligibilty control table rows dropped during load'
--                                    Create CP5, 170 for table as per CP4 and CP6
--
--                                 'CP11' 210, 'COMPASS SC File row counts dropped during loading'
--                                    Create CP5, 200 for table as per CP10 and CP12
--
--                                 Processing of input files preclone_recon_target and preclone_recon_teaccess
--                                 may not exist and processing should be skipped.
--
-- Lee Smith         15/04/2016    Table ELIGIBILITY_CONTROL_TABLE is now in the CIS Schema
-- Lee Smith         19/05/2016    Remove reference to moutran
-- Lee Smith         04/07/2016    I-269 Non market meters to be counted as 'Non eligible'
-- S.Badhan          05/07/2016    Call FN_RECONLOG with new counter l_prop_nonelig_count_nmm
-- Lee Smith         06/07/2016    I-275 Upper case check on cd_property_use_fut
-- Lee Smith         07/07/2016    Changes to reflect the keygen amendments
-- Kevin Burton      15/09/2016    Changes to CP21 output to work around limit violations resulting from phased delivery
--                                 changed in keygen
-- K.Burton          05/12/2016    Changes to check on fg_mecoms_rdy flag to include phase 4
-- D.Cheung          07/12/2016    Changes to check on fg_mecoms_rdy flag to include 8 and 9
--
-- DESCRIPTION    : Data load reconciliation.
-- NOTES  :
-- This package must be run each time the transform batch is run. 
-------------------------------------------------------------------------------*/
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_MIG_RECON';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  l_error_number                VARCHAR2(255);
  l_error_message               VARCHAR2(512);  
  l_progress                    VARCHAR2(100);
  l_prev_table_name             VARCHAR2(30);
  l_job                         MIG_JOBSTATUS%ROWTYPE;
  l_err                         MIG_ERRORLOG%ROWTYPE; 
  l_mo                          MO_ELIGIBLE_PREMISES%ROWTYPE; 
  l_site                        LU_CONSTRUCTION_SITE%ROWTYPE; 
  l_hlt                         LU_PUBHEALTHRESITE%ROWTYPE; 
  l_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;  
  l_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  l_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  l_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  l_TotalInserted               INTEGER;
  l_rec_written                 BOOLEAN;
  l_no_tables_processed         MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  l_no_rows                     INTEGER;
  l_field_number                INTEGER;
  l_newline                     VARCHAR2(2000);
  l_field_data                  VARCHAR2(2000);
  l_startchar                   INTEGER;
  l_endchar                     INTEGER;
  l_str_length                  INTEGER;
  l_field_cnt                   INTEGER;

  fHandle                       UTL_FILE.FILE_TYPE;
  l_dir                         VARCHAR2(2000):='FILES';

  l_recon_measure_no            NUMBER;
  l_recon_measure_tab           VARCHAR2(30);
  l_recon_measure_cp            VARCHAR2(30);
  l_recon_measure_own           VARCHAR2(30);
  l_recon_measure_desc          Varchar2(2000);
  l_recon_measure2_cp           VARCHAR2(30);
  l_recon_measure2_desc         Varchar2(2000);
  l_prev_prop_nonelig           cis.eligibility_control_table.cd_property_use_fut%TYPE;
  l_prev_prop_elig              cis.eligibility_control_table.cd_property_use_fut%TYPE;
  l_prop_nonelig_count_d        NUMBER:=0;
  l_prop_nonelig_count_e        NUMBER:=0;
  l_prop_nonelig_count_h        NUMBER:=0;
  l_prop_nonelig_count_x        NUMBER:=0;
  l_prop_nonelig_count_others   NUMBER:=0;
  l_prop_nonelig_total          NUMBER:=0;
  l_prop_elig_count_c           NUMBER:=0;
  l_prop_elig_count_i           NUMBER:=0;
  l_prop_elig_count_m           NUMBER:=0;
  l_prop_elig_count_n           NUMBER:=0;
  l_prop_elig_count_others      NUMBER:=0;
  l_prop_elig_total             NUMBER:=0;
  l_distinct_prop_tvmnhhdtl     NUMBER:=0;
  l_distinct_prop_bt_tvp054     NUMBER:=0;
  l_prop_eligible_dropped       NUMBER:=0;
  l_prop_bt_tvp054_dropped      NUMBER:=0;


-- INDEXED BY TABLE
-- HOLDS EACH RECORD UPLOADED FROM THE PRE CLONE RECONCILIATION INPUT FILES
  TYPE preload_type IS TABLE OF VARCHAR2(2000) INDEX BY binary_integer;
  preload_field preload_type;
  preload_init preload_type;

-- RECONCILIATION MEASURES LOOKUP ARRAY DATA.
--
-- IF ADDING OR REMOVING TABLES FROM THE ARRAY ALSO AMEND CURSOR cur_mig_tables BELOW.
--

-- CIS tables
l_measure_own1   VARCHAR2(30)    := 'CIS';
l_measure_no1    NUMBER          := 110;
l_measure_tab1   VARCHAR2(30)    := 'TADADDRESS';
l_measure_desc1  VARCHAR2(2000)  := 'Targets table TADADDRESS row counts after loading into the Reception Area';
l_measure_cp1    VARCHAR2(30)    := 'CP3';
l_measure2_desc1 VARCHAR2(2000)  := 'Targets table TADADDRESS load exception or error counts';
l_measure2_cp1   VARCHAR2(30)    := 'CP2';

l_measure_own2   VARCHAR2(30)    := 'CIS';
l_measure_no2    NUMBER          := 111;
l_measure_tab2   VARCHAR2(30)    := 'TADADDRFAST';
l_measure_desc2  VARCHAR2(2000)  := 'Targets table TADADDRFAST row counts after loading into the Reception Area';
l_measure_cp2    VARCHAR2(30)    := 'CP3';
l_measure2_desc2 VARCHAR2(2000)  := 'Targets table TADADDRFAST load exception or error counts';
l_measure2_cp2   VARCHAR2(30)    := 'CP2';

l_measure_own3   VARCHAR2(30)    := 'CIS';
l_measure_no3    NUMBER          := 112;
l_measure_tab3   VARCHAR2(30)    := 'TVP009APPLIEDCONFG';
l_measure_desc3  VARCHAR2(2000)  := 'Targets table TVP009APPLIEDCONFG row counts after loading into the Reception Area';
l_measure_cp3    VARCHAR2(30)    := 'CP3';
l_measure2_desc3 VARCHAR2(2000)  := 'Targets table TVP009APPLIEDCONFG load exception or error counts';
l_measure2_cp3   VARCHAR2(30)    := 'CP2';

l_measure_own4   VARCHAR2(30)    := 'CIS';
l_measure_no4    NUMBER          := 113;
l_measure_tab4   VARCHAR2(30)    := 'TVP036LEGALENTITY';
l_measure_desc4  VARCHAR2(2000)  := 'Targets table TVP036LEGALENTITY row counts after loading into the Reception Area';
l_measure_cp4    VARCHAR2(30)    := 'CP3';
l_measure2_desc4 VARCHAR2(2000)  := 'Targets table TVP036LEGALENTITY load exception or error counts';
l_measure2_cp4   VARCHAR2(30)    := 'CP2';

l_measure_own5   VARCHAR2(30)    := 'CIS';
l_measure_no5    NUMBER          := 114;
l_measure_tab5   VARCHAR2(30)    := 'TVP046PROPERTY';
l_measure_desc5  VARCHAR2(2000)  := 'Targets table TVP046PROPERTY row counts after loading into the Reception Area';
l_measure_cp5    VARCHAR2(30)    := 'CP3';
l_measure2_desc5 VARCHAR2(2000)  := 'Targets table TVP046PROPERTY load exception or error counts';
l_measure2_cp5   VARCHAR2(30)    := 'CP2';

l_measure_own6   VARCHAR2(30)    := 'CIS';
l_measure_no6    NUMBER          := 115;
l_measure_tab6   VARCHAR2(30)    := 'TVP052REGCONFIG';
l_measure_desc6  VARCHAR2(2000)  := 'Targets table TVP052REGCONFIG row counts after loading into the Reception Area';
l_measure_cp6    VARCHAR2(30)    := 'CP3';
l_measure2_desc6 VARCHAR2(2000)  := 'Targets table TVP052REGCONFIG load exception or error counts';
l_measure2_cp6   VARCHAR2(30)    := 'CP2';

l_measure_own7   VARCHAR2(30)    := 'CIS';
l_measure_no7    NUMBER          := 116;
l_measure_tab7   VARCHAR2(30)    := 'TVP053REGSPEC';
l_measure_desc7  VARCHAR2(2000)  := 'Targets table TVP053REGSPEC row counts after loading into the Reception Area';
l_measure_cp7    VARCHAR2(30)    := 'CP3';
l_measure2_desc7 VARCHAR2(2000)  := 'Targets table TVP053REGSPEC load exception or error counts';
l_measure2_cp7   VARCHAR2(30)    := 'CP2';

l_measure_own8   VARCHAR2(30)    := 'CIS';
l_measure_no8    NUMBER          := 117;
l_measure_tab8   VARCHAR2(30)    := 'TVP054SERVPROVRESP';
l_measure_desc8  VARCHAR2(2000)  := 'Targets table TVP054SERVPROVRESP row counts after loading into the Reception Area';
l_measure_cp8    VARCHAR2(30)    := 'CP3';
l_measure2_desc8 VARCHAR2(2000)  := 'Targets table TVP054SERVPROVRESP load exception or error counts';
l_measure2_cp8   VARCHAR2(30)    := 'CP2';

l_measure_own9   VARCHAR2(30)    := 'CIS';
l_measure_no9    NUMBER          := 118;
l_measure_tab9   VARCHAR2(30)    := 'TVP056SERVPROV';
l_measure_desc9  VARCHAR2(2000)  := 'Targets table TVP056SERVPROV row counts after loading into the Reception Area';
l_measure_cp9    VARCHAR2(30)    := 'CP3';
l_measure2_desc9 VARCHAR2(2000)  := 'Targets table TVP056SERVPROV load exception or error counts';
l_measure2_cp9   VARCHAR2(30)    := 'CP2';

l_measure_own10   VARCHAR2(30)   := 'CIS';
l_measure_no10    NUMBER         := 119;
l_measure_tab10   VARCHAR2(30)   := 'TVP057TARIFF';
l_measure_desc10  VARCHAR2(2000) := 'Targets table TVP057TARIFF row counts after loading into the Reception Area';
l_measure_CP10    VARCHAR2(30)   := 'CP3';
l_measure2_desc10 VARCHAR2(2000) := 'Targets table TVP057TARIFF load exception or error counts';
l_measure2_cp10   VARCHAR2(30)   := 'CP2';

l_measure_own11   VARCHAR2(30)   := 'CIS';
l_measure_no11    NUMBER         := 120;
l_measure_tab11   VARCHAR2(30)   := 'TVP058TARIFFASSGN';
l_measure_desc11  VARCHAR2(2000) := 'Targets table TVP058TARIFFASSGN row counts after loading into the Reception Area';
l_measure_cp11    VARCHAR2(30)   := 'CP3';
l_measure2_desc11 VARCHAR2(2000) := 'Targets table TVP058TARIFFASSGN load exception or error counts';
l_measure2_cp11   VARCHAR2(30)   := 'CP2';

l_measure_own12   VARCHAR2(30)   := 'CIS';
l_measure_no12    NUMBER         := 121;
l_measure_tab12   VARCHAR2(30)   := 'TVP063EQUIPMENT';
l_measure_desc12  VARCHAR2(2000) := 'Targets table TVP063EQUIPMENT row counts after loading into the Reception Area';
l_measure_cp12    VARCHAR2(30)   := 'CP3';
l_measure2_desc12 VARCHAR2(2000) := 'Targets table TVP063EQUIPMENT load exception or error counts';
l_measure2_cp12   VARCHAR2(30)   := 'CP2';

l_measure_own13   VARCHAR2(30)   := 'CIS';
l_measure_no13    NUMBER         := 122;
l_measure_tab13   VARCHAR2(30)   := 'TVP064LENAME';
l_measure_desc13  VARCHAR2(2000) := 'Targets table TVP064LENAME row counts after loading into the Reception Area';
l_measure_cp13    VARCHAR2(30)   := 'CP3';
l_measure2_desc13 VARCHAR2(2000) := 'Targets table TVP064LENAME load exception or error counts';
l_measure2_cp13   VARCHAR2(30)   := 'CP2';

l_measure_own14   VARCHAR2(30)   := 'CIS';
l_measure_no14    NUMBER         := 123;
l_measure_tab14   VARCHAR2(30)   := 'TVP097PROPSCALERT';
l_measure_desc14  VARCHAR2(2000) := 'Targets table TVP097PROPSCALERT row counts after loading into the Reception Area';
l_measure_cp14    VARCHAR2(30)   := 'CP3';
l_measure2_desc14 VARCHAR2(2000) := 'Targets table TVP097PROPSCALERT load exception or error counts';
l_measure2_cp14   VARCHAR2(30)   := 'CP2';

l_measure_own15   VARCHAR2(30)   := 'CIS';
l_measure_no15    NUMBER         := 124;
l_measure_tab15   VARCHAR2(30)   := 'TVP163EQUIPINST';
l_measure_desc15  VARCHAR2(2000) := 'Targets table TVP163EQUIPINST row counts after loading into the Reception Area';
l_measure_cp15    VARCHAR2(30)   := 'CP3';
l_measure2_desc15 VARCHAR2(2000) := 'Targets table TVP163EQUIPINST load exception or error counts';
l_measure2_cp15   VARCHAR2(30)   := 'CP2';

l_measure_own16   VARCHAR2(30)   := 'CIS';
l_measure_no16    NUMBER         := 125;
l_measure_tab16   VARCHAR2(30)   := 'TVP195READING';
l_measure_desc16  VARCHAR2(2000) := 'Targets table TVP195READING row counts after loading into the Reception Area';
l_measure_cp16    VARCHAR2(30)   := 'CP3';
l_measure2_desc16 VARCHAR2(2000) := 'Targets table TVP195READING load exception or error counts';
l_measure2_cp16   VARCHAR2(30)   := 'CP2';

l_measure_own17   VARCHAR2(30)   := 'CIS';
l_measure_no17    NUMBER         := 126;
l_measure_tab17   VARCHAR2(30)   := 'TVP225WATERMTR';
l_measure_desc17  VARCHAR2(2000) := 'Targets table TVP225WATERMTR row counts after loading into the Reception Area';
l_measure_cp17    VARCHAR2(30)   := 'CP3';
l_measure2_desc17 VARCHAR2(2000) := 'Targets table TVP225WATERMTR load exception or error counts';
l_measure2_cp17   VARCHAR2(30)   := 'CP2';

l_measure_own18   VARCHAR2(30)   := 'CIS';
l_measure_no18    NUMBER         := 127;
l_measure_tab18   VARCHAR2(30)   := 'TVP249BILLINGCYCLE';
l_measure_desc18  VARCHAR2(2000) := 'Targets table TVP249BILLINGCYCLE row counts after loading into the Reception Area';
l_measure_cp18    VARCHAR2(30)   := 'CP3';
l_measure2_desc18 VARCHAR2(2000) := 'Targets table TVP249BILLINGCYCLE load exception or error counts';
l_measure2_cp18   VARCHAR2(30)   := 'CP2';

l_measure_own19   VARCHAR2(30)   := 'CIS';
l_measure_no19    NUMBER         := 128;
l_measure_tab19   VARCHAR2(30)   := 'TVP310SERPROVWATER';
l_measure_desc19  VARCHAR2(2000) := 'Targets table TVP310SERPROVWATER row counts after loading into the Reception Area';
l_measure_cp19    VARCHAR2(30)   := 'CP3';
l_measure2_desc19 VARCHAR2(2000) := 'Targets table TVP310SERPROVWATER load exception or error counts';
l_measure2_cp19   VARCHAR2(30)   := 'CP2';

l_measure_own20   VARCHAR2(30)   := 'CIS';
l_measure_no20    NUMBER         := 129;
l_measure_tab20   VARCHAR2(30)   := 'TVP703EXTERNREFDET';
l_measure_desc20  VARCHAR2(2000) := 'Targets table TVP703EXTERNREFDET row counts after loading into the Reception Area';
l_measure_cp20    VARCHAR2(30)   := 'CP3';
l_measure2_desc20 VARCHAR2(2000) := 'Targets table TVP703EXTERNREFDET load exception or error counts';
l_measure2_cp20   VARCHAR2(30)   := 'CP2';

l_measure_own21   VARCHAR2(30)   := 'CIS';
l_measure_no21    NUMBER         := 130;
l_measure_tab21   VARCHAR2(30)   := 'TVP771SPRBLALGITEM';
l_measure_desc21  VARCHAR2(2000) := 'Targets table TVP771SPRBLALGITEM row counts after loading into the Reception Area';
l_measure_cp21    VARCHAR2(30)   := 'CP3';
l_measure2_desc21 VARCHAR2(2000) := 'Targets table TVP771SPRBLALGITEM load exception or error counts';
l_measure2_cp21   VARCHAR2(30)   := 'CP2';


-- RECEPTION TABLES
l_measure_own22   VARCHAR2(30)   := 'CIS';
l_measure_no22    NUMBER         := 180;
l_measure_tab22   VARCHAR2(30)   := 'ELIGIBILITY_CONTROL_TABLE';
l_measure_desc22  VARCHAR2(2000) := 'Targets table ELIGIBILITY_CONTROL_TABLE row counts after loading into the Reception Area';
l_measure_cp22    VARCHAR2(30)   := 'CP6';
l_measure2_desc22 VARCHAR2(2000) := 'Targets table ELIGIBILITY_CONTROL_TABLE load exception or error counts';
l_measure2_cp22   VARCHAR2(30)   := 'CP5';


-- TEACCESS tables
l_measure_own23   VARCHAR2(30)   := 'TEACCESS';
l_measure_no23    NUMBER         := 160;
l_measure_tab23   VARCHAR2(30)   := 'METER_DATA';
l_measure_desc23  VARCHAR2(2000) := 'TEACCESS table METER_DATA row counts after loading into the Reception Area';
l_measure_cp23    VARCHAR2(30)   := 'CP9';
l_measure2_desc23 VARCHAR2(2000) := 'TEACCESS table METER_DATA load exception or error counts';
l_measure2_cp23   VARCHAR2(30)   := 'CP8';

l_measure_own24   VARCHAR2(30)   := 'TEACCESS';
l_measure_no24    NUMBER         := 161;
l_measure_tab24   VARCHAR2(30)   := 'NOTES';
l_measure_desc24  VARCHAR2(2000) := 'TEACCESS table NOTES row counts after loading into the Reception Area';
l_measure_cp24    VARCHAR2(30)   := 'CP9';
l_measure2_desc24 VARCHAR2(2000) := 'TEACCESS table NOTES load exception or error counts';
l_measure2_cp24   VARCHAR2(30)   := 'CP8';


l_measure_own25   VARCHAR2(30)   := 'TEACCESS';
l_measure_no25    NUMBER         := 162;
l_measure_tab25   VARCHAR2(30)   := 'AUDIT_DATA';
l_measure_desc25  VARCHAR2(2000) := 'TEACCESS table AUDIT_DATA row counts after loading into the Reception Area';
l_measure_cp25    VARCHAR2(30)   := 'CP9';
l_measure2_desc25 VARCHAR2(2000) := 'TEACCESS table AUDIT_DATA load exception or error counts';
l_measure2_cp25   VARCHAR2(30)   := 'CP8';


l_measure_own26   VARCHAR2(30)   := 'TEACCESS';
l_measure_no26    NUMBER         := 163;
l_measure_tab26   VARCHAR2(30)   := 'CUS_DATA';
l_measure_desc26  VARCHAR2(2000) := 'TEACCESS table CUS_DATA row counts after loading into the Reception Area';
l_measure_cp26    VARCHAR2(30)   := 'CP9';
l_measure2_desc26 VARCHAR2(2000) := 'TEACCESS table CUS_DATA load exception or error counts';
l_measure2_cp26   VARCHAR2(30)   := 'CP8';


l_measure_own27   VARCHAR2(30)   := 'TEACCESS';
l_measure_no27    NUMBER         := 164;
l_measure_tab27   VARCHAR2(30)   := 'LETTERS';
l_measure_desc27  VARCHAR2(2000) := 'TEACCESS table LETTERS row counts after loading into the Reception Area';
l_measure_cp27    VARCHAR2(30)   := 'CP9';
l_measure2_desc27 VARCHAR2(2000) := 'TEACCESS table LETTERS load exception or error counts';
l_measure2_cp27   VARCHAR2(30)   := 'CP8';


l_measure_own28   VARCHAR2(30)   := 'TEACCESS';
l_measure_no28    NUMBER         := 165;
l_measure_tab28   VARCHAR2(30)   := 'SITE_DATA';
l_measure_desc28  VARCHAR2(2000) := 'TEACCESS table SITE_DATA row counts after loading into the Reception Area';
l_measure_cp28    VARCHAR2(30)   := 'CP9';
l_measure2_desc28 VARCHAR2(2000) := 'TEACCESS table SITE_DATA load exception or error counts';
l_measure2_cp28   VARCHAR2(30)   := 'CP8';

-- COMPASS tables
l_measure_own29   VARCHAR2(30)   := 'CIS';
l_measure_no29    NUMBER         := 200;
l_measure_tab29   VARCHAR2(30)   := 'COMPASS_CONSENTS';
l_measure_desc29  VARCHAR2(2000) := 'COMPASS table COMPASS_CONSENTS row counts after loading into the Reception Area';
l_measure_cp29    VARCHAR2(30)   := 'CP12';
l_measure2_desc29 VARCHAR2(2000) := 'COMPASS table COMPASS_CONSENTS load exception or error counts';
l_measure2_cp29   VARCHAR2(30)   := 'CP11';

-- RECORD OF THE RECONCILIATION MEASURES ARRAY
TYPE measures_type IS RECORD (measure_own   VARCHAR2(30),
                              measure_no    NUMBER(5),
                              measure_desc  VARCHAR2(2000),
                              measure_cp    VARCHAR2(30),
                              measure2_desc VARCHAR2(2000),
                              measure2_cp   VARCHAR2(30)
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
     WHERE owner IN ('CIS', 'TEACCESS', 'RECEPTION')
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
                          'ELIGIBILITY_CONTROL_TABLE',
                          'METER_DATA',
                          'NOTES',
                          'AUDIT_DATA',
                          'CUS_DATA',
                          'LETTERS',
                          'SITE_DATA',
                          'COMPASS_CONSENTS'
                         )
     ORDER BY owner, table_name;
TYPE tab_mig_tables IS TABLE OF cur_mig_tables%ROWTYPE INDEX BY PLS_INTEGER;
t_mig_tables  tab_mig_tables;


-- ROW COUNTS OF NON ELIGIBLE PROPERTIES BY USE CODE
-- No Longer required as test is now based on fg_mecoms_rdy not in 1,2,3
CURSOR cur_prop_noneligible
    IS
    SELECT COUNT(*) noneligibility_count, 
           UPPER(cd_property_use_fut) cd_property_use_fut
      FROM cis.eligibility_control_table
     WHERE UPPER(cd_property_use_fut) IN ('X','H','E','D')
     GROUP BY cd_property_use_fut
     ORDER BY cd_property_use_fut;
TYPE tab_prop_noneligible IS TABLE OF cur_prop_noneligible%ROWTYPE INDEX BY PLS_INTEGER;
t_prop_noneligible  tab_prop_noneligible;

-- ROW COUNTS OF ELIGIBLE PROPERTIES BY USE CODE
-- No Longer required as test is now based on fg_mecoms_rdy in 1,2,3
CURSOR cur_prop_eligible
    IS
    SELECT COUNT(*) eligibility_count, 
           UPPER(cd_property_use_fut) cd_property_use_fut
      FROM cis.eligibility_control_table
     WHERE UPPER(cd_property_use_fut) IN ('N','M','I','C')
        OR fg_nmm = 'Y'
     GROUP BY cd_property_use_fut
     ORDER BY cd_property_use_fut;
TYPE tab_prop_eligible IS TABLE OF cur_prop_eligible%ROWTYPE INDEX BY PLS_INTEGER;
t_prop_eligible  tab_prop_eligible;

CURSOR cur_prop_eligible_dropped  -- Eligible rows filtered/dropped
    IS 
SELECT DISTINCT no_property
  FROM CIS.eligibility_control_table
 MINUS
(SELECT DISTINCT no_property
   FROM tvmnhhdtl
  UNION
  SELECT DISTINCT no_property
    FROM cis.eligibility_control_table
   WHERE fg_mecoms_rdy NOT IN ('1','2','3','4','8','9')
         OR cd_company_system != 'STW1');
TYPE tab_prop_eligible_dropped IS TABLE OF cur_prop_eligible_dropped%ROWTYPE INDEX BY PLS_INTEGER;
t_prop_eligible_dropped  tab_prop_eligible_dropped;

CURSOR cur_prop_bt_tvp054_dropped
    IS
SELECT DISTINCT no_property
  FROM tvmnhhdtl
  MINUS
SELECT DISTINCT no_property
  FROM bt_tvp054;
TYPE tab_prop_bt_tvp054_dropped IS TABLE OF cur_prop_bt_tvp054_dropped%ROWTYPE INDEX BY PLS_INTEGER;
t_prop_bt_tvp054_dropped  tab_prop_bt_tvp054_dropped;


BEGIN
 
   -- initialise variables 
   l_progress := 'Start';
   l_err.TXT_DATA := c_module_name;
   l_err.TXT_KEY := 0;
   l_job.NO_INSTANCE := 0;
   l_no_row_read := 0;
   l_no_row_insert := 0;
   l_no_row_dropped := 0;
   l_no_row_war := 0;
   l_no_row_err := 0;
   l_no_row_exp := 0;
   l_TotalInserted := 0;
   l_prev_table_name := 'NONE';
   l_job.IND_STATUS := 'RUN';
   l_prev_prop_nonelig := '0';
   l_prev_prop_elig := '0';
   l_no_tables_processed := 0;
   l_no_rows := 0;

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
   
   l_progress := 'processing ';

   -- any errors set return code and exit out

   IF l_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, l_job.NO_INSTANCE, l_job.IND_STATUS); 
      return_code := -1;
      RETURN;
   END IF;


-- Load reconciliation measures data (before extract/cloning).
-- This data was generated from Targets tables and written as a comma delimited file
-- to the FILES directory.
  l_progress := 'Load before measures (TARGET) ';

  BEGIN
    fHandle := UTL_FILE.FOPEN(l_dir, 'preclone_recon_target', 'r');
--dbms_output.put_line(l_progress||' preclone_recon_target');

    LOOP
      BEGIN
        UTL_FILE.GET_LINE(fHandle, l_newline);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          EXIT;
      END;

      l_newline:=l_newline||',';

      l_field_number := regexp_count(l_newline, ',' );
--dbms_output.put_line(l_newline);

      l_field_cnt := 1;
      l_startchar := 1;
      preload_field := preload_init;
    

      WHILE l_field_cnt <= l_field_number
      LOOP
--dbms_output.put_line('Field_cnt ='||to_char(l_field_cnt));
        l_endchar := INSTR(l_NewLine, ',', 1, l_field_cnt);
        l_str_length := l_endchar - l_startchar;

        IF l_str_length > 0 THEN
           l_field_data := SUBSTR(l_newline,l_startchar,l_str_length);
        ELSE
           l_field_data:= 'EMPTY';
        END IF;

--dbms_output.put_line(l_field_data);
        preload_field(l_field_cnt):=l_field_data;

        l_field_cnt := l_field_cnt + 1;
        l_startchar := l_endchar+1;
      END LOOP;

--dbms_output.put_line('TARGET Array Data ');
--dbms_output.put_line(preload_field(4));
--dbms_output.put_line(TO_NUMBER(preload_field(3)));
--dbms_output.put_line(TO_NUMBER(preload_field(5)));
--dbms_output.put_line(preload_field(6));

      -- ***** Insert log row ******
      P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                              l_job.NO_INSTANCE,                                       -- Job number
                              preload_field(4),                                        -- Control Point
                              TO_NUMBER(preload_field(3)),                             -- Measure number
                              TO_NUMBER(preload_field(5)),                             -- Data to be recorded
                              preload_field(6));                                       -- Description

      l_TotalInserted := l_TotalInserted + 1;


    END LOOP;

    UTL_FILE.FCLOSE(fHandle);

  EXCEPTION
  -- There has been a problem with the input file and the processing has been skipped
     WHEN OTHERS THEN
        NULL;
  END;

  l_progress := 'Load before measures (TEACCESS) ';
  BEGIN
    fHandle := UTL_FILE.FOPEN(l_dir, 'preclone_recon_teaccess', 'r');
--dbms_output.put_line(l_progress||' preclone_recon_teaccess');

    LOOP
      BEGIN
        UTL_FILE.GET_LINE(fHandle, l_newline);
      EXCEPTION
        WHEN NO_DATA_FOUND THEN
          EXIT;
      END;

      l_newline:=l_newline||',';

      l_field_number := regexp_count(l_newline, ',' );
--dbms_output.put_line(l_newline);

      l_field_cnt := 1;
      l_startchar := 1;
      preload_field := preload_init;
    
      WHILE l_field_cnt <= l_field_number
      LOOP
--dbms_output.put_line('Field_cnt ='||to_char(l_field_cnt));
        l_endchar := INSTR(l_NewLine, ',', 1, l_field_cnt);
        l_str_length := l_endchar - l_startchar;

        IF l_str_length > 0 THEN
           l_field_data := SUBSTR(l_newline,l_startchar,l_str_length);
        ELSE
           l_field_data:= 'EMPTY';
        END IF;

--dbms_output.put_line(l_field_data);
        preload_field(l_field_cnt):=l_field_data;

        l_field_cnt := l_field_cnt + 1;
        l_startchar := l_endchar+1;
      END LOOP;

--dbms_output.put_line('TEACCESS Array Data ');
--dbms_output.put_line(preload_field(4));
--dbms_output.put_line(TO_NUMBER(preload_field(3)));
--dbms_output.put_line(TO_NUMBER(preload_field(5)));
--dbms_output.put_line(preload_field(6));

    -- ***** Insert log row ******
      P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                              l_job.NO_INSTANCE,                                       -- Job number
                              preload_field(4),                                        -- Control Point
                              TO_NUMBER(preload_field(3)),                             -- Measure number
                              TO_NUMBER(preload_field(5)),                             -- Data to be recorded
                              preload_field(6));                                       -- Description

      l_TotalInserted := l_TotalInserted + 1;
    END LOOP;
    UTL_FILE.FCLOSE(fHandle);
  EXCEPTION
  -- There has been a problem with the input file and the processing has been skipped
     WHEN OTHERS THEN
        NULL;
  END;

/*
-- Spec change see amendment history
  l_progress := 'Load before exceptions and errors ';
-- The initial data load from Target is an image copy not SQLLDR.
-- Therefore, there will be NO load exceptions or errors.
  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                          l_job.NO_INSTANCE,                                       -- Job number
                          'CP2',                                                       -- Control Point
                          150,                                                     -- Measure number
                          0,                                                       -- Data to be recorded
                          'Targets rows dropped during loading');                  -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                          l_job.NO_INSTANCE,                                       -- Job number
                          'CP8',                                                       -- Control Point
                          170,                                                     -- Measure number
                          0,                                                       -- Data to be recorded
                          'TEACCESS rows dropped during load');                    -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                          l_job.NO_INSTANCE,                                       -- Job number
                          'CP5',                                                       -- Control Point
                          190,                                                     -- Measure number
                          0,                                                       -- Data to be recorded
                          'Eligibilty control table rows dropped during load');    -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                -- Batch number
                          l_job.NO_INSTANCE,                                       -- Job number
                          'CP11',                                                      -- Control Point
                          210,                                                     -- Measure number
                          0,                                                       -- Data to be recorded **** AS LOADED FROM A FLAT FILE MAY NOT BE ZERO ****
                          'COMPASS SC File row counts dropped during loading');    -- Description
  l_TotalInserted := l_TotalInserted + 1;
*/


-- Populate lookup Table.
  l_progress := 'Load measures array ';
-- CIS tables
  t_measures(l_measure_tab1).measure_own     := l_measure_own1;
  t_measures(l_measure_tab1).measure_no      := l_measure_no1;
  t_measures(l_measure_tab1).measure_desc    := l_measure_desc1;
  t_measures(l_measure_tab1).measure_cp      := l_measure_cp1;
  t_measures(l_measure_tab1).measure2_desc   := l_measure2_desc1;
  t_measures(l_measure_tab1).measure2_cp     := l_measure2_cp1;

  t_measures(l_measure_tab2).measure_own     := l_measure_own2;
  t_measures(l_measure_tab2).measure_no      := l_measure_no2;
  t_measures(l_measure_tab2).measure_desc    := l_measure_desc2;
  t_measures(l_measure_tab2).measure_cp      := l_measure_cp2;
  t_measures(l_measure_tab2).measure2_desc   := l_measure2_desc2;
  t_measures(l_measure_tab2).measure2_cp     := l_measure2_cp2;

  t_measures(l_measure_tab3).measure_own     := l_measure_own3;
  t_measures(l_measure_tab3).measure_no      := l_measure_no3;
  t_measures(l_measure_tab3).measure_desc    := l_measure_desc3;
  t_measures(l_measure_tab3).measure_cp      := l_measure_cp3;
  t_measures(l_measure_tab3).measure2_desc   := l_measure2_desc3;
  t_measures(l_measure_tab3).measure2_cp     := l_measure2_cp3;

  t_measures(l_measure_tab4).measure_own     := l_measure_own4;
  t_measures(l_measure_tab4).measure_no      := l_measure_no4;
  t_measures(l_measure_tab4).measure_desc    := l_measure_desc4;
  t_measures(l_measure_tab4).measure_cp      := l_measure_cp4;
  t_measures(l_measure_tab4).measure2_desc   := l_measure2_desc4;
  t_measures(l_measure_tab4).measure2_cp     := l_measure2_cp4;

  t_measures(l_measure_tab5).measure_own     := l_measure_own5;
  t_measures(l_measure_tab5).measure_no      := l_measure_no5;
  t_measures(l_measure_tab5).measure_desc    := l_measure_desc5;
  t_measures(l_measure_tab5).measure_cp      := l_measure_cp5;
  t_measures(l_measure_tab5).measure2_desc   := l_measure2_desc5;
  t_measures(l_measure_tab5).measure2_cp     := l_measure2_cp5;

  t_measures(l_measure_tab6).measure_own     := l_measure_own6;
  t_measures(l_measure_tab6).measure_no      := l_measure_no6;
  t_measures(l_measure_tab6).measure_desc    := l_measure_desc6;
  t_measures(l_measure_tab6).measure_cp      := l_measure_cp6;
  t_measures(l_measure_tab6).measure2_desc   := l_measure2_desc6;
  t_measures(l_measure_tab6).measure2_cp     := l_measure2_cp6;

  t_measures(l_measure_tab7).measure_own     := l_measure_own7;
  t_measures(l_measure_tab7).measure_no      := l_measure_no7;
  t_measures(l_measure_tab7).measure_desc    := l_measure_desc7;
  t_measures(l_measure_tab7).measure_cp      := l_measure_cp7;
  t_measures(l_measure_tab7).measure2_desc   := l_measure2_desc7;
  t_measures(l_measure_tab7).measure2_cp     := l_measure2_cp7;

  t_measures(l_measure_tab8).measure_own     := l_measure_own8;
  t_measures(l_measure_tab8).measure_no      := l_measure_no8;
  t_measures(l_measure_tab8).measure_desc    := l_measure_desc8;
  t_measures(l_measure_tab8).measure_cp      := l_measure_cp8;
  t_measures(l_measure_tab8).measure2_desc   := l_measure2_desc8;
  t_measures(l_measure_tab8).measure2_cp     := l_measure2_cp8;

  t_measures(l_measure_tab9).measure_own     := l_measure_own9;
  t_measures(l_measure_tab9).measure_no      := l_measure_no9;
  t_measures(l_measure_tab9).measure_desc    := l_measure_desc9;
  t_measures(l_measure_tab9).measure_cp      := l_measure_cp9;
  t_measures(l_measure_tab9).measure2_desc   := l_measure2_desc9;
  t_measures(l_measure_tab9).measure2_cp     := l_measure2_cp9;

  t_measures(l_measure_tab10).measure_own    := l_measure_own10;
  t_measures(l_measure_tab10).measure_no     := l_measure_no10;
  t_measures(l_measure_tab10).measure_desc   := l_measure_desc10;
  t_measures(l_measure_tab10).measure_cp     := l_measure_cp10;
  t_measures(l_measure_tab10).measure2_desc  := l_measure2_desc10;
  t_measures(l_measure_tab10).measure2_cp    := l_measure2_cp10;

  t_measures(l_measure_tab11).measure_own    := l_measure_own11;
  t_measures(l_measure_tab11).measure_no     := l_measure_no11;
  t_measures(l_measure_tab11).measure_desc   := l_measure_desc11;
  t_measures(l_measure_tab11).measure_cp     := l_measure_cp11;
  t_measures(l_measure_tab11).measure2_desc  := l_measure2_desc11;
  t_measures(l_measure_tab11).measure2_cp    := l_measure2_cp11;

  t_measures(l_measure_tab12).measure_own    := l_measure_own12;
  t_measures(l_measure_tab12).measure_no     := l_measure_no12;
  t_measures(l_measure_tab12).measure_desc   := l_measure_desc12;
  t_measures(l_measure_tab12).measure_cp     := l_measure_cp12;
  t_measures(l_measure_tab12).measure2_desc  := l_measure2_desc12;
  t_measures(l_measure_tab12).measure2_cp    := l_measure2_cp12;

  t_measures(l_measure_tab13).measure_own    := l_measure_own13;
  t_measures(l_measure_tab13).measure_no     := l_measure_no13;
  t_measures(l_measure_tab13).measure_desc   := l_measure_desc13;
  t_measures(l_measure_tab13).measure_cp     := l_measure_cp13;
  t_measures(l_measure_tab13).measure2_desc  := l_measure2_desc13;
  t_measures(l_measure_tab13).measure2_cp    := l_measure2_cp13;

  t_measures(l_measure_tab14).measure_own    := l_measure_own14;
  t_measures(l_measure_tab14).measure_no     := l_measure_no14;
  t_measures(l_measure_tab14).measure_desc   := l_measure_desc14;
  t_measures(l_measure_tab14).measure_cp     := l_measure_cp14;
  t_measures(l_measure_tab14).measure2_desc  := l_measure2_desc14;
  t_measures(l_measure_tab14).measure2_cp    := l_measure2_cp14;

  t_measures(l_measure_tab15).measure_own    := l_measure_own15;
  t_measures(l_measure_tab15).measure_no     := l_measure_no15;
  t_measures(l_measure_tab15).measure_desc   := l_measure_desc15;
  t_measures(l_measure_tab15).measure_cp     := l_measure_cp15;
  t_measures(l_measure_tab15).measure2_desc  := l_measure2_desc15;
  t_measures(l_measure_tab15).measure2_cp    := l_measure2_cp15;

  t_measures(l_measure_tab16).measure_own    := l_measure_own16;
  t_measures(l_measure_tab16).measure_no     := l_measure_no16;
  t_measures(l_measure_tab16).measure_desc   := l_measure_desc16;
  t_measures(l_measure_tab16).measure_cp     := l_measure_cp16;
  t_measures(l_measure_tab16).measure2_desc  := l_measure2_desc16;
  t_measures(l_measure_tab16).measure2_cp    := l_measure2_cp16;

  t_measures(l_measure_tab17).measure_own    := l_measure_own17;
  t_measures(l_measure_tab17).measure_no     := l_measure_no17;
  t_measures(l_measure_tab17).measure_desc   := l_measure_desc17;
  t_measures(l_measure_tab17).measure_cp     := l_measure_cp17;
  t_measures(l_measure_tab17).measure2_desc  := l_measure2_desc17;
  t_measures(l_measure_tab17).measure2_cp    := l_measure2_cp17;

  t_measures(l_measure_tab18).measure_own    := l_measure_own18;
  t_measures(l_measure_tab18).measure_no     := l_measure_no18;
  t_measures(l_measure_tab18).measure_desc   := l_measure_desc18;
  t_measures(l_measure_tab18).measure_cp     := l_measure_cp18;
  t_measures(l_measure_tab18).measure2_desc  := l_measure2_desc18;
  t_measures(l_measure_tab18).measure2_cp    := l_measure2_cp18;

  t_measures(l_measure_tab19).measure_own    := l_measure_own19;
  t_measures(l_measure_tab19).measure_no     := l_measure_no19;
  t_measures(l_measure_tab19).measure_desc   := l_measure_desc19;
  t_measures(l_measure_tab19).measure_cp     := l_measure_cp19;
  t_measures(l_measure_tab19).measure2_desc  := l_measure2_desc19;
  t_measures(l_measure_tab19).measure2_cp    := l_measure2_cp19;

  t_measures(l_measure_tab20).measure_own    := l_measure_own20;
  t_measures(l_measure_tab20).measure_no     := l_measure_no20;
  t_measures(l_measure_tab20).measure_desc   := l_measure_desc20;
  t_measures(l_measure_tab20).measure_cp     := l_measure_cp20;
  t_measures(l_measure_tab20).measure2_desc  := l_measure2_desc20;
  t_measures(l_measure_tab20).measure2_cp    := l_measure2_cp20;

  t_measures(l_measure_tab21).measure_own    := l_measure_own21;
  t_measures(l_measure_tab21).measure_no     := l_measure_no21;
  t_measures(l_measure_tab21).measure_desc   := l_measure_desc21;
  t_measures(l_measure_tab21).measure_cp     := l_measure_cp21;
  t_measures(l_measure_tab21).measure2_desc  := l_measure2_desc21;
  t_measures(l_measure_tab21).measure2_cp    := l_measure2_cp21;

  -- RECEPTION tables
  t_measures(l_measure_tab22).measure_own    := l_measure_own22;
  t_measures(l_measure_tab22).measure_no     := l_measure_no22;
  t_measures(l_measure_tab22).measure_desc   := l_measure_desc22;
  t_measures(l_measure_tab22).measure_cp     := l_measure_cp22;
  t_measures(l_measure_tab22).measure2_desc  := l_measure2_desc22;
  t_measures(l_measure_tab22).measure2_cp    := l_measure2_cp22;

  -- TEACCESS tables
  t_measures(l_measure_tab23).measure_own    := l_measure_own23;
  t_measures(l_measure_tab23).measure_no     := l_measure_no23;
  t_measures(l_measure_tab23).measure_desc   := l_measure_desc23;
  t_measures(l_measure_tab23).measure_cp     := l_measure_cp23;
  t_measures(l_measure_tab23).measure2_desc  := l_measure2_desc23;
  t_measures(l_measure_tab23).measure2_cp    := l_measure2_cp23;

  t_measures(l_measure_tab24).measure_own    := l_measure_own24;
  t_measures(l_measure_tab24).measure_no     := l_measure_no24;
  t_measures(l_measure_tab24).measure_desc   := l_measure_desc24;
  t_measures(l_measure_tab24).measure_cp     := l_measure_cp24;
  t_measures(l_measure_tab24).measure2_desc  := l_measure2_desc24;
  t_measures(l_measure_tab24).measure2_cp    := l_measure2_cp24;

  t_measures(l_measure_tab25).measure_own    := l_measure_own25;
  t_measures(l_measure_tab25).measure_no     := l_measure_no25;
  t_measures(l_measure_tab25).measure_desc   := l_measure_desc25;
  t_measures(l_measure_tab25).measure_cp     := l_measure_cp25;
  t_measures(l_measure_tab25).measure2_desc  := l_measure2_desc25;
  t_measures(l_measure_tab25).measure2_cp    := l_measure2_cp25;

  t_measures(l_measure_tab26).measure_own    := l_measure_own26;
  t_measures(l_measure_tab26).measure_no     := l_measure_no26;
  t_measures(l_measure_tab26).measure_desc   := l_measure_desc26;
  t_measures(l_measure_tab26).measure_cp     := l_measure_cp26;
  t_measures(l_measure_tab26).measure2_desc  := l_measure2_desc26;
  t_measures(l_measure_tab26).measure2_cp    := l_measure2_cp26;

  t_measures(l_measure_tab27).measure_own    := l_measure_own27;
  t_measures(l_measure_tab27).measure_no     := l_measure_no27;
  t_measures(l_measure_tab27).measure_desc   := l_measure_desc27;
  t_measures(l_measure_tab27).measure_cp     := l_measure_cp27;
  t_measures(l_measure_tab27).measure2_desc  := l_measure2_desc27;
  t_measures(l_measure_tab27).measure2_cp    := l_measure2_cp27;

  t_measures(l_measure_tab28).measure_own    := l_measure_own28;
  t_measures(l_measure_tab28).measure_no     := l_measure_no28;
  t_measures(l_measure_tab28).measure_desc   := l_measure_desc28;
  t_measures(l_measure_tab28).measure_cp     := l_measure_cp28;
  t_measures(l_measure_tab28).measure2_desc  := l_measure2_desc28;
  t_measures(l_measure_tab28).measure2_cp    := l_measure2_cp28;

  -- COMPASS tables
  t_measures(l_measure_tab29).measure_own    := l_measure_own29;
  t_measures(l_measure_tab29).measure_no     := l_measure_no29;
  t_measures(l_measure_tab29).measure_desc   := l_measure_desc29;
  t_measures(l_measure_tab29).measure_cp     := l_measure_cp29;
  t_measures(l_measure_tab29).measure2_desc  := l_measure2_desc29;
  t_measures(l_measure_tab29).measure2_cp    := l_measure2_cp29;
      
  
  OPEN cur_mig_tables;

  l_progress := 'loop processing cloned tables ';

  LOOP
  
    FETCH cur_mig_tables BULK COLLECT INTO t_mig_tables LIMIT 99999999;   -- l_job.NO_COMMIT;
 
    FOR i IN 1..t_mig_tables.COUNT
    LOOP
    
      l_err.TXT_KEY := t_mig_tables(i).table_name;
      l_mo := NULL;
      
      IF l_prev_table_name <> t_mig_tables(i).table_name THEN

          -- Dynamically count the number of rows
         EXECUTE IMMEDIATE 'SELECT COUNT(*) FROM ' || t_mig_tables(i).owner || ' .' 
--                           || t_mig_tables(i).table_name || ' WHERE ROWNUM < 101 ' INTO l_no_rows;  -- Limit to 100 rows
                           || t_mig_tables(i).table_name INTO l_no_rows;                              -- All rows


          -- Lookup measure number and control points from the t_measures array
         BEGIN
            l_recon_measure_own  := t_measures(t_mig_tables(i).table_name).measure_own;
            l_recon_measure_no   := t_measures(t_mig_tables(i).table_name).measure_no;
            l_recon_measure_cp   := t_measures(t_mig_tables(i).table_name).measure_cp;
            l_recon_measure_desc := t_measures(t_mig_tables(i).table_name).measure_desc;
            l_recon_measure2_cp   := t_measures(t_mig_tables(i).table_name).measure2_cp;
            l_recon_measure2_desc := t_measures(t_mig_tables(i).table_name).measure2_desc;
--DBMS_OUTPUT.PUT_LINE(l_recon_measure_no);
--DBMS_OUTPUT.PUT_LINE(t_mig_tables(i).owner);
--DBMS_OUTPUT.PUT_LINE(t_mig_tables(i).table_name);
--DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_recon_measure_cp));
--DBMS_OUTPUT.PUT_LINE(TO_CHAR(l_recon_measure_desc));
--DBMS_OUTPUT.PUT_LINE('Count='||to_char(l_no_rows));
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
              DBMS_OUTPUT.PUT_LINE(TO_CHAR(t_mig_tables(i).table_name) || ' NOT FOUND');
         END;


         P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                -- Batch number
                                 l_job.NO_INSTANCE,                                                       -- Job number
                                 l_recon_measure_cp,                                                      -- Control Point
                                 l_recon_measure_no,                                                      -- Measure number
                                 l_no_rows,                                                               -- Data to be recorded
                                 l_recon_measure_desc);                                                   -- Description
         l_TotalInserted := l_TotalInserted + 1;

         P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                -- Batch number
                                 l_job.NO_INSTANCE,                                                       -- Job number
                                 l_recon_measure2_cp,                                                      -- Control Point
                                 l_recon_measure_no,                                                      -- Measure number
                                 0,                                                                       -- Data to be recorded
                                 l_recon_measure2_desc);                                                   -- Description
         l_TotalInserted := l_TotalInserted + 1;
      
          -- keep count of number of tables processed
         l_no_tables_processed := l_no_tables_processed + 1;
         l_prev_table_name := t_mig_tables(i).table_name;

      
      END IF;
      
    END LOOP;

    IF t_mig_tables.COUNT < 999999 THEN --l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_mig_tables;
 

  l_progress := 'loop processing non eligible property use codes (Future) ';
  SELECT COUNT(*)
    INTO l_prop_nonelig_total
    FROM cis.eligibility_control_table
   WHERE fg_mecoms_rdy NOT IN ('1','2','3','4','8','9')
         OR cd_company_system != 'STW1';

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          505,                                                                                           -- Measure number
                          l_prop_nonelig_total,                                                                          -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (Total) '); -- Description
  l_TotalInserted := l_TotalInserted + 1;
  
/*
  OPEN cur_prop_noneligible;
  LOOP
    FETCH cur_prop_noneligible BULK COLLECT INTO t_prop_noneligible LIMIT 99999999;   -- l_job.NO_COMMIT;

    FOR i IN 1..t_prop_noneligible.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop_noneligible(i).cd_property_use_fut;
      l_mo := NULL;
      
      IF l_prev_prop_nonelig <> t_prop_noneligible(i).cd_property_use_fut THEN
         IF t_prop_noneligible(i).cd_property_use_fut = 'D' THEN
            l_prop_nonelig_count_d := t_prop_noneligible(i).noneligibility_count;
         ELSIF t_prop_noneligible(i).cd_property_use_fut = 'E' THEN
            l_prop_nonelig_count_e := t_prop_noneligible(i).noneligibility_count;
         ELSIF t_prop_noneligible(i).cd_property_use_fut = 'H' THEN
            l_prop_nonelig_count_h := t_prop_noneligible(i).noneligibility_count;
         ELSIF t_prop_noneligible(i).cd_property_use_fut = 'X' THEN
            l_prop_nonelig_count_x := t_prop_noneligible(i).noneligibility_count;
         ELSE
            l_prop_nonelig_count_others := t_prop_noneligible(i).noneligibility_count;
         END IF;
      END IF;
    END LOOP;

    IF t_prop_noneligible.COUNT < 999999 THEN --l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_prop_noneligible;

  l_prop_nonelig_total := l_prop_nonelig_count_d +
                          l_prop_nonelig_count_e +
                          l_prop_nonelig_count_h +
                          l_prop_nonelig_count_x +
                          l_prop_nonelig_count_others;
--DBMS_OUTPUT.PUT_LINE('Non eligible totel = ' || TO_CHAR(l_prop_nonelig_total));

  l_progress := 'load non eligible log entries ';


  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                        -- Control Point
                          500,                                                                                           -- Measure number
                          l_prop_nonelig_count_x,                                                                        -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (X) ');     -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          501,                                                                                           -- Measure number
                          l_prop_nonelig_count_h,                                                                        -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (H) ');     -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          502,                                                                                           -- Measure number
                          l_prop_nonelig_count_e,                                                                        -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (E) ');     -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          503,                                                                                           -- Measure number
                          l_prop_nonelig_count_d,                                                                        -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (D) ');     -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          504,                                                                                           -- Measure number
                          l_prop_nonelig_count_others,                                                                          -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (Others) '); -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP16',                                                                                            -- Control Point
                          505,                                                                                           -- Measure number
                          l_prop_nonelig_total,                                                                          -- Data to be recorded
                          'Eligibility Control Table - Count of Non Eligible Properties by Property Use Code (Total) '); -- Description
  l_TotalInserted := l_TotalInserted + 1;
*/

  l_progress := 'loop processing eligible properties ';
  SELECT COUNT(*)
    INTO l_prop_elig_total
    FROM cis.eligibility_control_table
   WHERE fg_mecoms_rdy IN ('1','2','3','4','8','9')
         AND cd_company_system = 'STW1';
         
  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP17',                                                                                        -- Control Point
                          515,                                                                                           -- Measure number
                          l_prop_elig_total,                                                                             -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (Total) '); -- Description
  l_TotalInserted := l_TotalInserted + 1;

/*
  OPEN cur_prop_eligible;
  LOOP
    FETCH cur_prop_eligible BULK COLLECT INTO t_prop_eligible LIMIT 99999999;   -- l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop_eligible.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop_eligible(i).cd_property_use_fut;
      l_mo := NULL;
      
      IF l_prev_prop_elig <> t_prop_eligible(i).cd_property_use_fut THEN
         IF t_prop_eligible(i).cd_property_use_fut = 'N' THEN
            l_prop_elig_count_n := t_prop_eligible(i).eligibility_count;
         ELSIF t_prop_eligible(i).cd_property_use_fut = 'M' THEN
            l_prop_elig_count_m := t_prop_eligible(i).eligibility_count;
         ELSIF t_prop_eligible(i).cd_property_use_fut = 'I' THEN
            l_prop_elig_count_i := t_prop_eligible(i).eligibility_count;
         ELSIF t_prop_eligible(i).cd_property_use_fut = 'C' THEN
            l_prop_elig_count_c := t_prop_eligible(i).eligibility_count;
         ELSE
            l_prop_elig_count_others := t_prop_eligible(i).eligibility_count;
         END IF;
      END IF;
      
    END LOOP;

    IF t_prop_eligible.COUNT < 999999 THEN --l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_prop_eligible;

  l_prop_elig_total := l_prop_elig_count_n +
                       l_prop_elig_count_m +
                       l_prop_elig_count_i +
                       l_prop_elig_count_c +
                       l_prop_elig_count_others;
--DBMS_OUTPUT.PUT_LINE('Eligible totel = ' || TO_CHAR(l_prop_elig_total));


  l_progress := 'load non eligible log entries ';
  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                    -- Batch number
                          l_job.NO_INSTANCE,                                                                           -- Job number
                          'CP17',                                                                                          -- Control Point
                          510,                                                                                         -- Measure number
                          l_prop_elig_count_n,                                                                         -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code  (X) ');      -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                    -- Batch number
                          l_job.NO_INSTANCE,                                                                           -- Job number
                          'CP17',                                                                                          -- Control Point
                          511,                                                                                         -- Measure number
                          l_prop_elig_count_m,                                                                         -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (H) ');       -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                    -- Batch number
                          l_job.NO_INSTANCE,                                                                           -- Job number
                          'CP17',                                                                                          -- Control Point
                          512,                                                                                         -- Measure number
                          l_prop_elig_count_i,                                                                         -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (E) ');       -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                    -- Batch number
                          l_job.NO_INSTANCE,                                                                           -- Job number
                          'CP17',                                                                                          -- Control Point
                          513,                                                                                         -- Measure number
                          l_prop_elig_count_c,                                                                         -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (D) ');       -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                    -- Batch number
                          l_job.NO_INSTANCE,                                                                           -- Job number
                          'CP17',                                                                                          -- Control Point
                          514,                                                                                         -- Measure number
                          l_prop_elig_total,                                                                           -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (Others) ');   -- Description
  l_TotalInserted := l_TotalInserted + 1;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP17',                                                                                            -- Control Point
                          515,                                                                                           -- Measure number
                          l_prop_nonelig_total,                                                                          -- Data to be recorded
                          'Eligibility Control Table - Count of Eligible Properties by Property Use Code (Total) '); -- Description
  l_TotalInserted := l_TotalInserted + 1;
*/

  l_progress := 'Load count of Properties on TVMNHHDTL (Key_GEN Stage 1)';
  SELECT COUNT(DISTINCT no_property)
    INTO l_distinct_prop_tvmnhhdtl
    FROM reception.tvmnhhdtl;


  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP18',                                                                                            -- Control Point
                          630,                                                                                           -- Measure number
                          l_distinct_prop_tvmnhhdtl,                                                                        -- Data to be recorded
                          'Distinct count of Properties loaded on TVMNHHDTL (Key_GEN Stage 1)');                         -- Description
  l_TotalInserted := l_TotalInserted + 1;
  
  l_progress := 'Properties dropped/filtered from load (Key_GEN Stage 1)';
   OPEN cur_prop_eligible_dropped;

  LOOP
  
    FETCH cur_prop_eligible_dropped BULK COLLECT INTO t_prop_eligible_dropped LIMIT 99999999;   -- l_job.NO_COMMIT;
 
    FOR i IN 1..t_prop_eligible_dropped.COUNT
    LOOP
    
      l_err.TXT_KEY := t_prop_eligible_dropped(i).no_property;
      l_mo := NULL;
      l_prop_eligible_dropped := l_prop_eligible_dropped + 1;
      
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH,                                                                                  -- Batch number
                              L_JOB.NO_INSTANCE,                                                                         -- Job number
                              'X',                                                                                       -- log
                              SUBSTR('CP19 Properties dropped/filtered',1,100),                                          -- error
                              L_ERR.TXT_KEY,                                                                             -- key
                              SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100)                                          -- data
                             );

    END LOOP;

    IF t_prop_eligible_dropped.COUNT < 999999 THEN --l_job.NO_COMMIT THEN
       EXIT;
    ELSE
       COMMIT;
    END IF;
     
  END LOOP;
  CLOSE cur_prop_eligible_dropped;
 
  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP19',                                                                                            -- Control Point
                          620,                                                                                           -- Measure number
                          l_prop_eligible_dropped,             -- Dropped/Filtered by proc 1_create_tvmnhhdtl_from_elig  -- Data to be recorded
                          'Distinct count of Properties dropped from load TVMNHHDTL (Key_GEN Stage 1)');                 -- Description
  l_TotalInserted := l_TotalInserted + 1;


--DBMS_OUTPUT.PUT_LINE('Properties loaded on TVMNHHDTL = ' || TO_CHAR(l_distinct_prop_tvmnhhdtl));


  l_progress := 'Load count of Properties on BT_TVP054 (Key_GEN Stage 1)';
  SELECT COUNT(DISTINCT no_property)
    INTO l_distinct_prop_bt_tvp054
    FROM bt_tvp054;

  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP20',                                                                                            -- Control Point
                          640,                                                                                           -- Measure number
                          l_distinct_prop_bt_tvp054,                                                                        -- Data to be recorded
                          'Distinct count of Properties loaded on BT_TVP054 (Key_GEN Stage 1)');                         -- Description
  l_TotalInserted := l_TotalInserted + 1;


  l_progress := 'Properties dropped/filtered from load (BT_TVP054)';

  SELECT COUNT(*) INTO l_prop_bt_tvp054_dropped
  FROM (SELECT DISTINCT no_property FROM tvmnhhdtl
        MINUS
        SELECT DISTINCT no_property FROM bt_tvp054);
--   OPEN cur_prop_bt_tvp054_dropped;
--
--  LOOP
--  
--    FETCH cur_prop_bt_tvp054_dropped BULK COLLECT INTO t_prop_bt_tvp054_dropped LIMIT 99999999;   -- l_job.NO_COMMIT;
-- 
--    FOR i IN 1..t_prop_bt_tvp054_dropped.COUNT
--    LOOP
--    
--      l_err.TXT_KEY := t_prop_bt_tvp054_dropped(i).no_property;
--      l_mo := NULL;
--      l_prop_bt_tvp054_dropped := l_prop_bt_tvp054_dropped + 1;
--      
      P_MIG_BATCH.FN_ERRORLOG(NO_BATCH,                                                                                  -- Batch number
                              L_JOB.NO_INSTANCE,                                                                         -- Job number
                              'X',                                                                                       -- log
                              SUBSTR('CP21 ' || l_prop_bt_tvp054_dropped || ' Properties dropped/filtered',1,100),       -- error
                              L_ERR.TXT_KEY,                                                                             -- key
                              SUBSTR(l_err.TXT_DATA || ',' || L_PROGRESS,1,100)                                          -- data
                             );
--
--    END LOOP;
--
--    IF t_prop_bt_tvp054_dropped.COUNT < 999999 THEN --l_job.NO_COMMIT THEN
--       EXIT;
--    ELSE
--       COMMIT;
--    END IF;
--     
--  END LOOP;
--  CLOSE cur_prop_bt_tvp054_dropped;

 
  P_MIG_BATCH.FN_RECONLOG(no_batch,                                                                                      -- Batch number
                          l_job.NO_INSTANCE,                                                                             -- Job number
                          'CP21',                                                                                        -- Control Point
                          650,                                                                                           -- Measure number
                          l_prop_bt_tvp054_dropped,             -- Dropped/Filtered by proc                              -- Data to be recorded
                          'Distinct count of Properties dropped from load BT_TVP054 (Key_GEN Stage 1)');                 -- Description
  l_TotalInserted := l_TotalInserted + 1;


--DBMS_OUTPUT.PUT_LINE('Properties loaded on BT_TVP054 = ' || TO_CHAR(l_distinct_prop_bt_tvp054));


--dbms_output.put_line('Inserted rows '||to_char(l_TotalInserted));
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
END P_MIG_RECON;
/
show error;

exit;


