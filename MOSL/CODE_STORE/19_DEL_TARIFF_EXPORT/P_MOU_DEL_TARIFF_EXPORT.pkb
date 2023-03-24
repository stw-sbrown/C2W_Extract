create or replace
PACKAGE BODY P_MOU_DEL_TARIFF_EXPORT AS
----------------------------------------------------------------------------------------
-- PACKAGE SPECIFICATION: Wholesaler Tariff XML Export
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_DEL_TARIFF_EXPORT_PKG.pkb
--
-- Subversion $Revision: 5284 $
--
-- CREATED        : 31/03/2016
--
-- DESCRIPTION    : Package to export tariff data into XML file as specified
--                  in MOSL Tariff XSD
-- NOTES  :
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      01/04/2016  K.Burton   Initial Draft
-- V 0.02      07/04/2016  K.Burton   Amendments as per updated MOSL guidance in
--                                    Tariff-Table-Master-Data-Guidance-v1.0-060416
-- V 0.03      22/04/2016  K.Burton   Repositioned <chargeelementlist> tag as per MOSL feedback
--                                    Updated to exclude any isapplicable = false elements
-- V 0.04      26/04/2016  K.Burton   Minor name changes for tags as per MOSL feedback
--                                    Alterations for Standing Data format
-- V 0.05      28/04/2016  K.Burton   Changed cursor call for BoBT - was calling wrong cursor
--                                    causing <rows/> tag to be output in error as per MOSL feedback
-- V 0.06      29/04/2016  K.Burton   Code changes to cursors for addition of NIL data row
--                                    Added SetNilTag procedure
-- V 0.07      06/05/2016  K.Burton   Adjustment to counts for Standing Data - stop counting it
--                                    for reconciliations because it's not really a tariff
-- V 0.08      09/05/2016  K.Burton   Added pre-validation proc P_DEL_VALIDATION_CHECKS - validates
--                                    tariff table data to check all pre-reqs are met before exporting
--                                    to file.
-- V 0.09      12/05/2016  K.Burton   Fixed issue with " rendering in tarifflist tag
-- V 0.10      13/06/2016  K.Burton   Added filter to exclude crossborder tariff export
-- V 0.11      23/06/2016  K.Burton   Change to main cursor for seasonal tariffs
-- V 0.12      04/07/2016  K.Burton   Added new tariff version cursor to accommodate MOSL changes to
--                                    XML spec for seasonal charges
-- V 0.13      06/07/2016  K.Burton   Changes to P_DEL_TARIFF_EXPORT_SW from MOSL feedback
-- V 0.14      13/07/2016  K.Burton   Issue I-292 - additional lookup table checks needed for SW and HD
-- V 0.15      14/07/2016  K/Burton   Element name correction Assessed Band Charges for Surface Water
--                                    Draining SWBandCharge (D7452) corrected to Area Charges
-- V 0.16      25/08/2016  S.Badhan   I-320. If user FINDEL use directory FINEXPORT.
-----------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - MAIN ROUTINE
-- AUTHOR         : Kevin Burton
-- CREATED        : 01/04/2016
-- DESCRIPTION    : Main procedure to pull all the exports together
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_MAIN(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER) IS

  -- MAIN TARIFF CURSOR
  -- V 0.12 - Change to remove tariff version related information - this now comes from second cursor
  CURSOR cur_tariff IS
    SELECT MT.TARIFFNAME name,
           MT.TARIFFCODE_PK code,
           TO_CHAR (MT.TARIFFEFFECTIVEFROMDATE,'YYYY-MM-DD') effective_from_date,  -- Format changed for V 0.02
           MT.SERVICECOMPONENTTYPE,
           DECODE(MT.SERVICECOMPONENTTYPE,'MPW','Metered Potable Water',
                                          'MNPW','Metered Non-Potable Water',
                                          'UW','Unmeasured Water',
                                          'AS','Assessed Sewerage',
                                          'US','Unmeasured Sewerage',
                                          'HD','Highway Drainage',
                                          'AW','Assessed Water',
                                          'TE','Trade Effluent',
                                          'SW','Surface Water Drainage',
                                          'MS','Metered Sewerage',
                                          'SCA','Charge Adjustment Sewerage',  -- Added for V 0.02
                                          'WCA','Charge Adjustment Water',    -- Added for V 0.02
                                          'Not Known') service_component
    FROM MOUTRAN.MO_TARIFF MT
    WHERE MT.TARIFFCODE_PK LIKE '1STW%'    -- V 0.10
    ORDER BY MT.TARIFFCODE_PK;

  -- V 0.12 - tariff version related data - accommodates seasonal tariffs with multiple versions
  CURSOR cur_tariff_version (p_tariff_code VARCHAR2) IS
    SELECT MTV.TARIFF_VERSION_PK,
           TO_CHAR (MTV.TARIFFVEREFFECTIVEFROMDATE,'YYYY-MM-DD') effective_from_date,  -- Format changed for V 0.02
           MTV.STATE
    FROM MOUTRAN.MO_TARIFF MT,
         MOUTRAN.MO_TARIFF_VERSION MTV
    WHERE MT.TARIFFCODE_PK = MTV.TARIFFCODE_PK
    AND (MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION
                             WHERE TARIFFCODE_PK = MT.TARIFFCODE_PK
                             AND TARIFFCOMPONENTTYPE = MT.SERVICECOMPONENTTYPE)
         OR MT.SEASONALFLAG = 'Y') -- V 0.11
    AND MT.TARIFFCODE_PK LIKE '1STW%'    -- V 0.10
    AND MT.TARIFFCODE_PK = p_tariff_code
    ORDER BY MT.SERVICECOMPONENTTYPE,MTV.TARIFFVEREFFECTIVEFROMDATE;

  l_xmltype XMLTYPE;
  l_filepath VARCHAR2(30) := 'DELEXPORT';
  l_filename VARCHAR2(100);
  l_loop_count NUMBER := 0;
BEGIN
   -- Initial batch processing variables
   g_progress := 'Start';
   g_err.TXT_DATA := c_module_name;
   g_err.TXT_KEY := 0;
   g_job.NO_INSTANCE := 0;
   g_no_row_read := 0;
   g_no_row_insert := 0;
   g_no_row_dropped := 0;
   g_no_row_war := 0;
   g_no_row_err := 0;
   g_no_row_exp := 0;
   g_job.IND_STATUS := 'RUN';

   IF USER = 'FINDEL' THEN
      l_filepath := 'FINEXPORT';
   END IF;

   -- get job no and start job
   P_MIG_BATCH.FN_STARTJOB(no_batch, no_job, c_module_name,
                         g_job.NO_INSTANCE,
                         g_job.ERR_TOLERANCE,
                         g_job.EXP_TOLERANCE,
                         g_job.WAR_TOLERANCE,
                         g_job.NO_COMMIT,
                         g_job.NO_STREAM,
                         g_job.NO_RANGE_MIN,
                         g_job.NO_RANGE_MAX,
                         g_job.IND_STATUS);

   COMMIT;

   g_progress := 'processing ';

   -- any errors set return code and exit out
   IF g_job.IND_STATUS = 'ERR' THEN
      P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
      return_code := -1;
      RETURN;
   END IF;

  -- run pre-validation checks - any failures result in no file production
  P_DEL_VALIDATION_CHECKS(no_batch, no_job, return_code);
  IF g_job.IND_STATUS = 'ERR' THEN
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
    RETURN;
  END IF;

  g_progress := 'creating empty XML document ';

  -- Create an empty XML document
  g_domdoc := dbms_xmldom.newDomDocument;
  dbms_xmldom.setversion(g_domdoc,g_root);

  -- Create a root node
  g_root_node := dbms_xmldom.makeNode(g_domdoc);

  -- Create a new node tarifflist and add it to the root node
  g_tariff_list_node := dbms_xmldom.appendChild(g_root_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff_list)));-- V 0.09
  g_tariff_list_attribute := dbms_xmldom.createAttribute(g_domdoc,g_tariff_list_attr1);-- V 0.09
  g_tariff_list_attr_node := dbms_xmldom.appendChild(g_tariff_list_node, dbms_xmldom.makeNode(g_tariff_list_attribute));-- V 0.09
  dbms_xmldom.setvalue(g_tariff_list_attribute,g_tariff_list_attr_txt1);-- V 0.09

  g_tariff_list_attribute := dbms_xmldom.createAttribute(g_domdoc,g_tariff_list_attr2);-- V 0.09
  g_tariff_list_attr_node := dbms_xmldom.appendChild(g_tariff_list_node, dbms_xmldom.makeNode(g_tariff_list_attribute));-- V 0.09
  dbms_xmldom.setvalue(g_tariff_list_attribute,g_tariff_list_attr_txt2);-- V 0.09

  g_tariff_list_attribute := dbms_xmldom.createAttribute(g_domdoc,g_tariff_list_attr3);-- V 0.09
  g_tariff_list_attr_node := dbms_xmldom.appendChild(g_tariff_list_node, dbms_xmldom.makeNode(g_tariff_list_attribute));-- V 0.09
  dbms_xmldom.setvalue(g_tariff_list_attribute,g_tariff_list_attr_txt3);-- V 0.09

  FOR t IN cur_tariff
  LOOP
    IF L_LOOP_COUNT = 0 THEN
        -- Standing Data
       g_progress := 'Standing Data';
       g_tariff_node := dbms_xmldom.appendChild(g_tariff_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff)));

       -- Each tariff has servicecomponent, name, code and effectivefrom nodes to contain the tariff header data
       g_service_component_node := CreateXMLTag(g_domdoc,g_tariff_node,g_service_component,'Standing Data');
       g_name_node := CreateXMLTag(g_domdoc,g_tariff_node,g_name,'Standing Data STW');  -- these values are hard coded because Standing Data does not conform -- V 0.04
       g_code_node := CreateXMLTag(g_domdoc,g_tariff_node,g_code,'SD_STW');             -- to "normal" service component structures -- V 0.04
       g_effective_from_node := CreateXMLTag(g_domdoc,g_tariff_node,g_effective_from,t.effective_from_date); -- V 0.04

       -- For each tariff we now need the tariffdatalist
       g_tariff_data_list_node := dbms_xmldom.appendChild(g_tariff_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff_data_list)));
       g_tariff_data_node := dbms_xmldom.appendChild(g_tariff_data_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff_data)));

       -- Each tariffdata element has effectivefrom and state nodes to contain the tariffdata header data
       g_effective_from_node := CreateXMLTag(g_domdoc,g_tariff_data_node,g_effective_from,t.effective_from_date); -- V 0.04
       g_state_node := CreateXMLTag(g_domdoc,g_tariff_data_node,g_state,'Verified'); -- V 0.04 (hard coded state for standing data since this data not in main tariff table

       g_charge_element_list_node := dbms_xmldom.appendChild(g_tariff_data_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element_list)));

       P_DEL_TARIFF_EXPORT_SD(no_batch, no_job, return_code);  -- Added for V 0.02

       l_loop_count := l_loop_count + 1;
    END IF;

    g_progress := 'collating tariff data for ' || t.SERVICECOMPONENTTYPE;

    g_tariff_node := dbms_xmldom.appendChild(g_tariff_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff)));

    -- Each tariff has servicecomponent, name, code and effectivefrom nodes to contain the tariff header data
    g_service_component_node := CreateXMLTag(g_domdoc,g_tariff_node,g_service_component,t.service_component);
    g_name_node := CreateXMLTag(g_domdoc,g_tariff_node,g_name,t.name);
    g_code_node := CreateXMLTag(g_domdoc,g_tariff_node,g_code,t.code);
    g_effective_from_node := CreateXMLTag(g_domdoc,g_tariff_node,g_effective_from,t.effective_from_date);

    -- For each tariff we now need the tariffdatalist
    g_tariff_data_list_node := dbms_xmldom.appendChild(g_tariff_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff_data_list)));

    -- V 0.12 for each tariff version we need to do the following
    FOR tv IN cur_tariff_version(t.code)
    LOOP
      g_tariff_data_node := dbms_xmldom.appendChild(g_tariff_data_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_tariff_data)));

      -- Each tariffdata element has effectivefrom and state nodes to contain the tariffdata header data
      g_effective_from_node := CreateXMLTag(g_domdoc,g_tariff_data_node,g_effective_from,tv.effective_from_date);
      g_state_node := CreateXMLTag(g_domdoc,g_tariff_data_node,g_state,tv.state);

      -- Charge element list - V 0.03
      g_charge_element_list_node := dbms_xmldom.appendChild(g_tariff_data_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element_list)));

       -- Make calls to individual service component
       IF t.SERVICECOMPONENTTYPE = 'MPW' THEN  -- METERED POTABLE WATER
        P_DEL_TARIFF_EXPORT_MPW(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'MNPW' THEN  -- METERED NON-POTABLE WATER
        P_DEL_TARIFF_EXPORT_MNPW(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'UW' THEN  -- UNMEASURED WATER
        P_DEL_TARIFF_EXPORT_UW(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'AS' THEN  -- ASSESSED SEWERAGE
        P_DEL_TARIFF_EXPORT_AS(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'US' THEN  -- UNMEASURED SEWERAGE
        P_DEL_TARIFF_EXPORT_US(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'HD' THEN  -- HIGHWAY DRAINAGE
        P_DEL_TARIFF_EXPORT_HD(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'AW' THEN  -- ASSESSED WATER
        P_DEL_TARIFF_EXPORT_AW(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'TE' THEN  -- TRADE EFFLUENT
        P_DEL_TARIFF_EXPORT_TE(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'SW' THEN  -- SURFACE WATER DRAINAGE
        P_DEL_TARIFF_EXPORT_SW(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'MS' THEN  -- MEASURED SEWERAGE
        P_DEL_TARIFF_EXPORT_MS(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);
       ELSIF t.SERVICECOMPONENTTYPE = 'WCA' THEN  -- CHARGE ADJUSTMENT WATER
        P_DEL_TARIFF_EXPORT_WCA(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);  -- Added for V 0.02
       ELSIF t.SERVICECOMPONENTTYPE = 'SCA' THEN  -- CHARGE ADJUSTMENT SEWERAGE
        P_DEL_TARIFF_EXPORT_SCA(tv.TARIFF_VERSION_PK, no_batch, no_job, return_code);  -- Added for V 0.02
       END IF;
    END LOOP; -- cur_tariff_version
  END LOOP; -- cur_tariff

  l_filename := l_filepath || '/TARIFF_EXPORT_SEVERN-W_' || TO_CHAR(SYSDATE,'YYYYMMDDHH24MISS') || '.xml';
  g_progress := 'writing XML to file ' || l_filename;

  dbms_output.put_line('exporting file ' || l_filename);
  l_xmltype := dbms_xmldom.getXmlType(g_domdoc);
  dbms_xmldom.writetofile(g_domdoc,l_filename);
  dbms_xmldom.freeDocument(g_domdoc);
--  dbms_output.put_line(l_xmltype.getClobVal);

  P_MIG_BATCH.FN_RECONLOG(no_batch, g_job.NO_INSTANCE, 'CP45', 2420, g_no_row_read,    'Distinct Tariffs read from MO_TARIFF');
  P_MIG_BATCH.FN_RECONLOG(no_batch, g_job.NO_INSTANCE, 'CP45', 2430, g_no_row_dropped, 'Tariffs dropped during Export');
  P_MIG_BATCH.FN_RECONLOG(no_batch, g_job.NO_INSTANCE, 'CP45', 2440, g_no_row_insert,  'Tariffs written to ' || l_filename);

  g_job.IND_STATUS := 'END';
  P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
  g_progress := 'End ';

  COMMIT;
END P_DEL_TARIFF_EXPORT_MAIN;
----------------------------------------------------------------------------------------
-- FUNCTION SPECIFICATION: Wholesaler Tariff XML Export - Pre-export validation checks
-- AUTHOR         : Kevin Burton
-- CREATED        : 09/05/2016
-- DESCRIPTION    : Validates if all the pre-req tariff data is present before exporting
--                  to file - otherwise outputs and error
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_VALIDATION_CHECKS (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER) AS
  l_premtolfactor NUMBER;
  l_dailystdbyusagevol NUMBER;
  l_dailypremusagevol NUMBER;
  l_comband VARCHAR2(16);
  l_count NUMBER;
  l_ra NUMBER;
  l_va NUMBER;
  l_bva NUMBER;
  l_ma NUMBER;
  l_ba NUMBER;
  l_sa NUMBER;
  l_aa NUMBER;
  l_xa NUMBER;
  l_ya NUMBER;
  l_za NUMBER;
  l_vo NUMBER;
  l_bvo NUMBER;
  l_mo NUMBER;
  l_so NUMBER;
  l_ao NUMBER;
  l_os NUMBER;
  l_ss NUMBER;
  l_as NUMBER;
  l_am NUMBER;
  l_xo NUMBER;
  l_yo NUMBER;
  l_zo NUMBER;
  l_xs NUMBER;
  l_ys NUMBER;
  l_zs NUMBER;
  l_xm NUMBER;
  l_ym NUMBER;
  l_zm NUMBER;
  l_robt NUMBER;
  l_bobt NUMBER;

  CURSOR tariff_type_cur IS
    SELECT DISTINCT TARIFF_TYPE_PK, TARIFFCODE_PK, TARIFFCOMPONENTTYPE
    FROM (SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, MPW.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_MPW MPW,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE MPW.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK)
          UNION
          SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, MNPW.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE MNPW.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK)
          UNION
          SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, MS.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_MS MS,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE MS.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK)
          UNION
          SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, SW.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_SW SW,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE SW.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK)
          UNION
          SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, HD.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_HD HD,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE HD.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK)
          UNION
          SELECT MTV.TARIFF_VERSION_PK, MTV.TARIFFCODE_PK, TE.TARIFF_TYPE_PK, MTV.TARIFFCOMPONENTTYPE
          FROM MOUTRAN.MO_TARIFF_TYPE_TE TE,
               MOUTRAN.MO_TARIFF_VERSION MTV
          WHERE TE.TARIFF_VERSION_PK = MTV.TARIFF_VERSION_PK
          AND MTV.TARIFFVERSION = (SELECT MAX(TARIFFVERSION) FROM MOUTRAN.MO_TARIFF_VERSION WHERE TARIFFCODE_PK = MTV.TARIFFCODE_PK));

  CURSOR mpw_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_MWMFC_PK,
           TARIFF_TYPE_PK,
           LOWERMETERSIZE,
           UPPERMETERSIZE,
           CHARGE
    FROM MOUTRAN.MO_MPW_METER_MWMFC
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERMETERSIZE,UPPERMETERSIZE;

  CURSOR mnpw_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_MWMFC_PK,
           TARIFF_TYPE_PK,
           LOWERMETERSIZE,
           UPPERMETERSIZE,
           CHARGE
    FROM MOUTRAN.MO_MNPW_METER_MWMFC
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERMETERSIZE,UPPERMETERSIZE;

  CURSOR ms_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_MSMFC_PK,
           TARIFF_TYPE_PK,
           LOWERMETERSIZE,
           UPPERMETERSIZE,
           CHARGE
    FROM MOUTRAN.MO_MS_METER_MSMFC
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERMETERSIZE,UPPERMETERSIZE;
  
  -- V 0.14  
  CURSOR sw_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_SWMFC_PK,
           TARIFF_TYPE_PK,
           LOWERMETERSIZE,
           UPPERMETERSIZE,
           CHARGE
    FROM MOUTRAN.MO_SW_METER_SWMFC
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERMETERSIZE,UPPERMETERSIZE;

  CURSOR swab_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_AREA_BAND_PK,
           TARIFF_TYPE_PK,
           LOWERAREA,
           UPPERAREA,
           BAND
    FROM MOUTRAN.MO_SW_AREA_BAND
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERAREA,UPPERAREA;
    
  CURSOR hd_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_HDMFC_PK,
           TARIFF_TYPE_PK,
           LOWERMETERSIZE,
           UPPERMETERSIZE,
           CHARGE
    FROM MOUTRAN.MO_HD_METER_HDMFC
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERMETERSIZE,UPPERMETERSIZE;   
    
  CURSOR hdab_cur (p_tariff_type NUMBER) IS
    SELECT TARIFF_AREA_BAND_PK,
           TARIFF_TYPE_PK,
           LOWERAREA,
           UPPERAREA,
           BAND
    FROM MOUTRAN.MO_HD_AREA_BAND
    WHERE TARIFF_TYPE_PK = p_tariff_type
    ORDER BY LOWERAREA,UPPERAREA;    
BEGIN
  FOR tt IN tariff_type_cur
  LOOP
    -- check that for all Metered Fixed Charge tariffs the first row of the lookup table
    -- contains all zero values
    IF tt.TARIFFCOMPONENTTYPE = 'MPW' THEN
      FOR t IN mpw_cur(tt.TARIFF_TYPE_PK)
      LOOP
        IF mpw_cur%ROWCOUNT = 1 THEN
          IF NOT (t.LOWERMETERSIZE = 0 AND t.UPPERMETERSIZE = 0 AND t.CHARGE = 0) THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Meter Fixed Charges data is missing zero value first row',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
        END IF;
      END LOOP;

      -- check that for standby capacity charges we have provided mandatory single data
      SELECT COUNT(*)
      INTO l_count
      FROM MOUTRAN.MO_MPW_STANDBY_MWCAPCHG
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF l_count > 0 THEN
        SELECT MPWPREMIUMTOLFACTOR,
               MPWDAILYSTANDBYUSAGEVOLCHARGE,
               MPWDAILYPREMIUMUSAGEVOLCHARGE
        INTO l_premtolfactor,
             l_dailystdbyusagevol,
             l_dailypremusagevol
        FROM MOUTRAN.MO_TARIFF_TYPE_MPW
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

        IF l_premtolfactor IS NULL OR l_dailystdbyusagevol IS NULL OR l_dailypremusagevol IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Standby Usage Charges must be provided if Standby Capacity is not none',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
      END IF;
    ELSIF tt.TARIFFCOMPONENTTYPE = 'MNPW' THEN
      FOR t IN mnpw_cur(tt.TARIFF_TYPE_PK)
      LOOP
        IF mnpw_cur%ROWCOUNT = 1 THEN
          IF NOT (t.LOWERMETERSIZE = 0 AND t.UPPERMETERSIZE = 0 AND t.CHARGE = 0) THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Meter Fixed Charges data is missing zero value first row',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
        END IF;
      END LOOP;

      -- check that for standby capacity charges we have provided mandatory single data
      SELECT COUNT(*)
      INTO l_count
      FROM MOUTRAN.MO_MNPW_STANDBY_MWCAPCHG
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF l_count > 0 THEN
        SELECT MNPWPREMIUMTOLFACTOR,
               MNPWDAILYSTANDBYUSAGEVOLCHARGE,
               MNPWDAILYPREMIUMUSAGEVOLCHARGE
        INTO l_premtolfactor,
             l_dailystdbyusagevol,
             l_dailypremusagevol
        FROM MOUTRAN.MO_TARIFF_TYPE_MNPW
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;
      END IF;

        IF l_premtolfactor IS NULL OR l_dailystdbyusagevol IS NULL OR l_dailypremusagevol IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Standby Usage Charges must be provided if Standby Capacity is not none',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
    ELSIF tt.TARIFFCOMPONENTTYPE = 'MS' THEN
      FOR t IN ms_cur(tt.TARIFF_TYPE_PK)
      LOOP
        IF ms_cur%ROWCOUNT = 1 THEN
          IF NOT (t.LOWERMETERSIZE = 0 AND t.UPPERMETERSIZE = 0 AND t.CHARGE = 0) THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Meter Fixed Charges data is missing zero value first row',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
        END IF;
      END LOOP;
    ELSIF tt.TARIFFCOMPONENTTYPE = 'SW' THEN
    -- check that for all metered drainage tariffs the first row of the lookup tables
    -- contains all zero values
      FOR t IN sw_cur(tt.TARIFF_TYPE_PK)
      LOOP
        IF sw_cur%ROWCOUNT = 1 THEN
          IF NOT (t.LOWERMETERSIZE = 0 AND t.UPPERMETERSIZE = 0 AND t.CHARGE = 0) THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Meter Fixed Charges data is missing zero value first row',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
        END IF;
      END LOOP;
      
      -- check if there are any Area Bands defined for this tariff and if there are we must also have Band Charges
      -- and community band
      SELECT COUNT(*)
      INTO l_count
      FROM MOUTRAN.MO_SW_AREA_BAND
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF l_count > 0 THEN
        -- check that the first row for area band lookup table contains zero as first entry
        FOR t IN swab_cur(tt.TARIFF_TYPE_PK)
        LOOP
          IF swab_cur%ROWCOUNT = 1 THEN
            IF NOT t.LOWERAREA = 0 THEN
              P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' SW Area Band data is missing zero value first record',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
              return_code := -1;
              g_job.IND_STATUS := 'ERR';
            END IF;
          END IF;
        END LOOP;
      
        SELECT COUNT(*)
        INTO l_count
        FROM MOUTRAN.MO_SW_BAND_CHARGE
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

        IF l_count = 0 THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Surface Water Band Charge required if Area Band provided',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        SELECT SWCOMBAND
        INTO l_comband
        FROM MOUTRAN.MO_TARIFF_TYPE_SW
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

        IF l_comband IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Surface Water Community Band required if Area Band provided',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
      END IF;
    ELSIF tt.TARIFFCOMPONENTTYPE = 'HD' THEN
    -- check that for all metered drainage tariffs the first row of the lookup tables
    -- contains all zero values
      FOR t IN hd_cur(tt.TARIFF_TYPE_PK)
      LOOP
        IF hd_cur%ROWCOUNT = 1 THEN
          IF NOT (t.LOWERMETERSIZE = 0 AND t.UPPERMETERSIZE = 0 AND t.CHARGE = 0) THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' Meter Fixed Charges data is missing zero value first row',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
        END IF;
      END LOOP;      -- check if there are any Area Bands defined for this tariff and if there are we must also have Band Charges
      -- and community band
      SELECT COUNT(*)
      INTO l_count
      FROM MOUTRAN.MO_HD_AREA_BAND
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF l_count > 0 THEN
        FOR t IN hdab_cur(tt.TARIFF_TYPE_PK)
        LOOP
          IF hdab_cur%ROWCOUNT = 1 THEN
            IF NOT t.LOWERAREA = 0 THEN
              P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || ' HD Area Band data is missing zero value first record',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
              return_code := -1;
              g_job.IND_STATUS := 'ERR';
            END IF;
          END IF;
        END LOOP;
        
        SELECT COUNT(*)
        INTO l_count
        FROM MOUTRAN.MO_HD_BAND_CHARGE
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

        IF l_count = 0 THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Surface Water Band Charge required if Area Band provided',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        SELECT HDCOMBAND
        INTO l_comband
        FROM MOUTRAN.MO_TARIFF_TYPE_HD
        WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

        IF l_comband IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Surface Water Community Band required if Area Band provided',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
      END IF;
    ELSIF tt.TARIFFCOMPONENTTYPE = 'TE' THEN
      -- For TE there are three different checks to pass
      -- First check capacity charges this part could also be a trigger on MO_TARIFF_TYPE_TE table
      SELECT TECHARGECOMPRA,
             TECHARGECOMPVA,
             TECHARGECOMPBVA,
             TECHARGECOMPMA,
             TECHARGECOMPBA,
             TECHARGECOMPSA,
             TECHARGECOMPAA,
             TECHARGECOMPXA,
             TECHARGECOMPYA,
             TECHARGECOMPZA,
             TECHARGECOMPVO,
             TECHARGECOMPBVO,
             TECHARGECOMPMO,
             TECHARGECOMPSO,
             TECHARGECOMPAO,
             TECHARGECOMPOS,
             TECHARGECOMPSS,
             TECHARGECOMPAS,
             TECHARGECOMPAM,
             TECHARGECOMPXO,
             TECHARGECOMPYO,
             TECHARGECOMPZO,
             TECHARGECOMPXS,
             TECHARGECOMPYS,
             TECHARGECOMPZS,
             TECHARGECOMPXM,
             TECHARGECOMPYM,
             TECHARGECOMPZM
      INTO l_ra,
           l_va,
           l_bva,
           l_ma,
           l_ba,
           l_sa,
           l_aa,
           l_xa,
           l_ya,
           l_za,
           l_vo,
           l_bvo,
           l_mo,
           l_so,
           l_ao,
           l_os,
           l_ss,
           l_as,
           l_am,
           l_xo,
           l_yo,
           l_zo,
           l_xs,
           l_ys,
           l_zs,
           l_xm,
           l_ym,
           l_zm
      FROM MOUTRAN.MO_TARIFF_TYPE_TE
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF (l_ra IS NOT NULL OR
          l_va IS NOT NULL OR
          l_bva IS NOT NULL OR
          l_ma IS NOT NULL OR
          l_ba IS NOT NULL OR
          l_sa IS NOT NULL OR
          l_aa IS NOT NULL OR
          l_xa IS NOT NULL OR
          l_ya IS NOT NULL OR
          l_za IS NOT NULL) THEN

        IF l_ra IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Reception Capacity Charging Component (Ra) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_va IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Volumetric Capacity Charging Component (Va) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_bva IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Additional Volumetric Capacity Charging Component (Bva) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_ma IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Marine Treatment Capacity Charging Component (Ma) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_ba IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Biological Capacity Charging Component (Ba) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_sa IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Sludge Capacity Charging Component (Sa) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_aa IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Ammonia Capacity Charging Component (Aa) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
      END IF;
      -- Second check charging components - most of this could be a trigger on MO_TARIFF_TYPE_TE
      -- except the checks on RoBT and BoBT which is more tricky because it's a block tariff table
      SELECT COUNT(*)
      INTO l_robt
      FROM MOUTRAN.MO_TE_BLOCK_ROBT
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      SELECT COUNT(*)
      INTO l_bobt
      FROM MOUTRAN.MO_TE_BLOCK_BOBT
      WHERE TARIFF_TYPE_PK = tt.TARIFF_TYPE_PK;

      IF (l_robt > 0 OR
          l_vo IS NOT NULL OR
          l_bvo IS NOT NULL OR
          l_mo IS NOT NULL OR
          l_so IS NOT NULL OR
          l_ao IS NOT NULL OR
          l_os IS NOT NULL OR
          l_ss IS NOT NULL OR
          l_as IS NOT NULL OR
          l_am IS NOT NULL OR
          l_xo IS NOT NULL OR
          l_yo IS NOT NULL OR
          l_zo IS NOT NULL OR
          l_xs IS NOT NULL OR
          l_ys IS NOT NULL OR
          l_zs IS NOT NULL OR
          l_xm IS NOT NULL OR
          l_ym IS NOT NULL OR
          l_zm IS NOT NULL) THEN

          IF l_robt = 0 THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Reception Block Tariff Component (RoBT) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_vo IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Volumetric Charging Component (Vo) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_bvo IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Additional Volumetric Charging Component (Bvo) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_mo IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Marine Treatment Charging Component (Mo) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_bobt = 0 THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Secondary Treatment Block Tariff Component (BoBT) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_so IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Sludge Treatment Charging Component (So) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_ao IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Ammoniacal Nitrogen Charging Component (Ao) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_os IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Chemical Oxygen Demand Base Value (Os) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_ss IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Suspended Solids Base Value (Ss) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_as IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Ammoniacal Nitrogen Base Value (As) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;

          IF l_am IS NULL THEN
            P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Min Val of Ammoniacal Nitrogen content charged (Am) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
            return_code := -1;
            g_job.IND_STATUS := 'ERR';
          END IF;
      END IF;
      -- Third check TE Componentcapacity charges this part could also be a trigger on MO_TARIFF_TYPE_TE table
      IF (l_ra IS NOT NULL OR
          l_va IS NOT NULL OR
          l_bva IS NOT NULL OR
          l_ma IS NOT NULL OR
          l_ba IS NOT NULL OR
          l_sa IS NOT NULL OR
          l_aa IS NOT NULL) THEN

        IF l_xa IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Trade Effluent Component X Capacity Charging Component (Xa) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_ya IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Trade Effluent Component Y Capacity Charging Component (Ya) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;

        IF l_za IS NULL THEN
          P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Error: ' || tt.TARIFFCODE_PK || 'Trade Effluent Component Z Capacity Charging Component (Za) required',  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
          return_code := -1;
          g_job.IND_STATUS := 'ERR';
        END IF;
      END IF;
    END IF; -- tariff component type
  END LOOP; -- tariff type cur
END P_DEL_VALIDATION_CHECKS;

----------------------------------------------------------------------------------------
-- FUNCTION SPECIFICATION: Wholesaler Tariff XML Export - CreateXMLTag
-- AUTHOR         : Kevin Burton
-- CREATED        : 31/03/2016
-- DESCRIPTION    : Creates an XML tag and the text value beneath a specified parent node
-----------------------------------------------------------------------------------------
FUNCTION CreateXMLTag(v_doc dbms_xmldom.DOMDocument,
                      v_parent dbms_xmldom.DOMNode,
                      v_tag VARCHAR2,
                      v_tag_text VARCHAR2) RETURN dbms_xmldom.DOMNode IS
  l_xml_node dbms_xmldom.DOMNode;
  l_xml_node_text dbms_xmldom.DOMNode;
BEGIN
    l_xml_node := dbms_xmldom.appendChild(v_parent, dbms_xmldom.makeNode(dbms_xmldom.createElement(v_doc, v_tag)));
    l_xml_node_text := dbms_xmldom.appendChild(l_xml_node, dbms_xmldom.makeNode(dbms_xmldom.createTextNode(v_doc, v_tag_text)));

    RETURN l_xml_node_text;
END CreateXMLTag;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - SingleChargeElement
-- AUTHOR         : Kevin Burton
-- CREATED        : 31/03/2016
-- DESCRIPTION    : Outputs the XML for single charge elements
-----------------------------------------------------------------------------------------
PROCEDURE SingleChargeElement(v_element_name VARCHAR2,
                              v_field_name VARCHAR2,
                              v_field_value VARCHAR2) IS
  l_applicable VARCHAR2(10) := 'true';  -- Added for V 0.02
BEGIN
  IF v_field_value IS NULL THEN  -- Added for V 0.02
    l_applicable := 'false';
  END IF;

  IF l_applicable = 'true' THEN  -- Moved for V 0.03
--  g_charge_element_list_node := dbms_xmldom.appendChild(g_tariff_data_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element_list)));  -- Charge element list - V 0.03
  g_charge_element_node := dbms_xmldom.appendChild(g_charge_element_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element)));

  -- Each charge element has name and isapplicable flag
  g_name_node := CreateXMLTag(g_domdoc,g_charge_element_node,g_name,v_element_name);
  g_is_applicable_node := CreateXMLTag(g_domdoc,g_charge_element_node,g_is_applicable,l_applicable);

--  IF l_applicable = 'true' THEN  -- Added for V 0.02
    -- For each charge element we have one or more fields
    g_field_list_node := dbms_xmldom.appendChild(g_charge_element_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_field_list)));
    g_field_node := dbms_xmldom.appendChild(g_field_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_field)));

    -- For single values the field node has name and value data
    g_name_node := CreateXMLTag(g_domdoc,g_field_node,g_name,v_field_name);
    g_value_node := CreateXMLTag(g_domdoc,g_field_node,g_value,v_field_value);
  END IF;
END SingleChargeElement;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - MultiChargeElementHeader
-- AUTHOR         : Kevin Burton
-- CREATED        : 31/03/2016
-- DESCRIPTION    : Outputs the header XML for multiple charge elements
-----------------------------------------------------------------------------------------
PROCEDURE MultiChargeElementHeader(v_element_name VARCHAR2,
                                   v_applicable VARCHAR2,
                                   v_field_name VARCHAR2) IS
BEGIN
  IF v_applicable = 'true' THEN  -- Added for V 0.03
--  g_charge_element_list_node := dbms_xmldom.appendChild(g_tariff_data_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element_list))); -- Charge element list - V 0.03
  g_charge_element_node := dbms_xmldom.appendChild(g_charge_element_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_charge_element)));

  -- Each charge element has name and isapplicable flag
  g_name_node := CreateXMLTag(g_domdoc,g_charge_element_node,g_name,v_element_name);
  g_is_applicable_node := CreateXMLTag(g_domdoc,g_charge_element_node,g_is_applicable,v_applicable);

--  IF v_applicable = 'true' THEN  -- Added for V 0.02
    -- For each charge element we have one or more fields
    g_field_list_node := dbms_xmldom.appendChild(g_charge_element_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_field_list)));
    g_field_node := dbms_xmldom.appendChild(g_field_list_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_field)));

    -- in the multiple case the field elements have a name and then the additional rows list to accomodate the values
    g_name_node := CreateXMLTag(g_domdoc,g_field_node,g_name,v_field_name);

    g_rows_node := dbms_xmldom.appendChild(g_field_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_rows)));
  END IF;
END MultiChargeElementHeader;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - SetNilTag
-- AUTHOR         : Kevin Burton
-- CREATED        : 29/04/2016
-- DESCRIPTION    : Outputs the NIL column value XML for multiple charge elements
-----------------------------------------------------------------------------------------
PROCEDURE SetNilTag(v_col_val_node VARCHAR2) IS
  l_column_value_node dbms_xmldom.DOMNode;
  l_column_value_node_text dbms_xmldom.DOMNode;
BEGIN
  l_column_value_node := dbms_xmldom.appendChild(g_row_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, v_col_val_node)));
  g_col_nil_attribute := dbms_xmldom.createAttribute(g_domdoc,g_nil_attr);
  g_col_nil_attr_node := dbms_xmldom.appendChild(l_column_value_node, dbms_xmldom.makeNode(g_col_nil_attribute));
  dbms_xmldom.setvalue(g_col_nil_attribute,g_nil_attr_txt);
END SetNilTag;
----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - MultiChargeElementColVals
-- AUTHOR         : Kevin Burton
-- CREATED        : 31/03/2016
-- DESCRIPTION    : Outputs the column value XML for multiple charge elements
-----------------------------------------------------------------------------------------
PROCEDURE MultiChargeElementColVals(v_col_val_node VARCHAR2,
                                    v_col_val_node_text VARCHAR2) IS
  l_column_value_node dbms_xmldom.DOMNode;
  l_column_value_node_text dbms_xmldom.DOMNode;
BEGIN
  l_column_value_node := CreateXMLTag(g_domdoc,g_row_node,v_col_val_node,v_col_val_node_text);
END MultiChargeElementColVals;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - METERED POTABLE WATER (MPW)
-- AUTHOR         : Kevin Burton
-- CREATED        : 31/03/2016
-- DESCRIPTION    : Procedure to export tariff data for MPW Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_MPW (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_mwspfc        NUMBER(9,2); --(D7102 - MWSPFC)
  l_premtolfactor NUMBER(5,2); --(D7105 - PremTolFactor
  l_mwdsuvc       NUMBER(9,2); --(D7106 - MWDSUVC)
  l_mwdpuvc       NUMBER(9,2); --(D7107 - MWDPUVC)
  l_mwmdt         NUMBER(9,2); --(D7108 - MWMDT)

  CURSOR cur_mwmfc (v_tariff_version NUMBER) IS
    SELECT MWMFC.LOWERMETERSIZE column1value,
           MWMFC.UPPERMETERSIZE column2value,
           RTRIM(TO_CHAR(MWMFC.CHARGE,'FM9999999990.99'),'.') column3value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MPW_METER_MWMFC MWMFC,
         MOUTRAN.MO_TARIFF_TYPE_MPW MPW
    WHERE MWMFC.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
    AND MPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_mwbt (v_tariff_version NUMBER) IS
    SELECT MWBT.UPPERANNUALVOL column1value,
           RTRIM(TO_CHAR(MWBT.CHARGE,'FM9999999990.99'),'.') column2value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MPW_BLOCK_MWBT MWBT,
         MOUTRAN.MO_TARIFF_TYPE_MPW MPW
    WHERE MWBT.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
    AND MPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_mwcapchg (v_tariff_version NUMBER) IS
    SELECT MWCAPCHG.RESERVATIONVOLUME column1value,
           RTRIM(TO_CHAR(MWCAPCHG.CHARGE,'FM9999999990.99'),'.') column2value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MPW_STANDBY_MWCAPCHG MWCAPCHG,
         MOUTRAN.MO_TARIFF_TYPE_MPW MPW
    WHERE MWCAPCHG.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
    AND MPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_MPW - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT MPW.MPWSUPPLYPOINTFIXEDCHARGES,     -- Supply Point Fixed Charges (D7102 - MWSPFC)
         MPW.MPWPREMIUMTOLFACTOR,            -- Standby Usage Charges (D7105 - PremTolFactor)
         MPW.MPWDAILYSTANDBYUSAGEVOLCHARGE,  -- Standby Usage Charges (D7106 - MWDSUVC)
         MPW.MPWDAILYPREMIUMUSAGEVOLCHARGE,  -- Standby Usage Charges (D7107 -MWDPVUC)
         MPW.MPWMAXIMUMDEMANDTARIFF          -- Maximum Demand Charges (D7108 - MWMDT)
  INTO l_mwspfc,
       l_premtolfactor,
       l_mwdsuvc,
       l_mwdpuvc,
       l_mwmdt
  FROM MOUTRAN.MO_TARIFF_TYPE_MPW MPW
  WHERE MPW.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Supply Point Fixed Charges',
                        v_field_name => 'MWSPFC',
                        v_field_value => l_mwspfc);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'PremTolFactor',
                        v_field_value => l_premtolfactor);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'MWDSUVC',
                        v_field_value => l_mwdsuvc);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'MWDPUVC',
                        v_field_value => l_mwdpuvc);

    SingleChargeElement(v_element_name => 'Maximum Demand Charges',
                        v_field_name => 'MWMDT',
                        v_field_value => l_mwmdt);

  -- Muliple charge elements
  -- Meter Fixed Charges (D7101 - MWMFC)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MPW_METER_MWMFC MWMFC,
       MOUTRAN.MO_TARIFF_TYPE_MPW MPW
  WHERE MWMFC.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
  AND MPW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'MWMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwmfc IN cur_mwmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => mwmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwmfc%ROWCOUNT < mwmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => mwmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => mwmfc.column3value);
    END LOOP;
  END IF;

  -- Metered Volumetric Charges (D7103 - MWBT)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MPW_BLOCK_MWBT MWBT,
       MOUTRAN.MO_TARIFF_TYPE_MPW MPW
  WHERE MWBT.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
  AND MPW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Metered Volumetric Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'MWBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwbt IN cur_mwbt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwbt%ROWCOUNT < mwbt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => mwbt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => mwbt.column2value);
    END LOOP;
  END IF;

  -- Standby Capacity Charges (D7104 - MWCapChg)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MPW_STANDBY_MWCAPCHG MWCAPCHG,
       MOUTRAN.MO_TARIFF_TYPE_MPW MPW
  WHERE MWCAPCHG.TARIFF_TYPE_PK = MPW.TARIFF_TYPE_PK
  AND MPW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Standby Capacity Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'MWCapChg');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwcapchg IN cur_mwcapchg(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwcapchg%ROWCOUNT < mwcapchg.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => mwcapchg.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => mwcapchg.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_MPW;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - METERED NON-POTABLE WATER (MNPW)
-- AUTHOR         : Kevin Burton
-- CREATED        : 05/04/2016
-- DESCRIPTION    : Procedure to export tariff data for MNPW Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_MNPW (v_tariff_version NUMBER,
                                    no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                    no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                    return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_mwspfc        NUMBER(9,2); --(D7152 - MWSPFC)
  l_premtolfactor NUMBER(5,2); --(D7155 - PremTolFactor
  l_mwdsuvc       NUMBER(9,2); --(D7156 - MWDSUVC)
  l_mwdpuvc       NUMBER(9,2); --(D7157 - MWDPUVC)
  l_mwmdt         NUMBER(9,2); --(D7158 - MWMDT)

  CURSOR cur_mwmfc (v_tariff_version NUMBER) IS
    SELECT MWMFC.LOWERMETERSIZE column1value,
           MWMFC.UPPERMETERSIZE column2value,
           RTRIM(TO_CHAR(MWMFC.CHARGE,'FM9999999990.99'),'.') column3value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MNPW_METER_MWMFC MWMFC,
         MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
    WHERE MWMFC.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
    AND MNPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_mwbt (v_tariff_version NUMBER) IS
    SELECT MWBT.UPPERANNUALVOL column1value,
           RTRIM(TO_CHAR(MWBT.CHARGE,'FM9999999990.99'),'.') column2value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MNPW_BLOCK_MWBT MWBT,
         MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
    WHERE MWBT.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
    AND MNPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_mwcapchg (v_tariff_version NUMBER) IS
    SELECT MWCAPCHG.RESERVATIONVOLUME column1value,
           RTRIM(TO_CHAR(MWCAPCHG.CHARGE,'FM9999999990.99'),'.') column2value,
           COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MNPW_STANDBY_MWCAPCHG MWCAPCHG,
         MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
    WHERE MWCAPCHG.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
    AND MNPW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_MNPW - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT MNPW.MNPWSUPPLYPOINTFIXEDCHARGE,      -- Supply Point Fixed Charges (D7152 - MWSPFC)
         MNPW.MNPWPREMIUMTOLFACTOR,            -- Standby Usage Charges (D7155 - PremTolFactor)
         MNPW.MNPWDAILYSTANDBYUSAGEVOLCHARGE,  -- Standby Usage Charges (D7156 - MWDSUVC)
         MNPW.MNPWDAILYPREMIUMUSAGEVOLCHARGE,  -- Standby Usage Charges (D7157 -MWDPVUC)
         MNPW.MNPWMAXIMUMDEMANDTARIFF          -- Maximum Demand Charges (D7158 - MWMDT)
  INTO l_mwspfc,
       l_premtolfactor,
       l_mwdsuvc,
       l_mwdpuvc,
       l_mwmdt
  FROM MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
  WHERE MNPW.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Supply Point Fixed Charges',
                        v_field_name => 'MWSPFC',
                        v_field_value => l_mwspfc);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'PremTolFactor',
                        v_field_value => l_premtolfactor);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'MWDSUVC',
                        v_field_value => l_mwdsuvc);

    SingleChargeElement(v_element_name => 'Standby Usage Charges',
                        v_field_name => 'MWDPUVC',
                        v_field_value => l_mwdpuvc);

    SingleChargeElement(v_element_name => 'Maximum Demand Charges',
                        v_field_name => 'MWMDT',
                        v_field_value => l_mwmdt);

  -- Muliple charge elements
  -- Meter Fixed Charges (D7101 - MWMFC)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MNPW_METER_MWMFC MWMFC,
       MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
  WHERE MWMFC.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
  AND MNPW.TARIFF_VERSION_PK = v_tariff_version;

  MultiChargeElementHeader(v_element_name => 'Meter Fixed Charges',
                           v_applicable => l_applicable,
                           v_field_name => 'MWMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwmfc IN cur_mwmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => mwmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwmfc%ROWCOUNT < mwmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => mwmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => mwmfc.column3value);
    END LOOP;
  END IF;

  -- Metered Volumetric Charges (D7103 - MWBT)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MNPW_BLOCK_MWBT MWBT,
       MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
  WHERE MWBT.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
  AND MNPW.TARIFF_VERSION_PK = v_tariff_version;

  MultiChargeElementHeader(v_element_name => 'Metered Volumetric Charges',
                           v_applicable => l_applicable,
                           v_field_name => 'MWBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwbt IN cur_mwbt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwbt%ROWCOUNT < mwbt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => mwbt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => mwbt.column2value);
    END LOOP;
  END IF;

  -- Standby Capacity Charges (D7104 - MWCapChg)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_MPW_STANDBY_MWCAPCHG MWCAPCHG,
       MOUTRAN.MO_TARIFF_TYPE_MNPW MNPW
  WHERE MWCAPCHG.TARIFF_TYPE_PK = MNPW.TARIFF_TYPE_PK
  AND MNPW.TARIFF_VERSION_PK = v_tariff_version;

  MultiChargeElementHeader(v_element_name => 'Standby Capacity Charges',
                           v_applicable => l_applicable,
                           v_field_name => 'MWCapChg');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR mwcapchg IN cur_mwcapchg(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_mwcapchg%ROWCOUNT < mwcapchg.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => mwcapchg.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => mwcapchg.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_MNPW;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - UNMEASURED WATER (UW)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for UW Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_UW (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_uwfixedcharge  NUMBER(9,2); --(D7251 - UWFixedCharge)
  l_uwrvpoundage   NUMBER(9,2); --(D7252 - UWRVPoundage)
  l_uwrvthresh     NUMBER(9,2); --(D7253 - UWRVThresh)
  l_uwrvmaxcharge  NUMBER(9,2); --(D7254 - UWRVMaxCharge)
  l_uwrvmincharge  NUMBER(9,2); --(D7255 - UWRVMinCharge)
  l_uwmisccharge_a NUMBER(9,2); --(D7256 - UWMiscChargeA)
  l_uwmisccharge_b NUMBER(9,2); --(D7257 - UWMiscChargeB)
  l_uwmisccharge_c NUMBER(9,2); --(D7258 - UWMiscChargeC)
  l_uwmisccharge_d NUMBER(9,2); --(D7259 - UWMiscChargeD)
  l_uwmisccharge_e NUMBER(9,2); --(D7260 - UWMiscChargeE)
  l_uwmisccharge_f NUMBER(9,2); --(D7261 - UWMiscChargeF)
  l_uwmisccharge_g NUMBER(9,2); --(D7262 - UWMiscChargeG)
  l_uwmisccharge_h NUMBER(9,2); --(D7263 - UWMiscChargeH)

  CURSOR cur_uwpfc (v_tariff_version NUMBER) IS
    SELECT UWPFC.LOWERMETERSIZE column1value,
         UWPFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(UWPFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_UW_METER_UWPFC UWPFC,
       MOUTRAN.MO_TARIFF_TYPE_UW UW
    WHERE UWPFC.TARIFF_TYPE_PK = UW.TARIFF_TYPE_PK
    AND UW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_UW - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT UW.UWFIXEDCHARGE, -- Unmeasured Fixed Charges (D7251 - UWFixedCharge)
         UW.UWRVPOUNDAGE,  -- Rateable Value Charges (D7252 - UWRVPoundage)
         UW.UWRVTHRESHOLD,  -- Rateable Value Charges (D7253 - UWRVThresh)
         UW.UWRVMAXCHARGE,  -- Rateable Value Charges (D7254 - UWRVMaxCharge)
         UW.UWRVMINCHARGE,  -- Rateable Value Charges (D7255 - UWRVMinCharge)
         UW.UWMISCTYPEACHARGE, -- Miscellaneous Charges (D7256 - UWMiscChargeA)
         UW.UWMISCTYPEBCHARGE, -- Miscellaneous Charges (D7257 - UWMiscChargeB)
         UW.UWMISCTYPECCHARGE, -- Miscellaneous Charges (D7258 - UWMiscChargeC)
         UW.UWMISCTYPEDCHARGE, -- Miscellaneous Charges (D7259 - UWMiscChargeD)
         UW.UWMISCTYPEECHARGE, -- Miscellaneous Charges (D7260 - UWMiscChargeE)
         UW.UWMISCTYPEFCHARGE, -- Miscellaneous Charges (D7261 - UWMiscChargeF)
         UW.UWMISCTYPEGCHARGE, -- Miscellaneous Charges (D7262 - UWMiscChargeG)
         UW.UWMISCTYPEHCHARGE  -- Miscellaneous Charges (D7263 - UWMiscChargeH)
  INTO l_uwfixedcharge,
       l_uwrvpoundage,
       l_uwrvthresh,
       l_uwrvmaxcharge,
       l_uwrvmincharge,
       l_uwmisccharge_a,
       l_uwmisccharge_b,
       l_uwmisccharge_c,
       l_uwmisccharge_d,
       l_uwmisccharge_e,
       l_uwmisccharge_f,
       l_uwmisccharge_g,
       l_uwmisccharge_h
  FROM MOUTRAN.MO_TARIFF_TYPE_UW UW
  WHERE UW.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Unmeasured Fixed Charges',
                        v_field_name => 'UWFixedCharge',
                        v_field_value => l_uwfixedcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'UWRVPoundage',
                        v_field_value => l_uwrvpoundage);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'UWRVThresh',
                        v_field_value => l_uwrvthresh);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'UWRVMaxCharge',
                        v_field_value => l_uwrvmaxcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'UWRVMinCharge',
                        v_field_value => l_uwrvmincharge);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeA',
                        v_field_value => l_uwmisccharge_a);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeB',
                        v_field_value => l_uwmisccharge_b);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeC',
                        v_field_value => l_uwmisccharge_c);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeD',
                        v_field_value => l_uwmisccharge_d);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeE',
                        v_field_value => l_uwmisccharge_e);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeF',
                        v_field_value => l_uwmisccharge_f);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeG',
                        v_field_value => l_uwmisccharge_g);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'UWMiscChargeH',
                        v_field_value => l_uwmisccharge_h);

  -- Muliple charge elements
  -- Unmeasured Water Pipe Fixed Charges (D7263 - UWPFC)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_UW_METER_UWPFC UWPFC,
       MOUTRAN.MO_TARIFF_TYPE_UW UW
  WHERE UWPFC.TARIFF_TYPE_PK = UW.TARIFF_TYPE_PK
  AND UW.TARIFF_VERSION_PK = v_tariff_version;

  MultiChargeElementHeader(v_element_name => 'Unmeasured Water Pipe Fixed Charges',
                           v_applicable => l_applicable,
                           v_field_name => 'UWPFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR uwpfc IN cur_uwpfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => uwpfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_uwpfc%ROWCOUNT < uwpfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => uwpfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => uwpfc.column3value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_UW;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - ASSESSED SEWERAGE (AS)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for AS Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_AS (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_asfixedcharge NUMBER(9,2); --(D7351 - ASFixedCharge)
  l_asvcharge     NUMBER(9,2); --(D7353 - ASVCharge)

  CURSOR cur_asmfc (v_tariff_version NUMBER) IS
    SELECT ASMFC.LOWERMETERSIZE column1value,
         ASMFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(ASMFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_AS_METER_ASMFC ASMFC,
       MOUTRAN.MO_TARIFF_TYPE_AS AST
    WHERE ASMFC.TARIFF_TYPE_PK = AST.TARIFF_TYPE_PK
    AND AST.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_asbandcharge(v_tariff_version NUMBER) IS
    SELECT ASBAND.BAND column1value,
         RTRIM(TO_CHAR(ASBAND.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_AS_BAND_CHARGE ASBAND,
       MOUTRAN.MO_TARIFF_TYPE_AS AST
    WHERE ASBAND.TARIFF_TYPE_PK = AST.TARIFF_TYPE_PK
    AND AST.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_AS - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT AST.ASFIXEDCHARGE, -- Assessed Fixed Charges (D7351 - ASFixedCharge)
         AST.ASVOLMETCHARGE -- Assessed Volumetric Charges (D7353 - ASVCharge)
  INTO l_asfixedcharge,
       l_asvcharge
  FROM MOUTRAN.MO_TARIFF_TYPE_AS AST
  WHERE AST.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Assessed Fixed Charges',
                        v_field_name => 'ASFixedCharge',
                        v_field_value => l_asfixedcharge);

    SingleChargeElement(v_element_name => 'Assessed Volumetric Charges',
                        v_field_name => 'ASVCharge',
                        v_field_value => l_asvcharge);

  -- Muliple charge elements
  -- Assessed Meter Fixed Charges (D7352 - ASMFC)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_AS_METER_ASMFC ASMFC,
       MOUTRAN.MO_TARIFF_TYPE_AS AST
    WHERE ASMFC.TARIFF_TYPE_PK = AST.TARIFF_TYPE_PK
    AND AST.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Assessed Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'ASMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR asmfc IN cur_asmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => asmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_asmfc%ROWCOUNT < asmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => asmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => asmfc.column3value);
    END LOOP;
  END IF;

  -- Assessed Band Charge (D7354 - ASBandCharge)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_AS_BAND_CHARGE ASBAND,
         MOUTRAN.MO_TARIFF_TYPE_AS AST
    WHERE ASBAND.TARIFF_TYPE_PK = AST.TARIFF_TYPE_PK
    AND AST.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Assessed Band Charge',
                             v_applicable => l_applicable,
                             v_field_name => 'ASBandCharge');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR asbandcharge IN cur_asbandcharge(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_asbandcharge%ROWCOUNT < asbandcharge.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => asbandcharge.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => asbandcharge.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_AS;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - UNMEASURED SEWERAGE (US)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for US Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_US (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_usfixedcharge  NUMBER(9,2); --(D7401 - USFixedCharge)
  l_usrvpoundage   NUMBER(9,2); --(D7402 - USRVPoundage)
  l_usrvthresh     NUMBER(9,2); --(D7403 - USRVThresh)
  l_usrvmaxcharge  NUMBER(9,2); --(D7404 - USRVMaxCharge)
  l_usrvmincharge  NUMBER(9,2); --(D7405 - USRVMinCharge)
  l_usmisccharge_a NUMBER(9,2); --(D7406 - USMiscChargeA)
  l_usmisccharge_b NUMBER(9,2); --(D7407 - USMiscChargeB)
  l_usmisccharge_c NUMBER(9,2); --(D7408 - USMiscChargeC)
  l_usmisccharge_d NUMBER(9,2); --(D7409 - USMiscChargeD)
  l_usmisccharge_e NUMBER(9,2); --(D7410 - USMiscChargeE)
  l_usmisccharge_f NUMBER(9,2); --(D7411 - USMiscChargeF)
  l_usmisccharge_g NUMBER(9,2); --(D7412 - USMiscChargeG)
  l_usmisccharge_h NUMBER(9,2); --(D7413 - USMiscChargeH)

  CURSOR cur_uspfc (v_tariff_version NUMBER) IS
    SELECT USPFC.LOWERMETERSIZE column1value,
         USPFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(USPFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_US_METER_USPFC USPFC,
       MOUTRAN.MO_TARIFF_TYPE_US US
    WHERE USPFC.TARIFF_TYPE_PK = US.TARIFF_TYPE_PK
    AND US.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_US - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT US.USFIXEDCHARGE, -- Unmeasured Fixed Charges (D7401 - USFixedCharge)
         US.USRVPOUNDAGE,  -- Rateable Value Charges (D7402 - USRVPoundage)
         US.USRVTHRESHOLD,  -- Rateable Value Charges (D7403 - USRVThresh)
         US.USRVMAXIMUMCHARGE,  -- Rateable Value Charges (D7404 - USRVMaxCharge)
         US.USRVMINIMUMCHARGE,  -- Rateable Value Charges (D7405 - USRVMinCharge)
         US.USMISCTYPEACHARGE, -- Miscellaneous Charges (D7506 - USMiscChargeA)
         US.USMISCTYPEBCHARGE, -- Miscellaneous Charges (D7407 - USMiscChargeB)
         US.USMISCTYPECCHARGE, -- Miscellaneous Charges (D7408 - USMiscChargeC)
         US.USMISCTYPEDCHARGE, -- Miscellaneous Charges (D7409 - USMiscChargeD)
         US.USMISCTYPEECHARGE, -- Miscellaneous Charges (D7410 - USMiscChargeE)
         US.USMISCTYPEFCHARGE, -- Miscellaneous Charges (D7411 - USMiscChargeF)
         US.USMISCTYPEGCHARGE, -- Miscellaneous Charges (D7412 - USMiscChargeG)
         US.USMISCTYPEHCHARGE  -- Miscellaneous Charges (D7413 - USMiscChargeH)
  INTO l_usfixedcharge,
       l_usrvpoundage,
       l_usrvthresh,
       l_usrvmaxcharge,
       l_usrvmincharge,
       l_usmisccharge_a,
       l_usmisccharge_b,
       l_usmisccharge_c,
       l_usmisccharge_d,
       l_usmisccharge_e,
       l_usmisccharge_f,
       l_usmisccharge_g,
       l_usmisccharge_h
  FROM MOUTRAN.MO_TARIFF_TYPE_US US
  WHERE US.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Unmeasured Fixed Charges',
                        v_field_name => 'USFixedCharge',
                        v_field_value => l_usfixedcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'USRVPoundage',
                        v_field_value => l_usrvpoundage);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'USRVThresh',
                        v_field_value => l_usrvthresh);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'USRVMaxCharge',
                        v_field_value => l_usrvmaxcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'USRVMinCharge',
                        v_field_value => l_usrvmincharge);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeA',
                        v_field_value => l_usmisccharge_a);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeB',
                        v_field_value => l_usmisccharge_b);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeC',
                        v_field_value => l_usmisccharge_c);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeD',
                        v_field_value => l_usmisccharge_d);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeE',
                        v_field_value => l_usmisccharge_e);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeF',
                        v_field_value => l_usmisccharge_f);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeG',
                        v_field_value => l_usmisccharge_g);

    SingleChargeElement(v_element_name => 'Miscellaneous Charges',
                        v_field_name => 'USMiscChargeH',
                        v_field_value => l_usmisccharge_h);

  -- Muliple charge elements
  -- Unmeasured Sewerage Pipe Fixed Charges (D7413 - USPFC)
  SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
  INTO l_applicable
  FROM MOUTRAN.MO_US_METER_USPFC USPFC,
       MOUTRAN.MO_TARIFF_TYPE_US US
  WHERE USPFC.TARIFF_TYPE_PK = US.TARIFF_TYPE_PK
  AND US.TARIFF_VERSION_PK = v_tariff_version;

  MultiChargeElementHeader(v_element_name => 'Unmeasured Sewerage Pipe Fixed Charges',
                           v_applicable => l_applicable,
                           v_field_name => 'USPFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR uspfc IN cur_uspfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => uspfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_uspfc%ROWCOUNT < uspfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => uspfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => uspfc.column3value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_US;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - HIGHWAY DRAINAGE (HD)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for HD Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_HD (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_hdcomband      VARCHAR2(16 BYTE); --(D7503 - HDComBand)
  l_hdfixedcharge  NUMBER(9,2); --(D7504 - HDFixedCharge)
  l_hdrvpoundage   NUMBER(9,2); --(D7505 - HDRVPoundage)
  l_hdrvthresh     NUMBER(9,2); --(D7506 - HDRVThresh)
  l_hdrvmaxcharge  NUMBER(9,2); --(D7507 - HDRVMaxCharge)
  l_hdrvmincharge  NUMBER(9,2); --(D7508 - HDRVMinCharge)

  CURSOR cur_hdareaband (v_tariff_version NUMBER) IS
    SELECT HDAB.LOWERAREA column1value,
         HDAB.UPPERAREA column2value,
         RTRIM(TO_CHAR(HDAB.BAND,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_HD_AREA_BAND HDAB,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDAB.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_hdbandcharge(v_tariff_version NUMBER) IS
    SELECT HDBAND.BAND column1value,
         RTRIM(TO_CHAR(HDBAND.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_HD_BAND_CHARGE HDBAND,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDBAND.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_hdbt(v_tariff_version NUMBER) IS
    SELECT HDBT.UPPERANNUALVOL column1value,
         RTRIM(TO_CHAR(HDBT.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_HD_BLOCK_HDBT HDBT,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDBT.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_hdmfc(v_tariff_version NUMBER) IS
    SELECT HDMFC.LOWERMETERSIZE column1value,
         HDMFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(HDMFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_HD_METER_HDMFC HDMFC,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDMFC.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_HD - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT HD.HDCOMBAND, -- Area Charges (D7503 - HDComBand)
         HD.HDFIXEDCHARGE, -- Highway Drainage Fixed Charges (D7504 - HDFixedCharge)
         HD.HDRVPOUNDAGE, -- Rateable Value Charges (D7505 - HDRVPoundage)
         HD.HDRVTHRESHOLD, -- Rateable Value Charges (D7506 - HDRVThresh)
         HD.HDRVMAXCHARGE, -- Rateable Value Charges (D7507 - HDRVMaxCharge)
         HD.HDRVMINCHARGE -- Rateable Value Charges (D7508 - HDRVMinCharge)
  INTO l_hdcomband,
       l_hdfixedcharge,
       l_hdrvpoundage,
       l_hdrvthresh,
       l_hdrvmaxcharge,
       l_hdrvmincharge
  FROM MOUTRAN.MO_TARIFF_TYPE_HD HD
  WHERE HD.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Area Charges',
                        v_field_name => 'HDComBand',
                        v_field_value => l_hdcomband);

    SingleChargeElement(v_element_name => 'Highway Drainage Fixed Charges',
                        v_field_name => 'HDFixedCharge',
                        v_field_value => l_hdfixedcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'HDRVPoundage',
                        v_field_value => l_hdrvpoundage);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'HDRVThresh',
                        v_field_value => l_hdrvthresh);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'HDRVMaxCharge',
                        v_field_value => l_hdrvmincharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'HDRVMinCharge',
                        v_field_value => l_hdrvmaxcharge);

  -- Muliple charge elements
  -- Area Charges (D7501 - HDAreaBand)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_HD_AREA_BAND HDAB,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDAB.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Area Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'HDAreaBand');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR hdareaband IN cur_hdareaband(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => hdareaband.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_hdareaband%ROWCOUNT < hdareaband.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => hdareaband.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => hdareaband.column3value);
    END LOOP;
  END IF;

  -- Area Charges (D7502 - HDBandCharge)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_HD_BAND_CHARGE HDBAND,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDBAND.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Area Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'HDBandCharge');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR hdbandcharge IN cur_hdbandcharge(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_hdbandcharge%ROWCOUNT < hdbandcharge.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => hdbandcharge.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => hdbandcharge.column2value);
    END LOOP;
  END IF;

  -- Highway Drainage: Foul Sewerage Volumetric Charges (D7510 - HDBT)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_HD_BLOCK_HDBT HDBT,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDBT.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Highway Drainage: Foul Sewerage Volumetric Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'HDBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR hdbt IN cur_hdbt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_hdbt%ROWCOUNT < hdbt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => hdbt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => hdbt.column2value);
    END LOOP;
  END IF;

  -- Highway Drainage Meter Fixed Charges (D7509 - HDMFC)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_HD_METER_HDMFC HDMFC,
       MOUTRAN.MO_TARIFF_TYPE_HD HD
    WHERE HDMFC.TARIFF_TYPE_PK = HD.TARIFF_TYPE_PK
    AND HD.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Highway Drainage Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'HDMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR hdmfc IN cur_hdmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => hdmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_hdmfc%ROWCOUNT < hdmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => hdmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => hdmfc.column3value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_HD;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - ASSESSED WATER (AW)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for AW Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_AW (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_awfixedcharge NUMBER(9,2); --(D7201 - AWFixedCharge)
  l_awvcharge     NUMBER(9,2); --(D7203 - AWVCharge)

  CURSOR cur_awmfc (v_tariff_version NUMBER) IS
    SELECT AWMFC.LOWERMETERSIZE column1value,
         AWMFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(AWMFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_AW_METER_AWMFC AWMFC,
       MOUTRAN.MO_TARIFF_TYPE_AW AW
    WHERE AWMFC.TARIFF_TYPE_PK = AW.TARIFF_TYPE_PK
    AND AW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_awbandcharge(v_tariff_version NUMBER) IS
    SELECT AWBAND.BAND column1value,
         RTRIM(TO_CHAR(AWBAND.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_AW_BAND_CHARGE AWBAND,
       MOUTRAN.MO_TARIFF_TYPE_AW AW
    WHERE AWBAND.TARIFF_TYPE_PK = AW.TARIFF_TYPE_PK
    AND AW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_AW - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT AW.AWFIXEDCHARGE, -- Assessed Fixed Charges (D7201 - AWFixedCharge)
         AW.AWVOLUMETRICCHARGE -- Assessed Volumetric Charges (D7203 - AWVCharge)
  INTO l_awfixedcharge,
       l_awvcharge
  FROM MOUTRAN.MO_TARIFF_TYPE_AW AW
  WHERE AW.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Assessed Fixed Charges',
                        v_field_name => 'AWFixedCharge',
                        v_field_value => l_awfixedcharge);

    SingleChargeElement(v_element_name => 'Assessed Volumetric Charges',
                        v_field_name => 'AWVCharge',
                        v_field_value => l_awvcharge);

  -- Muliple charge elements
  -- Assessed Meter Fixed Charges (D7202 - AWMFC)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_AW_METER_AWMFC AWMFC,
       MOUTRAN.MO_TARIFF_TYPE_AW AW
    WHERE AWMFC.TARIFF_TYPE_PK = AW.TARIFF_TYPE_PK
    AND AW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Assessed Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'AWMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR awmfc IN cur_awmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => awmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_awmfc%ROWCOUNT < awmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => awmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => awmfc.column3value);
    END LOOP;
  END IF;

  -- Assessed Band Charge (D7204 - AWBandCharge)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_AW_BAND_CHARGE AWBAND,
         MOUTRAN.MO_TARIFF_TYPE_AW AW
    WHERE AWBAND.TARIFF_TYPE_PK = AW.TARIFF_TYPE_PK
    AND AW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Assessed Band Charge',
                             v_applicable => l_applicable,
                             v_field_name => 'AWBandCharge');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR awbandcharge IN cur_awbandcharge(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
    IF cur_awbandcharge%ROWCOUNT < awbandcharge.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => awbandcharge.column1value);
    ELSE
      SetNilTag(v_col_val_node => g_column_1_value);
    END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => awbandcharge.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_AW;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - TRADE EFFLUENT (TE)
-- AUTHOR         : Kevin Burton
-- CREATED        : 04/04/2016
-- DESCRIPTION    : Procedure to export tariff data for TE Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_TE (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_tefixedcharge NUMBER(9,2); --(D7571 - TEFixedCharge)
  l_ra            NUMBER(9,2); --(D7552 - Ra)
  l_va            NUMBER(9,2); --(D7553 - Va)
  l_bva           NUMBER(9,2); --(D7554 - Bva)
  l_ma            NUMBER(9,2); --(D7555 - Ma)
  l_ba            NUMBER(9,2); --(D7556 - Ba)
  l_sa            NUMBER(9,2); --(D7557 - Sa)
  l_aa            NUMBER(9,2); --(D7558 - Aa)
  l_xa            NUMBER(9,2); --(D7572 - Xa)
  l_ya            NUMBER(9,2); --(D7573 - Ya)
  l_za            NUMBER(9,2); --(D7574 - Za)
  l_vo            NUMBER(9,2); --(D7560 - Vo)
  l_bvo           NUMBER(9,2); --(D7561 - Bvo)
  l_mo            NUMBER(9,2); --(D7562 - Mo)
  l_so            NUMBER(9,2); --(D7564 - So)
  l_ao            NUMBER(9,2); --(D7565 - Ao)
  l_xo            NUMBER(9,2); --(D7575 - Xo)
  l_yo            NUMBER(9,2); --(D7576 - Yo)
  l_zo            NUMBER(9,2); --(D7577 - Zo)
  l_os            NUMBER(9,0); --(D7566 - Os)
  l_ss            NUMBER(9,0); --(D7567 - Ss)
  l_as            NUMBER(9,0); --(D7568 - As)
  l_xs            NUMBER(9,0); --(D7578 - Xs)
  l_ys            NUMBER(9,0); --(D7579 - Ys)
  l_zs            NUMBER(9,0); --(D7580 - Zs)
  l_am            NUMBER(9,0); --(D7569 - Am)
  l_xm            NUMBER(9,0); --(D7581 - Xm)
  l_ym            NUMBER(9,0); --(D7582 - Ym)
  l_zm            NUMBER(9,0); --(D7583 - Zm)
  l_temincharge   NUMBER(9,2); --(D7570 - TEMinCharge)

  CURSOR cur_teband (v_tariff_version NUMBER) IS
    SELECT TEBAND.BAND column1value,
         RTRIM(TO_CHAR(TEBAND.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_TE_BAND_CHARGE TEBAND,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE TEBAND.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_robt (v_tariff_version NUMBER) IS
    SELECT ROBT.UPPERANNUALVOL column1value,
         RTRIM(TO_CHAR(ROBT.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_TE_BLOCK_ROBT ROBT,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE ROBT.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_bobt (v_tariff_version NUMBER) IS
    SELECT BOBT.UPPERANNUALVOL column1value,
         RTRIM(TO_CHAR(BOBT.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_TE_BLOCK_BOBT BOBT,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE BOBT.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_TE - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT TE.TEFIXEDCHARGE, -- Trade Effluent Fixed Charges (D7571 - TEFixedCharge)
         TE.TECHARGECOMPRA, -- Availability Charges (D7552 - Ra)
         TE.TECHARGECOMPVA, -- Availability Charges (D7553 - Va)
         TE.TECHARGECOMPBVA,-- Availability Charges (D7554 - Bva)
         TE.TECHARGECOMPMA, -- Availability Charges (D7555 - Ma)
         TE.TECHARGECOMPBA, -- Availability Charges (D7556 - Ba)
         TE.TECHARGECOMPSA, -- Availability Charges (D7557 - Sa)
         TE.TECHARGECOMPAA, -- Availability Charges (D7558 - Aa)
         TE.TECHARGECOMPXA, -- Availability Charges (D7572 - Xa)
         TE.TECHARGECOMPYA, -- Availability Charges (D7573 - Ya)
         TE.TECHARGECOMPZA, -- Availability Charges (D7574 - Za)
         TE.TECHARGECOMPVO, -- Operational Charges (D7560 - Vo)
         TE.TECHARGECOMPBVO,-- Operational Charges (D7561 - Bvo)
         TE.TECHARGECOMPMO, -- Operational Charges (D7562 - Mo)
         TE.TECHARGECOMPSO, -- Operational Charges (D7564 - So)
         TE.TECHARGECOMPAO, -- Operational Charges (D7565 - Ao)
         TE.TECHARGECOMPXO, -- Operational Charges (D7575 - Xo)
         TE.TECHARGECOMPYO, -- Operational Charges (D7576 - Yo)
         TE.TECHARGECOMPZO, -- Operational Charges (D7577 - Zo)
         TE.TECHARGECOMPOS, -- Operational Charges (D7566 - Os)
         TE.TECHARGECOMPSS, -- Operational Charges (D7567 - Ss)
         TE.TECHARGECOMPAS, -- Operational Charges (D7568 - As)
         TE.TECHARGECOMPXS, -- Operational Charges (D7578 - Xs)
         TE.TECHARGECOMPYS, -- Operational Charges (D7579 - Ys)
         TE.TECHARGECOMPZS, -- Operational Charges (D7580 - Zs)
         TE.TECHARGECOMPAM, -- Operational Charges (D7569 - Am)
         TE.TECHARGECOMPXM, -- Operational Charges (D7581 - Xm)
         TE.TECHARGECOMPYM, -- Operational Charges (D7582 - Ym)
         TE.TECHARGECOMPZM, -- Operational Charges (D7583 - Zm)
         TE.TEMINCHARGE -- TE Minimum Charge (D7570 - TEMinCharge)
  INTO l_tefixedcharge,
       l_ra,
       l_va,
       l_bva,
       l_ma,
       l_ba,
       l_sa,
       l_aa,
       l_xa,
       l_ya,
       l_za,
       l_vo,
       l_bvo,
       l_mo,
       l_so,
       l_ao,
       l_xo,
       l_yo,
       l_zo,
       l_os,
       l_ss,
       l_as,
       l_xs,
       l_ys,
       l_zs,
       l_am,
       l_xm,
       l_ym,
       l_zm,
       l_temincharge
  FROM MOUTRAN.MO_TARIFF_TYPE_TE TE
  WHERE TE.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Trade Effluent Fixed Charges',
                        v_field_name => 'TEFixedCharge',
                        v_field_value => l_tefixedcharge);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Ra',
                        v_field_value => l_ra);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Va',
                        v_field_value => l_va);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Bva',
                        v_field_value => l_bva);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Ma',
                        v_field_value => l_ma);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Ba',
                        v_field_value => l_ba);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Sa',
                        v_field_value => l_sa);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Aa',
                        v_field_value => l_aa);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Xa',
                        v_field_value => l_xa);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Ya',
                        v_field_value => l_ya);

    SingleChargeElement(v_element_name => 'Availability Charges',
                        v_field_name => 'Za',
                        v_field_value => l_za);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Vo',
                        v_field_value => l_vo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Bvo',
                        v_field_value => l_bvo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Mo',
                        v_field_value => l_mo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'So', -- changed for V 0.04
                        v_field_value => l_so);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Ao',
                        v_field_value => l_ao);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Xo',
                        v_field_value => l_xo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Yo',
                        v_field_value => l_yo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Zo',
                        v_field_value => l_zo);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Os',
                        v_field_value => l_os);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Ss',
                        v_field_value => l_ss);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'As',
                        v_field_value => l_as);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Xs',
                        v_field_value => l_xs);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Ys',
                        v_field_value => l_ys);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Zs',
                        v_field_value => l_zs);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Am',
                        v_field_value => l_am);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Xm',
                        v_field_value => l_xm);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Ym',
                        v_field_value => l_ym);

    SingleChargeElement(v_element_name => 'Operational Charges',
                        v_field_name => 'Zm',
                        v_field_value => l_zm);

    SingleChargeElement(v_element_name => 'TE Minimum Charge',
                        v_field_name => 'TEMinCharge', -- changed for V 0.04
                        v_field_value => l_temincharge);

  -- Muliple charge elements
  -- Assessed Band Charges (D7551 - TEBandCharge)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_TE_BAND_CHARGE TEBAND,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE TEBAND.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Assessed Band Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'TEBandCharge');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR teband IN cur_teband(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_teband%ROWCOUNT < teband.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => teband.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => teband.column2value);
    END LOOP;
  END IF;

  -- Operational Charges (D7559 - RoBT)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_TE_BLOCK_ROBT ROBT,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE ROBT.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Operational Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'RoBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR robt IN cur_robt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_robt%ROWCOUNT < robt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => robt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => robt.column2value);
    END LOOP;
  END IF;

  -- Operational Charges (D7563 - BoBT)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_TE_BLOCK_BOBT BOBT,
       MOUTRAN.MO_TARIFF_TYPE_TE TE
    WHERE BOBT.TARIFF_TYPE_PK = TE.TARIFF_TYPE_PK
    AND TE.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Operational Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'BoBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR bobt IN cur_bobt(v_tariff_version)  --Changed for V 0.05
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_bobt%ROWCOUNT < bobt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => bobt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => bobt.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_TE;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - SURFACE WATER DRAINAGE (SW)
-- AUTHOR         : Kevin Burton
-- CREATED        : 05/04/2016
-- DESCRIPTION    : Procedure to export tariff data for SW Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_SW (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_swcomband     NUMBER(9,2); --(D7453 - SWComBand)
  l_swfixedcharge NUMBER(9,2); --(D7454 - SWFixedCharge)
  l_swrvpoundage  NUMBER(9,2); --(D7455 - SWRVPoundage)
  l_swrvthresh    NUMBER(9,2); --(D7456 - SWRVThresh)
  l_swrvmaxcharge NUMBER(9,2); --(D7457 - SWRVMaxCharge)
  l_swrvmincharge NUMBER(9,2); --(D7458 - SWRVMinCharge)

  CURSOR cur_swareaband (v_tariff_version NUMBER) IS
    SELECT ABAND.LOWERAREA column1value,
         ABAND.UPPERAREA column2value,
         RTRIM(TO_CHAR(ABAND.BAND,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_SW_AREA_BAND ABAND,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE ABAND.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_swbandcharge(v_tariff_version NUMBER) IS
    SELECT SWBAND.BAND column1value,
         RTRIM(TO_CHAR(SWBAND.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_SW_BAND_CHARGE SWBAND,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWBAND.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_swbt(v_tariff_version NUMBER) IS
    SELECT SWBT.UPPERANNUALVOL column1value,
         RTRIM(TO_CHAR(SWBT.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_SW_BLOCK_SWBT SWBT,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWBT.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_swmfc(v_tariff_version NUMBER) IS
    SELECT SWMFC.LOWERMETERSIZE column1value,
         SWMFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(SWMFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_SW_METER_SWMFC SWMFC,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWMFC.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_SW - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT SW.SWCOMBAND, -- Area Charges (D7453 - SWComBand)
         SW.SWFIXEDCHARGE, -- Surface Water Fixed Charges (D7454 - SWFixedCharge)
         SW.SWRVPOUNDAGE, -- Rateable Value Charges (D7455 - SWRVPoundage)
         SW.SWRVTHRESHOLD, -- Rateable Value Charges (D7456 - SWRVThresh)
         SW.SWRVMAXIMUMCHARGE, -- Rateable Value Charges (D7457 - SWRVMaxCharge)
         SW.SWRVMINIMUMCHARGE -- Rateable Value Charges (D7458 - SWRVMinCharge)
  INTO l_swcomband,
       l_swfixedcharge,
       l_swrvpoundage,
       l_swrvthresh,
       l_swrvmaxcharge,
       l_swrvmincharge
  FROM MOUTRAN.MO_TARIFF_TYPE_SW SW
  WHERE SW.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Area Charges',
                        v_field_name => 'SWComBand',
                        v_field_value => l_swcomband);

    SingleChargeElement(v_element_name => 'Surface Water Fixed Charges',
                        v_field_name => 'SWFixedCharge',
                        v_field_value => l_swfixedcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'SWRVPoundage',
                        v_field_value => l_swrvpoundage);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'SWRVThresh',
                        v_field_value => l_swrvthresh);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'SWRVMaxCharge',
                        v_field_value => l_swrvmaxcharge);

    SingleChargeElement(v_element_name => 'Rateable Value Charges',
                        v_field_name => 'SWRVMinCharge',
                        v_field_value => l_swrvmincharge);


  -- Muliple charge elements
  -- Area Charges (D7451 - SWAreaBand)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_SW_AREA_BAND ABAND,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE ABAND.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Area Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'SWAreaBand');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR swareaband IN cur_swareaband(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => swareaband.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_swareaband%ROWCOUNT < swareaband.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => swareaband.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => swareaband.column3value);
    END LOOP;
  END IF;

  -- Area Charges (D7452 - SWBandCharge)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_SW_BAND_CHARGE SWBAND,
         MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWBAND.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Area Charges',  --- V 0.15
                             v_applicable => l_applicable,
                             v_field_name => 'SWBandCharge'); -- V 0.13

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR swbandcharge IN cur_swbandcharge(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_swbandcharge%ROWCOUNT < swbandcharge.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => swbandcharge.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => swbandcharge.column2value);
    END LOOP;
  END IF;

  -- Surface Water: Foul Sewerage Volumetric Charges (D7460 - SWBT)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_SW_BLOCK_SWBT SWBT,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWBT.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Surface Water: Foul Sewerage Volumetric Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'SWBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR swbt IN cur_swbt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_swbt%ROWCOUNT < swbt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => swbt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;
      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => swbt.column2value);
    END LOOP;
  END IF;

  -- Surface Water Fixed Meter Charges (D7459 - SWMFC) -- V 0.13
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_SW_METER_SWMFC SWMFC,
       MOUTRAN.MO_TARIFF_TYPE_SW SW
    WHERE SWMFC.TARIFF_TYPE_PK = SW.TARIFF_TYPE_PK
    AND SW.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Surface Water Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'SWMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR swmfc IN cur_swmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => swmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_swmfc%ROWCOUNT < swmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => swmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;
      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => swmfc.column3value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_SW;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - METERED SEWERAGE (MS)
-- AUTHOR         : Kevin Burton
-- CREATED        : 05/04/2016
-- DESCRIPTION    : Procedure to export tariff data for MS Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_MS (v_tariff_version NUMBER,
                                  no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                  no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                  return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_msspfc     NUMBER(9,2); --(D7302 - MSSPFC)

  CURSOR cur_msmfc (v_tariff_version NUMBER) IS
    SELECT MSMFC.LOWERMETERSIZE column1value,
         MSMFC.UPPERMETERSIZE column2value,
         RTRIM(TO_CHAR(MSMFC.CHARGE,'FM9999999990.99'),'.') column3value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MS_METER_MSMFC MSMFC,
       MOUTRAN.MO_TARIFF_TYPE_MS MS
    WHERE MSMFC.TARIFF_TYPE_PK = MS.TARIFF_TYPE_PK
    AND MS.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;

  CURSOR cur_msbt(v_tariff_version NUMBER) IS
    SELECT MSBT.UPPERANNUALVOL column1value,
         RTRIM(TO_CHAR(MSBT.CHARGE,'FM9999999990.99'),'.') column2value,
         COUNT(*) OVER () tot_rows
    FROM MOUTRAN.MO_MS_BLOCK_MSBT MSBT,
       MOUTRAN.MO_TARIFF_TYPE_MS MS
    WHERE MSBT.TARIFF_TYPE_PK = MS.TARIFF_TYPE_PK
    AND MS.TARIFF_VERSION_PK = v_tariff_version ORDER BY 1,2;
BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_MS - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT MS.MSSUPPLYPOINTFIXEDCHARGES -- Supply Point Fixed Charges (D7302 - MSSPFC)
  INTO l_msspfc
  FROM MOUTRAN.MO_TARIFF_TYPE_MS MS
  WHERE MS.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Supply Point Fixed Charges',
                        v_field_name => 'MSSPFC',
                        v_field_value => l_msspfc);

  -- Muliple charge elements
  -- Meter Fixed Charges (D7301 - MSMFC)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_MS_METER_MSMFC MSMFC,
       MOUTRAN.MO_TARIFF_TYPE_MS MS
    WHERE MSMFC.TARIFF_TYPE_PK = MS.TARIFF_TYPE_PK
    AND MS.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Meter Fixed Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'MSMFC');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR msmfc IN cur_msmfc(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                v_col_val_node_text => msmfc.column1value);

      -- Change for V0.06 - added NIL value to last row
      IF cur_msmfc%ROWCOUNT < msmfc.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                  v_col_val_node_text => msmfc.column2value);
      ELSE
        SetNilTag(v_col_val_node => g_column_2_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_3_value,
                                v_col_val_node_text => msmfc.column3value);
    END LOOP;
  END IF;

  -- Metered Volumetric Charges (D7303 - MSBT)
    SELECT DECODE(COUNT(*),0,'false','true')  -- Changed for V 0.02
    INTO l_applicable
    FROM MOUTRAN.MO_MS_BLOCK_MSBT MSBT,
         MOUTRAN.MO_TARIFF_TYPE_MS MS
    WHERE MSBT.TARIFF_TYPE_PK = MS.TARIFF_TYPE_PK
    AND MS.TARIFF_VERSION_PK = v_tariff_version;

    MultiChargeElementHeader(v_element_name => 'Metered Volumetric Charges',
                             v_applicable => l_applicable,
                             v_field_name => 'MSBT');

  IF l_applicable = 'true' THEN  -- Added for V 0.02
    FOR msbt IN cur_msbt(v_tariff_version)
    LOOP
      g_row_node := dbms_xmldom.appendChild(g_rows_node, dbms_xmldom.makeNode(dbms_xmldom.createElement(g_domdoc, g_row)));

      -- Change for V0.06 - added NIL value to last row
      IF cur_msbt%ROWCOUNT < msbt.tot_rows THEN
        MultiChargeElementColVals(v_col_val_node => g_column_1_value,
                                  v_col_val_node_text => msbt.column1value);
      ELSE
        SetNilTag(v_col_val_node => g_column_1_value);
      END IF;

      MultiChargeElementColVals(v_col_val_node => g_column_2_value,
                                v_col_val_node_text => msbt.column2value);
    END LOOP;
  END IF;
  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_MS;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - CHARGE ADJUSTMENT WATER (WCA)
-- AUTHOR         : Kevin Burton
-- CREATED        : 08/04/2016
-- DESCRIPTION    : Procedure to export tariff data for WCA Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_WCA (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_sect154a     NUMBER(9,2); --(D7601 - Sec154AValue)

BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_WCA - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT WCA.SECTION154A -- Section 154A (D7601) - Sec154AValue)
  INTO l_sect154a
  FROM MOUTRAN.MO_TARIFF_TYPE_WCA WCA
  WHERE WCA.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Section 154A',
                        v_field_name => 'Sec154AValue',
                        v_field_value => l_sect154a);

  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_WCA;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - CHARGE ADJUSTMENT SEWERAGE (SCA)
-- AUTHOR         : Kevin Burton
-- CREATED        : 08/04/2016
-- DESCRIPTION    : Procedure to export tariff data for SCA Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_SCA (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_sect154a     NUMBER(9,2); --(D7601 - Sec154AValue)

BEGIN
  g_progress := 'P_DEL_TARIFF_EXPORT_SCA - Tariff Version ' || v_tariff_version;
  g_no_row_read := g_no_row_read + 1;

  -- Check the single charge elements
  SELECT SCA.SECTION154A -- Section 154A (D7601) - Sec154AValue)
  INTO l_sect154a
  FROM MOUTRAN.MO_TARIFF_TYPE_SCA SCA
  WHERE SCA.TARIFF_VERSION_PK = v_tariff_version;

    SingleChargeElement(v_element_name => 'Section 154A',
                        v_field_name => 'Sec154AValue',
                        v_field_value => l_sect154a);

  g_no_row_insert := g_no_row_insert + 1;
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_SCA;

----------------------------------------------------------------------------------------
-- PROCEDURE SPECIFICATION: Wholesaler Tariff XML Export - STANDING DATA (SD)
-- AUTHOR         : Kevin Burton
-- CREATED        : 08/04/2016
-- DESCRIPTION    : Procedure to export tariff data for SD Service Component
-----------------------------------------------------------------------------------------
PROCEDURE P_DEL_TARIFF_EXPORT_SD (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                 no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                 return_code IN OUT NUMBER) IS
  l_applicable VARCHAR2(10);

  l_defretsewer        NUMBER(5,2);   --(D7051 - Default Return to Sewer)
  l_vacchgmethwat      VARCHAR2(128); --(D7052 - Vacancy Charging Method Water)
  l_vacchgmethsew      VARCHAR2(128); --(D7053 - Vacancy Charging Method Sewerage)
  l_tempdisconwat      VARCHAR2(128); --(D7054 - Temporary Disconnection Charging Method Water)
  l_tempdisconsew      VARCHAR2(128); --(D7055 - Temporary Disconnection Charging Method Sewerage)

BEGIN
  G_PROGRESS := 'P_DEL_TARIFF_EXPORT_SD';
--  g_no_row_read := g_no_row_read + 1;  <-- Don't count standing data as being read in from main tariff data

  -- Check the single charge elements
  SELECT SD.DEFAULTRETURNTOSEWER, -- Default Return to Sewer - D7051
         SD.VACANCYCHARGINGMETHODWATER, -- Vacancy Charging Method Water - D7052
         SD.VACANCYCHARGINGMETHODSEWERAGE, -- Vacancy Charging Method Sewerage - D7053
         SD.TEMPDISCONCHARGINGMETHODWAT, -- Temporary Disconnection Charging Method Water - D7054
         SD.TEMPDISCONCHARGINGMETHODSEW  -- Temporary Disconnection Charging Method Sewerage - D7055
  INTO l_defretsewer,
       l_vacchgmethwat,
       l_vacchgmethsew,
       l_tempdisconwat,
       l_tempdisconsew
  FROM MOUTRAN.MO_TARIFF_STANDING_DATA SD;

    SingleChargeElement(v_element_name => 'Default Return to Sewer',
                        v_field_name => 'Default Return to Sewer',
                        v_field_value => l_defretsewer);

    SingleChargeElement(v_element_name => 'Vacancy Charging Method Water',
                        v_field_name => 'Vacancy Charging Method Water',
                        v_field_value => l_vacchgmethwat);

    SingleChargeElement(v_element_name => 'Vacancy Charging Method Sewerage',
                        v_field_name => 'Vacancy Charging Method Sewerage',
                        v_field_value => l_vacchgmethsew);

    SingleChargeElement(v_element_name => 'Temporary Disconnection Charging Method Water',
                        v_field_name => 'Temporary Disconnection Charging Method Water',
                        v_field_value => l_tempdisconwat);

    SingleChargeElement(v_element_name => 'Temporary Disconnection Charging Method Sewerage',
                        v_field_name => 'Temporary Disconnection Charging Method Sewerage',
                        v_field_value => l_tempdisconsew);

--  g_no_row_insert := g_no_row_insert + 1; <-- Don't count standing data as being read in from main tariff data
EXCEPTION
  WHEN OTHERS THEN
    g_no_row_dropped := g_no_row_dropped + 1;
    g_error_number := SQLCODE;
    g_error_message := SQLERRM;
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', substr(g_error_message,1,100),  g_err.TXT_KEY,  substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    P_MIG_BATCH.FN_ERRORLOG(no_batch, g_job.NO_INSTANCE, 'E', 'Job Ended - Unexpected Error',  g_err.TXT_KEY, substr(g_err.TXT_DATA || ',' || g_progress,1,100));
    g_job.IND_STATUS := 'ERR';
    P_MIG_BATCH.FN_UPDATEJOB(no_batch, g_job.NO_INSTANCE, g_job.IND_STATUS);
    return_code := -1;
END P_DEL_TARIFF_EXPORT_SD;

END P_MOU_DEL_TARIFF_EXPORT;
/
exit;