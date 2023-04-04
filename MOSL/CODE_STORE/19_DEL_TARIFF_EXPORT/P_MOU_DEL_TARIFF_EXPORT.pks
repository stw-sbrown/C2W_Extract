create or replace
PACKAGE P_MOU_DEL_TARIFF_EXPORT AS
----------------------------------------------------------------------------------------
-- PACKAGE SPECIFICATION: Wholesaler Tariff XML Export
--
-- AUTHOR         : Kevin Burton
--
-- FILENAME       : P_DEL_TARIFF_EXPORT.pks
--
-- Subversion $Revision: 6460 $
--
-- CREATED        : 31/03/2016
--
-- DESCRIPTION    : Procedure to export tariff data into XML file as specified
--                  in MOSL Tariff XSD
-- NOTES  :
---------------------------- Modification History ---------------------------------------
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      31/03/2016  K.Burton   Initial Draft
-- V 0.02      07/04/2016  K.Burton   Amendments as per updated MOSL guidance in
--                                    Tariff-Table-Master-Data-Guidance-v1.0-060416
-- V 0.03      29/04/2016  K.Burton   Added NIL attribute code
-- V 0.04      10/05/2016  K.Burton   Added new proc P_DEL_VALIDATION_CHECKS
-- V 0.05      12/05/2016  K.Burton   Fixed " rendering in tarifflist tag
-- V 0.06      05/12/2016  K.Burton   Changes to SingleChargeElement and MultiChargeElementHeader procs
--                                    to accomodate changes to MOSL XML structure validation rules
-----------------------------------------------------------------------------------------

  -- static variables for XML element tags
  g_root                 VARCHAR2(30) := '1.0" encoding="UTF-8';
  g_tariff_list          VARCHAR2(30) := 'tarifflist';
  g_tariff_list_attr1     VARCHAR2(30) := 'xsi:schemaLocation'; -- Changed for V 0.05
  g_tariff_list_attr2     VARCHAR2(30) := 'xmlns:xsi'; -- Changed for V 0.05
  g_tariff_list_attr3     VARCHAR2(30) := 'xmlns'; -- Changed for V 0.05
  g_tariff_list_attr_txt1 VARCHAR2(150) := 'http://tempuri.org/XMLSchema.xsd TariffUpload.xsd'; -- Changed for V 0.05
  g_tariff_list_attr_txt2 VARCHAR2(150) := 'http://www.w3.org/2001/XMLSchema-instance'; -- Changed for V 0.05
  g_tariff_list_attr_txt3 VARCHAR2(150) := 'http://tempuri.org/XMLSchema.xsd'; -- Changed for V 0.05
  g_tariff               VARCHAR2(30) := 'tariff';
  g_service_component    VARCHAR2(30) := 'servicecomponent';
  g_name                 VARCHAR2(30) := 'name';
  g_code                 VARCHAR2(30) := 'code';
  g_effective_from       VARCHAR2(30) := 'effectivefrom';
  g_tariff_data_list     VARCHAR2(30) := 'tariffdatalist';
  g_tariff_data          VARCHAR2(30) := 'tariffdata';
  g_state                VARCHAR2(30) := 'state';
  g_charge_element_list  VARCHAR2(30) := 'chargeelementlist';
  g_charge_element       VARCHAR2(30) := 'chargeelement';
  g_is_applicable        VARCHAR2(30) := 'isapplicable';
  g_field_list           VARCHAR2(30) := 'fieldlist';
  g_field                VARCHAR2(30) := 'field';
  g_value                VARCHAR2(30) := 'value';
  g_rows                 VARCHAR2(30) := 'rows';
  g_row                  VARCHAR2(30) := 'row';
  g_column_1_value       VARCHAR2(30) := 'column1value';
  g_column_2_value       VARCHAR2(30) := 'column2value';
  g_column_3_value       VARCHAR2(30) := 'column3value';
  g_nil_attr             VARCHAR2(30) := 'xsi:nil'; -- Added for V 0.02
  g_nil_attr_txt         VARCHAR2(30) := 'true'; -- Added for V 0.02

  g_domdoc dbms_xmldom.DOMDocument;
  g_root_node dbms_xmldom.DOMNode;

  g_tariff_list_node dbms_xmldom.DOMNode;
  g_tariff_list_attribute dbms_xmldom.DOMAttr;
  g_tariff_list_attr_node dbms_xmldom.DOMNode;
  g_tariff_node dbms_xmldom.DOMNode;
  g_service_component_node dbms_xmldom.DOMNode;
  g_name_node dbms_xmldom.DOMNode;
  g_code_node dbms_xmldom.DOMNode;
  g_effective_from_node dbms_xmldom.DOMNode;
  g_tariff_data_list_node dbms_xmldom.DOMNode;
  g_tariff_data_node dbms_xmldom.DOMNode;
  g_state_node dbms_xmldom.DOMNode;
  g_charge_element_list_node dbms_xmldom.DOMNode;
  g_charge_element_node dbms_xmldom.DOMNode;
  g_is_applicable_node dbms_xmldom.DOMNode;
  g_field_list_node dbms_xmldom.DOMNode;
  g_field_node dbms_xmldom.DOMNode;
  g_value_node dbms_xmldom.DOMNode;
  g_rows_node dbms_xmldom.DOMNode;
  g_row_node dbms_xmldom.DOMNode;
  g_col_nil_attribute dbms_xmldom.DOMAttr; -- Added for V 0.03
  g_col_nil_attr_node dbms_xmldom.DOMNode; -- Added for V 0.03

  -- batch processing variables
  c_module_name                 CONSTANT VARCHAR2(30) := 'P_DEL_TARIFF_EXPORT_MAIN';
  c_company_cd                  CONSTANT VARCHAR2(4) := 'STW1';
  g_error_number                VARCHAR2(255);
  g_error_message               VARCHAR2(512);
  g_progress                    VARCHAR2(100);
  g_job                         MIG_JOBSTATUS%ROWTYPE;
  g_err                         MIG_ERRORLOG%ROWTYPE;
  g_no_row_read                 MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  g_no_row_insert               MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  g_no_row_dropped              MIG_CPLOG.RECON_MEASURE_TOTAL%TYPE;
  g_no_row_war                  MIG_JOBSTATUS.WAR_TOLERANCE%TYPE;
  g_no_row_err                  MIG_JOBSTATUS.ERR_TOLERANCE%TYPE;
  g_no_row_exp                  MIG_JOBSTATUS.EXP_TOLERANCE%TYPE;
  g_rec_written                 BOOLEAN;

  FUNCTION CreateXMLTag(v_doc dbms_xmldom.DOMDocument,
                        v_parent dbms_xmldom.DOMNode,
                        v_tag VARCHAR2,
                        v_tag_text VARCHAR2) RETURN dbms_xmldom.DOMNode;

  PROCEDURE P_DEL_TARIFF_EXPORT_MAIN(no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                     no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                     return_code IN OUT NUMBER);

  PROCEDURE SingleChargeElement(v_element_name VARCHAR2, v_field_name VARCHAR2, v_field_value VARCHAR2, v_new_element BOOLEAN DEFAULT TRUE);  -- Changed for V 0.02

  PROCEDURE MultiChargeElementHeader(v_element_name VARCHAR2, v_applicable VARCHAR2, v_field_name VARCHAR2, v_new_element BOOLEAN DEFAULT TRUE);

  PROCEDURE MultiChargeElementColVals(v_col_val_node VARCHAR2, v_col_val_node_text VARCHAR2);
  
  PROCEDURE SetNilTag(v_col_val_node VARCHAR2);  -- Added for V 0.03
  
   -- METERED POTABLE WATER
  PROCEDURE P_DEL_TARIFF_EXPORT_MPW (v_tariff_version NUMBER,
                                     no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                     no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                     return_code IN OUT NUMBER);

  -- METERED NON-POTABLE WATER (No tariff data currently for ST)
  PROCEDURE P_DEL_TARIFF_EXPORT_MNPW (v_tariff_version NUMBER,
                                      no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                      no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                      return_code IN OUT NUMBER);

  -- UNMEASURED WATER
  PROCEDURE P_DEL_TARIFF_EXPORT_UW (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- ASSESSED SEWERAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_AS (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- UNMEASURED SEWERAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_US (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- HIGHWAY DRAINAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_HD (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- ASSESSED WATER
  PROCEDURE P_DEL_TARIFF_EXPORT_AW (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- TRADE EFFLUENT
  PROCEDURE P_DEL_TARIFF_EXPORT_TE (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- SURFACE WATER DRAINAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_SW (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- METERED SEWERAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_MS (v_tariff_version NUMBER,
                                   no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  -- CHARGE ADJUSTMENT WATER
  PROCEDURE P_DEL_TARIFF_EXPORT_WCA (v_tariff_version NUMBER,
                                     no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                     no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                     return_code IN OUT NUMBER);

  -- CHARGE ADJUSTMENT SEWERAGE
  PROCEDURE P_DEL_TARIFF_EXPORT_SCA (v_tariff_version NUMBER,
                                     no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                     no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                     return_code IN OUT NUMBER);

  -- STANDING DATA
  PROCEDURE P_DEL_TARIFF_EXPORT_SD (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);

  PROCEDURE P_DEL_VALIDATION_CHECKS (no_batch IN MIG_BATCHSTATUS.NO_BATCH%TYPE,
                                   no_job IN MIG_JOBREF.NO_JOB%TYPE,
                                   return_code IN OUT NUMBER);                                  

END P_MOU_DEL_TARIFF_EXPORT;
/
exit;