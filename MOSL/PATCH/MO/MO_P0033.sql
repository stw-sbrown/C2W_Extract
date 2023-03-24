-- Subversion $Revision: 4140 $	
--
-- CREATED        		: 	19/05/2016
--	
-- DESCRIPTION 		   	: 	Add MO_METER fields for SAP
--							
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ------------------------------------------------
-- V1.01          02/06/2016      D.Cheung        Add Index to MO_METER on INSTALLEDPROPERTYNUMBER for TRAN_ADDRESS performance
-- V0.02          24/05/2016      D.Cheung        Add field DPID_PK to MO_METER for TE
-- V0.01		      19/05/2016      D.Cheung	      Add field columns to MO_METER table for SAP
--								                                Corrections to MO_METER to align with F&V
--                                                Add new field for MANUFACTURER CODE mapping
--                                                Add new field for INSTALLEDPROPERTYNUMBER to METER, METER_READING, METER_ADDRESS and METER_DPIDXREF
--                                                Add new field for STWPROPERTYNUMBER to DISCHARGE_POINT
------------------------------------------------------------------------------------------------------------
-- CHANGES
------------------------------------------------------------------------------------------------------------
ALTER TABLE mo_meter ADD meterlocationdesc VARCHAR2(100);
ALTER TABLE mo_meter ADD meterlocspecialloc VARCHAR2(100);
ALTER TABLE mo_meter ADD meterlocspecialinstr VARCHAR2(100);

ALTER TABLE mo_meter ADD manufcode VARCHAR2(32);
ALTER TABLE mo_meter_address ADD manufcode VARCHAR2(32);
ALTER TABLE mo_meter_dpidxref ADD manufcode VARCHAR2(32);
ALTER TABLE mo_meter_network ADD main_manufcode VARCHAR2(32);
ALTER TABLE mo_meter_network ADD sub_manufcode VARCHAR2(32);
ALTER TABLE mo_meter_reading ADD manufcode VARCHAR2(32);
ALTER TABLE mo_meter_spid_assoc ADD manufcode VARCHAR2(32);

ALTER TABLE mo_meter ADD installedpropertynumber NUMBER(9,0);
ALTER TABLE mo_meter_reading ADD installedpropertynumber NUMBER(9,0);
ALTER TABLE mo_meter_address ADD installedpropertynumber NUMBER(9,0);
ALTER TABLE mo_meter_dpidxref ADD installedpropertynumber NUMBER(9,0);

ALTER TABLE mo_meter MODIFY MEASUREUNITATMETER VARCHAR2(12 BYTE);

ALTER TABLE mo_discharge_point ADD stwpropertynumber_pk NUMBER(9,0);

ALTER TABLE mo_meter ADD DPID_PK VARCHAR2(32);

CREATE INDEX INSTALLEDPROPERTYNUMBER_IDX ON MO_METER (INSTALLEDPROPERTYNUMBER);

commit;
/
show errors;
exit;
