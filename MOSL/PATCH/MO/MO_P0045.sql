------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0045.sql
--
-- Subversion $Revision: 5822 $	
--
-- CREATED        		: 	05/10/2016
--	
-- DESCRIPTION 		   	: 	Add column OWC
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author    Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.02       		12/10/2016     S.Badhan  Swapped with P0049 as later patches use these new columns
-- V0.01       		05/10/2016     S.Badhan  Initial version
------------------------------------------------------------------------------------------------------------


ALTER TABLE MO_ELIGIBLE_PREMISES ADD OWC VARCHAR2(32);
ALTER TABLE MO_SUPPLY_POINT ADD OWC VARCHAR2(32);
ALTER TABLE MO_SERVICE_COMPONENT ADD OWC VARCHAR2(32);
ALTER TABLE MO_DISCHARGE_POINT ADD OWC VARCHAR2(32);
ALTER TABLE MO_CALCULATED_DISCHARGE ADD OWC VARCHAR2(32);
ALTER TABLE MO_CUSTOMER ADD OWC VARCHAR2(32);
ALTER TABLE MO_ADDRESS ADD OWC VARCHAR2(32);
ALTER TABLE MO_PROPERTY_ADDRESS ADD OWC VARCHAR2(32);
ALTER TABLE MO_CUST_ADDRESS ADD OWC VARCHAR2(32);
ALTER TABLE MO_METER ADD OWC VARCHAR2(32);
ALTER TABLE MO_METER_ADDRESS ADD OWC VARCHAR2(32);
ALTER TABLE MO_METER_NETWORK ADD OWC VARCHAR2(32);
ALTER TABLE MO_METER_SPID_ASSOC ADD OWC VARCHAR2(32);
ALTER TABLE MO_METER_READING ADD OWC VARCHAR2(32);

commit;
/
show errors;
exit;