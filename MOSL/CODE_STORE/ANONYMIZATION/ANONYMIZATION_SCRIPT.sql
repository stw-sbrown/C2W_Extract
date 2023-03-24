create or replace
PROCEDURE ANONYMIZATION_SCRIPT
IS
--
-- CREATED        		: 	20/05/2016
--	
-- DESCRIPTION 		   	: 	Make Customer and Address details anonymous in MOUTRAN tables
--							
-- $Revision: 4055 $
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      ----------      -------         ------------------------------------------------
-- V0.03          23/05/2016      D.Cheung        Readded Country - set to NULL
-- V0.02          20/05/2016      D.Cheung        Removed Country
-- V0.01		      20/05/2016      D.Cheung	      Initial Draft
--
------------------------------------------------------------------------------------------------------------
BEGIN

-- Update Customer details
UPDATE MO_CUSTOMER SET CUSTOMERNAME = 'STW_CUST_' || CUSTOMERNUMBER_PK WHERE CUSTOMERNAME IS NOT NULL;
UPDATE MO_CUSTOMER SET CUSTOMERBANNERNAME = 'STW_CUST_' || CUSTOMERNUMBER_PK WHERE CUSTOMERBANNERNAME IS NOT NULL;

--Update Address details
--UPDATE MO_ADDRESS SET PRIMARYADDRESSABLEOBJECT = 'UNIT 4' WHERE PRIMARYADDRESSABLEOBJECT IS NOT NULL;
--UPDATE MO_ADDRESS SET SECONDADDRESABLEOBJECT = 'SEVERN TRENT CENTRE' WHERE SECONDADDRESABLEOBJECT IS NOT NULL;
--UPDATE MO_ADDRESS SET ADDRESSLINE01 = '2 ST. JOHNS STREET';
--UPDATE MO_ADDRESS SET ADDRESSLINE02 = NULL;
--UPDATE MO_ADDRESS SET ADDRESSLINE03 = NULL;
--UPDATE MO_ADDRESS SET ADDRESSLINE04 = NULL;
--UPDATE MO_ADDRESS SET ADDRESSLINE05 = 'COVENTRY' WHERE ADDRESSLINE05 IS NOT NULL;
--UPDATE MO_ADDRESS SET POSTCODE = 'CV1 2LZ';
--UPDATE MO_ADDRESS SET COUNTRY = NULL;
UPDATE MO_ADDRESS SET PRIMARYADDRESSABLEOBJECT = 'UNIT 4'
, SECONDADDRESABLEOBJECT = 'SEVERN TRENT CENTRE'
, ADDRESSLINE01 = '2 ST. JOHNS STREET'
, ADDRESSLINE02 = NULL
, ADDRESSLINE03 = NULL
, ADDRESSLINE04 = NULL
, ADDRESSLINE05 = 'COVENTRY'
, POSTCODE = 'CV1 2LZ'
, COUNTRY = NULL;

commit;

END;

/
show errors;
exit;