--
-- CREATED        		: 	25/02/2016
--	
-- DESCRIPTION 		   	: 	Changes all percentage type fields where the length was (2,2)
--							
--
--
-- Subversion $Revision: 4023 $	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Change the fields listed below so that the percentage
--													can be 100.00 if needed
--
-- Special notes			:  This procedure can not be run if there is data in the table.
--
-- 
------------------------------------------------------------------------------------------------------------


ALTER TABLE MO_ORG MODIFY (DEFAULTRETURNTOSEWER NUMBER(5,2));
ALTER TABLE MO_SERVICE_COMPONENT MODIFY (SPECIALAGREEMENTFACTOR NUMBER(5,2));
ALTER TABLE MO_DISCHARGE_POINT MODIFY (SEASONALFACTOR NUMBER (5,2));
ALTER TABLE MO_DISCHARGE_POINT MODIFY (PERCENTAGEALLOWANCE NUMBER (5,2));
ALTER TABLE MO_DISCHARGE_POINT MODIFY (DPIDSPECIALAGREEMENTFACTOR NUMBER (5,2));
ALTER TABLE MO_METER MODIFY (RETURNTOSEWER NUMBER (5,2));
ALTER TABLE MO_METER_DPIDXREF MODIFY (PERCENTAGEDISCHARGE NUMBER (5,2));
ALTER TABLE MO_TARIFF_VERSION MODIFY (DEFAULTRETURNTOSEWER NUMBER (5,2));
ALTER TABLE MO_TARIFF_TYPE_MPW MODIFY (MPWPREMIUMTOLFACTOR NUMBER (5,2));
ALTER TABLE MO_TARIFF_TYPE_MNPW MODIFY (MNPWPREMIUMTOLFACTOR NUMBER (5,2));
commit;
exit;