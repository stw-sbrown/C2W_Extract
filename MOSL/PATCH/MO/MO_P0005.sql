------------------------------------------------------------------------------
-- TASK					: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	Nigel Henderson
--
-- FILENAME       		: 	P00005.sql
--
--
-- Subversion $Revision: 4023 $	
--
-- CREATED        		: 	25/02/2016
--
--
-- Subversion $Revision: 4023 $	
--	
-- DESCRIPTION 		   	: 	Update all money/costs fields so that their field sizes are valid 
--
-- NOTES  				:	
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    25/02/2016		N.Henderson		Update all money/costs fields so that their field sizes are valid		
--													Currently set to 6,2 which is a total of six digits, with two 
--													to the right of the decimal place
--
--
-- 
------------------------------------------------------------------------------------------------------------

--V0.01

ALTER TABLE MO_TARIFF_VERSION MODIFY (SECTION154PAYMENTVALUE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MPW MODIFY (MPWSUPPLYPOINTFIXEDCHARGES NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MPW MODIFY (MPWDAILYSTANDBYUSAGEVOLCHARGE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MPW MODIFY (MPWDAILYPREMIUMUSAGEVOLCHARGE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MPW MODIFY(MPWMAXIMUMDEMANDTARIFF NUMBER(9,2));
ALTER TABLE MO_MPW_METER_MWMFC MODIFY (CHARGE NUMBER(9,2));
ALTER TABLE MO_MPW_BLOCK_MWBT MODIFY (CHARGE NUMBER(9,2));
ALTER TABLE MO_MPW_STANDBY_MWCAPCHG MODIFY (CHARGE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MNPW MODIFY (MNPWSUPPLYPOINTFIXEDCHARGE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MNPW MODIFY (MNPWDAILYSTANDBYUSAGEVOLCHARGE NUMBER(9,2));
ALTER TABLE MO_TARIFF_TYPE_MNPW MODIFY (MNPWDAILYPREMIUMUSAGEVOLCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_MNPW MODIFY (MNPWMAXIMUMDEMANDTARIFF NUMBER (9,2));
ALTER TABLE MO_MNPW_METER_MWMFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_MNPW_BLOCK_MWBT MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_MNPW_STANDBY_MWCAPCHG MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_AW MODIFY (AWFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_AW MODIFY (AWVOLUMETRICCHARGE NUMBER (9,2));
ALTER TABLE MO_AW_METER_AWMFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_AW_BAND_CHARGE MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWRVPOUNDAGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWRVTHRESHOLD NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWRVMAXCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWRVMINCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEACHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEBCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPECCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEECHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEFCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEGCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_UW MODIFY (UWMISCTYPEHCHARGE NUMBER (9,2));
ALTER TABLE MO_UW_METER_UWPFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_MS MODIFY (MSSUPPLYPOINTFIXEDCHARGES NUMBER (9,2));
ALTER TABLE MO_MS_BLOCK_MSBT MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_MS_METER_MSMFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_AS MODIFY (ASFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_AS MODIFY (ASVOLMETCHARGE NUMBER (9,2));
ALTER TABLE MO_AS_METER_ASMFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_AS_BAND_CHARGE MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USRVPOUNDAGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USRVTHRESHOLD NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USRVMAXIMUMCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USRVMINIMUMCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEACHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEBCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPECCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEECHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEFCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEGCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_US MODIFY (USMISCTYPEHCHARGE NUMBER (9,2));
ALTER TABLE MO_US_METER_USPFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWRVPOUNDAGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWRVTHRESHOLD NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWRVMAXIMUMCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWRVMINIMUMCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_SW MODIFY (SWMETERFIXEDCHARGES NUMBER (9,2));
ALTER TABLE MO_SW_BAND_CHARGE MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_SW_BLOCK_SWBT MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_SW_METER_SWMFC MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_HD MODIFY (HDFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_HD MODIFY (HDRVPOUNDAGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_HD MODIFY (HDRVTHRESHOLD NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_HD MODIFY (HDRVMAXCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_HD MODIFY (HDRVMINCHARGE NUMBER (9,2));
ALTER TABLE MO_HD_BAND_CHARGE MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_HD_BLOCK_HDBT MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_HD_METER_HDMFC	MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TEFIXEDCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPRA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPVA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPBVA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPMA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPBA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPSA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPAA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPVO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPBVO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPMO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPSO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPAO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TEMINCHARGE NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPXA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPYA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPZA NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPXO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPYO NUMBER (9,2));
ALTER TABLE MO_TARIFF_TYPE_TE MODIFY (TECHARGECOMPZO NUMBER (9,2));
ALTER TABLE MO_TE_BAND_CHARGE MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TE_BLOCK_ROBT MODIFY (CHARGE NUMBER (9,2));
ALTER TABLE MO_TE_BLOCK_BOBT MODIFY (CHARGE NUMBER (9,2));
commit;
exit;
