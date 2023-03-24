------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0045.sql
--
-- Subversion $Revision: 5458 $	
--
-- CREATED        		: 	26/08/2016
--	
-- DESCRIPTION 		   	: 	Remove SAPFLOCNUMBER constraint MO_ELIGIBLE_PREMISES
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.01       		26/08/2016     S.Badhan  Remove SAPFLOCNUMBER constraint from MO_ELIGIBLE_PREMISES.
-- V0.02          07/09/2016     K.Burton  Added Tariff Table indexes for DEL_SERVICE_COMPONENT performance issues
------------------------------------------------------------------------------------------------------------

ALTER TABLE MO_ELIGIBLE_PREMISES DROP CONSTRAINT CH01_SAPFLOCNUMBER;

CREATE INDEX IDX_SUPPLY_POINT_CORESPID ON MO_SUPPLY_POINT(CORESPID_PK);
CREATE INDEX IDX_TARIFF_VERSION_TARIFFCODE ON MO_TARIFF_VERSION(TARIFFCODE_PK);
CREATE INDEX IDX_TARIFF_VERSION_MPW ON MO_TARIFF_TYPE_MPW(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_AS ON MO_TARIFF_TYPE_AS(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_AW ON MO_TARIFF_TYPE_AW(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_HD ON MO_TARIFF_TYPE_HD(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_MNPW ON MO_TARIFF_TYPE_MNPW(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_MS ON MO_TARIFF_TYPE_MS(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_SW ON MO_TARIFF_TYPE_SW(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_TE ON MO_TARIFF_TYPE_TE(TARIFF_VERSION_PK);
CREATE INDEX IDX_TARIFF_VERSION_UW ON MO_TARIFF_TYPE_UW(TARIFF_VERSION_PK);

commit;
/
show errors;
exit;


