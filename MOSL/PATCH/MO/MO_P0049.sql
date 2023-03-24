------------------------------------------------------------------------------
-- TASK				: 	MOSL RDBMS PATCHES 
--
-- AUTHOR         		: 	S.Badhan
--
-- FILENAME       		: 	MO_P0049.sql
--
-- Subversion $Revision: 6088 $	
--
-- CREATED        		: 	26/08/2016
--	
-- DESCRIPTION 		   	: 	Remove SAPFLOCNUMBER constraint MO_ELIGIBLE_PREMISES
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date           Author   Description
-- ---------      ----------     -------   -----------------------------------------------------------------
-- V0.03       		02/11/2016     S.Badhan  Add index on MO_SUPPLY_POINT
-- V0.02       		12/10/2016     S.Badhan  Swapped with P0045.
-- V0.01       		26/08/2016     S.Badhan  Remove SAPFLOCNUMBER constraint from MO_ELIGIBLE_PREMISES.
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

CREATE INDEX IDX_SUPPLY_POINT_PROPERTY ON MO_SUPPLY_POINT(STWPROPERTYNUMBER_PK);

commit;
/
show errors;
exit;


