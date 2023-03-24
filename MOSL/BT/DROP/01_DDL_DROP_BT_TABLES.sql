--Drop all BT tables.
--N.Henderson
-- $Revision: 5214 $
--12/04/2016
--19/05/2016 M.Marron addd BT_TE tables

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.07      23/08/2016  K.Burton   Added BT_ADDRESSES
-- V 0.06      27/07/2016  D.Cheung   I-319. Added drop of BT_SENSI_SITE_SPECIFIC.
-- V 0.05      07/07/2016  S.Badhan   I-278. Added drop of BT_MISS_AG_SC.
-- V 0.04      01/07/2016  D.Cheung   Added drop of BT_METER_NETWORK.
-- V 0.03      29/06/2016  S.Badhan   I-260. Added drop of BT_SC_AS.
-- V 0.02      15/06/2016  S.Badhan   Added drop of BT_BAD_DATA.
------------------------------------------------------------------------------------------

DROP TABLE BT_CLOCKOVER;
DROP TABLE BT_METER_READ_FREQ;
DROP TABLE BT_METER_SPID;
DROP TABLE BT_SC_MPW;
DROP TABLE BT_SC_UW;
DROP TABLE BT_SPR_METER;
DROP TABLE BT_SPR_TARIFF;
DROP TABLE BT_SPR_TARIFF_ALGITEM;
DROP TABLE BT_SPR_TARIFF_EXTREF;
DROP TABLE BT_SP_TARIFF;
DROP TABLE BT_SP_TARIFF_ALG;
DROP TABLE BT_SP_TARIFF_EXTREF;
DROP TABLE BT_SP_TARIFF_REFTAB;
DROP TABLE BT_SP_TARIFF_SPLIT;
DROP TABLE BT_TVP054;
DROP TABLE BT_TVP163;
DROP TABLE BT_SP_TARIFF_ALGITEM;
DROP TABLE BT_TE_SUMMARY;
DROP table BT_TE_WORKING;
DROP TABLE BT_INSTALL_ADDRESS;
DROP TABLE BT_BAD_DATA;
DROP TABLE BT_SC_AS;
DROP TABLE BT_METER_NETWORK;
DROP TABLE BT_SENSI_SITE_SPECIFIC;
drop table BT_MISS_AG_SC;
DROP TABLE BT_FOREIGN_ADDRESSES;
DROP TABLE BT_OWC_CUST_SWITCHED_SUPPLIER;
DROP TABLE BT_ADDRESSES;
commit;
exit;


