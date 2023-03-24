--O.Badmus
--01/06/2016
--Subversion $Revision: 5163 $
--Drop associated Address tables.
--

---------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.02      15/05/2016  S.Badhan   removed from .sql from end of proc name
-- V 0.01      10/06/2016  O.Badmus   Initial Draft
-----------------------------------------------------------------------------------------



drop table SAP_METER_ADDRESS;
drop table SAP_CUST_ADDRESS;
drop table SAP_PROPERTY_ADDRESS;
drop TABLE SAP_ADDRESS;

drop procedure P_SAP_TRAN_ADDRESS;
commit;
exit;