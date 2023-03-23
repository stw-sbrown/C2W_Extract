--
-- Subversion $Revision: 4023 $
-- DDL to re-create table BT_CLOCKOVER
-- Drop table first.  This will cause an error if the table does not exist, which can be ignored.

--drop table BT_CLOCKOVER;

  CREATE TABLE BT_CLOCKOVER 
   (	STWMETERREF_PK NUMBER(32,0) NOT NULL, 
	METERREADDATE DATE NOT NULL, 
	METERREAD NUMBER(12,0), 
	ROLLOVERINDICATOR NUMBER(1,0) NOT NULL, 
	 CONSTRAINT CHK01_BTCLOCKOVERINDICATOR CHECK (ROLLOVERINDICATOR IN (0,1)) 
   );  
commit;
exit;

