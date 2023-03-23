--Create table BT_METER_READ_FREQ
--N.Henderson - 12/04/2016
-- Subversion $Revision: 4023 $
--Attempt to drop the table first. Will generate warnings if does not
--exist but can be ignored.

--DROP TABLE BT_METER_READ_FREQ;
  CREATE TABLE BT_METER_READ_FREQ 
   (CD_SCHED_FREQ CHAR(1 BYTE) NOT NULL ENABLE, 
	DS_SCHED_FREQ CHAR(15 BYTE) NOT NULL ENABLE, 
	NO_PROPERTY NUMBER(9,0), 
	NO_EQUIPMENT NUMBER(9,0) NOT NULL ENABLE
   );

  CREATE INDEX MOUTRAN.INDEX3 ON MOUTRAN.BT_METER_READ_FREQ (NO_EQUIPMENT);
  
  commit;
  exit;
