--
-- 18_DDL_BT_BAD_DATA.sql
-- Create bad data table
-- Subversion $Revision: 4023 $
-- Date - 20/05/2016
-- Written By - Surinder Badhan
--

 CREATE TABLE BT_BAD_DATA 
   (	ROW_ID 		ROWID, 
	SEARCH_TERM 	VARCHAR2(30), 
	TABLE_NAME 	VARCHAR2(30), 
	COLUMN_NAME 	VARCHAR2(30), 
	VALUE 		VARCHAR2(2000), 
	STATUS 		VARCHAR2(1)
   )  ;

COMMENT ON TABLE BT_BAD_DATA IS 'BT_BAD_DATA';

commit;

exit;


