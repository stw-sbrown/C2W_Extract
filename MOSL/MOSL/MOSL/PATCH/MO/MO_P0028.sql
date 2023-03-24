--
-- CREATED        		: 	29/04/2016
--	
-- DESCRIPTION 		   	: 	Alter constraint RF01_DISCONRECONDEREGSTATUS to also include ‘REC’
--
--
-- Subversion $Revision: 4023 $	
--							
--
-- ASSOCIATED FILES		:	
-- ASSOCIATED SCRIPTS  	:	
--
--
---------------------------- Modification History ----------------------------------------------------------
--
-- Version     		Date            Author         	Description
-- ---------      	----------      -------         ------------------------------------------------
-- V0.01		    29/04/2016		S.Badhan		Alter constraint RF01_DISCONRECONDEREGSTATUS to also include ‘REC
--
--
-- 
------------------------------------------------------------------------------------------------------------


ALTER TABLE MO_SUPPLY_POINT DROP CONSTRAINT RF01_DISCONRECONDEREGSTATUS;

ALTER TABLE MO_SUPPLY_POINT ADD CONSTRAINT RF01_DISCONRECONDEREGSTATUS CHECK (DISCONRECONDEREGSTATUS IN ('TDISC', 'REC'));

commit;
exit;