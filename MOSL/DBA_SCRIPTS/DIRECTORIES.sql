--  Subversion $Revision: 5194 $

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      20/07/2016  M.Marron   Initial Version
-- V 0.02      17/08/2017  S.Badhan   Add directory FINEXPORT for FINDEL.
-----------------------------------------------------------------------------------------
--NB physical directory is different fro each enviroment so uncomment the one your need before running 

-- Give access to a folder to a user account Change create dependent on enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWD'; <--change the directory for each enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWS'; <--change the directory for each enviroment
-- CREATE OR REPLACE DIRECTORY "FILES" AS '/recload/FILES/DOWP'; <--change the directory for each enviroment

grant read,write on directory FILES to MOUTRAN;
grant read,write on directory FILES TO SAPTRAN;
grant read,write on directory FILES to FINTRAN;


-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWD';
-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWS';
-- CREATE OR REPLACE DIRECTORY "DELEXPORT" AS '/recload/EXPORT/DOWP';

GRANT READ,WRITE ON DIRECTORY DELEXPORT TO MOUDEL;

-- CREATE OR REPLACE DIRECTORY "FINEXPORT" AS '/recload/EXPORT/DOWD/FIN';
-- CREATE OR REPLACE DIRECTORY "FINEXPORT" AS '/recload/EXPORT/DOWS/FIN';
-- CREATE OR REPLACE DIRECTORY "FINEXPORT" AS '/recload/EXPORT/DOWP/FIN';

grant read,write on directory FINEXPORT to FINDEL;


-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWD';
-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWS';
-- CREATE OR REPLACE DIRECTORY "SAPDELEXPORT" AS '/recload/EXPORT/SAP/DOWP';

grant read,write on directory SAPDELEXPORT to SAPDEL;


-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWD/FINIMPORTS/SAP';
-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWS/FINIMPORTS/SAP';
-- CREATE OR REPLACE DIRECTORY "FINSAPIMPORT" AS '/recload/FILES/DOWP/FINIMPORTS/SAP';

grant read,write on directory FINSAPIMPORT to RECEPTION;

-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWD/FINIMPORTS/OWC';
-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWS/FINIMPORTS/OWC';
-- CREATE OR REPLACE DIRECTORY "FINOWCIMPORT" AS '/recload/FILES/DOWP/FINIMPORTS/OWC';

grant read,write on directory FINSAPIMPORT to RECEPTION;