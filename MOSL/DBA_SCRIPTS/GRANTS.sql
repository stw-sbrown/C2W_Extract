--  Subversion $Revision: 4936 $

--------------------------- Modification History ---------------------------------------   
--
-- Version     Date        Author     Description
-- ---------   ----------  -------    ---------------------------------------------------
-- V 0.01      16/06/2016  S.Badhan   Change directory name for export for DOWS
-- V 0.02      19/07/2016  M.Marron   Added permissions to FINTRAN and FINDEL
-----------------------------------------------------------------------------------------

grant create sequence to MOUTRAN;
grant create synonym to MOUTRAN;
grant create trigger to MOUTRAN;
grant create procedure to MOUTRAN;
grant create view to MOUTRAN;
grant debug connect session to MOUTRAN;
grant drop public synonym to MOUTRAN;
alter user MOUTRAN DEFAULT TABLESPACE SOWMTRAN quota unlimited on SOWMTRAN;

grant create sequence to MOUDEL;
grant create synonym to MOUDEL;
grant create trigger to MOUDEL;
grant create procedure to MOUDEL;
grant create view to MOUDEL;
grant debug connect session to MOUDEL;
grant drop public synonym to MOUDEL;
alter user MOUDEL DEFAULT TABLESPACE SOWMDEL quota unlimited on SOWMDEL;

grant create sequence to RECEPTION;
grant create synonym to RECEPTION;
grant create trigger to RECEPTION;
grant create procedure to RECEPTION;
grant create view to RECEPTION;
grant debug connect session to RECEPTION;
grant drop public synonym to RECPTION;
alter user RECEPTION DEFAULT TABLESPACE SOWREC quota unlimited on SOWREC;

grant create sequence to SAPDEL;
grant create synonym to SAPDEL;
grant create trigger to SAPDEL;
grant create procedure to SAPDEL;
grant create view to SAPDEL;
grant debug connect session to SAPDEL;
grant drop public synonym to SAPDEL;
alter user SAPDEL DEFAULT TABLESPACE SOWSDEL quota unlimited on SOWSDEL;


grant create sequence to SAPTRAN;
grant create synonym to SAPTRAN;
grant create trigger to SAPTRAN;
grant create procedure to SAPTRAN;
grant create view to SAPTRAN;
grant debug connect session to SAPTRAN;
grant drop public synonym to SAPTRAN;
alter user SAPTRAN DEFAULT TABLESPACE SOWSTRAN quota unlimited on SOWSTRAN;

GRANT CREATE SEQUENCE TO FINTRAN;
GRANT CREATE SYNONYM TO FINTRAN;
GRANT CREATE TRIGGER TO FINTRAN;
GRANT CREATE PROCEDURE TO FINTRAN;
GRANT CREATE VIEW TO FINTRAN;
GRANT DEBUG CONNECT SESSION TO FINTRAN;
GRANT DROP PUBLIC SYNONYM TO FINTRAN;
alter user SAPTRAN DEFAULT TABLESPACE SOWFTRAN quota unlimited on SOWFTRAN;

GRANT CREATE SEQUENCE TO FINDEL;
GRANT CREATE SYNONYM TO FINDEL;
GRANT CREATE TRIGGER TO FINDEL;
GRANT CREATE PROCEDURE TO FINDEL;
GRANT CREATE VIEW TO FINDEL;
GRANT DEBUG CONNECT SESSION TO FINDEL;
GRANT DROP PUBLIC SYNONYM TO FINDEL;
alter user SAPTRAN DEFAULT TABLESPACE SOWFDEL quota unlimited on SOWFDEL;




