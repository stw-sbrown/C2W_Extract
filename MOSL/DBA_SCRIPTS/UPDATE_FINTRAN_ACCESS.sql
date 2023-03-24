--
-- Subversion $Revision: 5193 $
--
grant create sequence to FINTRAN;
grant create synonym to FINTRAN;
grant create trigger to FINTRAN;
grant create procedure to FINTRAN;
grant create view to FINTRAN;
grant debug connect session to FINTRAN;
alter user FINTRAN DEFAULT TABLESPACE SOWMTRAN quota unlimited on SOWMTRAN;
commit;
exit;
