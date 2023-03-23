--
-- Subversion $Revision: 4023 $
--
grant create sequence to MOUTRAN;
grant create synonym to MOUTRAN;
grant create trigger to MOUTRAN;
grant create procedure to MOUTRAN;
grant create view to MOUTRAN;
grant debug connect session to MOUTRAN;
alter user MOUTRAN DEFAULT TABLESPACE SOWMTRAN quota unlimited on SOWMTRAN;
commit;
exit;
