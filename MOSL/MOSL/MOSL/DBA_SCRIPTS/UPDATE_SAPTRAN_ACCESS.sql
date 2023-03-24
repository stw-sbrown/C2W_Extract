--
-- Subversion $Revision: 4023 $
--
grant create sequence to SAPTRAN;
grant create synonym to SAPTRAN;
grant create trigger to SAPTRAN;
grant create procedure to SAPTRAN;
grant create view to SAPTRAN;
grant debug connect session to SAPTRAN;
alter user SAPTRAN DEFAULT TABLESPACE SOWSTRAN quota unlimited on SOWSTRAN;
commit;
exit;

