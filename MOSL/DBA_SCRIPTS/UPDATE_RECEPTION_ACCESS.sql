--
-- Subversion $Revision: 4023 $
--
grant create sequence to RECEPTION;
grant create synonym to RECEPTION;
grant create trigger to RECEPTION;
grant create procedure to RECEPTION;
grant create view to RECEPTION;
grant debug connect session to RECEPTION;
alter user RECEPTION DEFAULT TABLESPACE SOWREC quota unlimited on SOWREC;
commit;
exit;

