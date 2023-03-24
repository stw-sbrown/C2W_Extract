--
-- Subversion $Revision: 4023 $
--
grant create sequence to SAPDEL;
grant create synonym to SAPDEL;
grant create trigger to SAPDEL;
grant create procedure to SAPDEL;
grant create view to SAPDEL;
grant debug connect session to SAPDEL;
alter user SAPDEL DEFAULT TABLESPACE SOWSDEL quota unlimited on SOWSDEL;
commit;
exit;

