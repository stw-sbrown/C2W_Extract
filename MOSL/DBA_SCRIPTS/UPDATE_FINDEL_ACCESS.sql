--
-- Subversion $Revision: 5193 $
--
grant create sequence to FINDEL;
grant create synonym to FINDEL;
grant create trigger to FINDEL;
grant create procedure to FINDEL;
grant create view to FINDEL;
grant debug connect session to FINDEL;
alter user FINDEL DEFAULT TABLESPACE SOWMDEL quota unlimited on SOWMDEL;
commit;
exit;

