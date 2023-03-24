--
-- Subversion $Revision: 4023 $
--
grant create sequence to MOUDEL;
grant create synonym to MOUDEL;
grant create trigger to MOUDEL;
grant create procedure to MOUDEL;
grant create view to MOUDEL;
grant debug connect session to MOUDEL;
alter user MOUDEL DEFAULT TABLESPACE SOWMDEL quota unlimited on SOWMDEL;
commit;
exit;

