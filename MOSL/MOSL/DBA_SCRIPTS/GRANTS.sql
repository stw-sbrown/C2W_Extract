--  Subversion $Revision: 4023 $
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



-- Give access to a folder to a user account
grant read,write on directory DELEXPORT to MOUTRAN;

