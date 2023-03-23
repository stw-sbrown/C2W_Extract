SELECT u.username, u.default_tablespace, 
u.temporary_tablespace "TMP TBS", u.profile, r.granted_role,
r.admin_option, r.default_role
FROM sys.dba_users u, sys.dba_role_privs r
WHERE u.username = r.grantee (+) 
AND U.USERNAME IN ('MOUDEL','MOUTRAN','RECEPTION','SAPDEL','SAPTRAN')
GROUP BY u.username, u.default_tablespace,
u.temporary_tablespace, u.profile, r.granted_role,
r.admin_option, r.default_role;
