select d.table_name,
d.constraint_name "Primary Constraint Name",
b.constraint_name "Referenced Constraint Name"
from user_constraints d,
(select c.constraint_name,c.r_constraint_name, c.table_name
from user_constraints c 
where table_name='MO_METER' --your table name instead of EMPLOYEES
and constraint_type='R') b
where d.constraint_name=b.r_constraint_name



select 'alter table '||table_name||' enable constraint '||constraint_name||';'
from user_constraints
where constraint_type = 'R';

select 'TRUNCATE TABLE ' || table_name || ';' from user_tables;