-- N.Henderson
-- V0.01	Initial build
-- Create folder definitions in Oracle in MOUDEL

create or replace directory "DELEXPORT" as '/recload/EXPORT'
grant read,write on directory DELEXPORT to MOUDEL;
