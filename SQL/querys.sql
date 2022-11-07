/*
    This file are the basic querys for SQL
*/
-- Select
select * from schema.table_name where condition (x > y, x < y, x = y, x = 'text') with ur;

-- Update
update schema.table_name set field=value where condition (x > y, x < y, x = y, x = 'text') with ur;

-- Delete
delete from schema.table_name where condition (x > y, x < y, x = y, x = 'text') with ur;

-- Truncate
truncate table_name with ur;

-- Drop
Drop table_name with ur;
