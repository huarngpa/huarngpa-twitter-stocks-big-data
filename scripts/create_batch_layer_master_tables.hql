create table if not exists huarngpa_master_twitter(
  col0 string,
  col1 date,
  col2 string,
  col3 string,
  col4 bigint,
  col5 bigint,
  col6 string
) stored as orc;

create table if not exists huarngpa_master_stock(
  col0 string,
  col1 date,
  col2 double,
  col3 double,
  col4 double,
  col5 double,
  col6 bigint,
  col7 double
) stored as orc;
