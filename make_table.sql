-- create table for ts ohlcv data
create table if not exists binanceklines1h (
    ts timestamptz not null,
    o float8,
    h float8,
    l float8,
    c float8,
    v float8,
    sym varchar(10) not null,
    primary key (ts,sym)
);
