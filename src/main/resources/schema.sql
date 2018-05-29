drop table if exists orders;
drop table if exists customers;

create table customers (
  id    bigint(10) auto_increment not null primary key,
  name  varchar(255)              not null,
  email varchar(255)              null
);

create table orders (
  id          bigint(10) auto_increment not null primary key,
  sku         varchar(255)              not null,
  customer_fk bigint                    not null references customers (id)
);