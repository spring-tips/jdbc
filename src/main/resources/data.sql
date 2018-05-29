insert into customers (name, email) values ('josh', 'josh@joshlong.com');
insert into customers (name, email) values ('jane', 'jane@jane.com');
insert into customers (name, email) values ('bob', 'bob@bob.com');
insert into customers (name, email) values ('tam mie', 'tammie@tammie.com');
insert into customers (name, email) values ('michelle', 'michelle@michelle.com');
insert into customers (name, email) values ('eva', 'eva@eva.com');

insert into orders (sku, customer_fk) values ('1a754da6-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'josh'));
insert into orders (sku, customer_fk) values ('1a754da5-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'josh'));
insert into orders (sku, customer_fk) values ('1a754da3-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'josh'));

insert into orders (sku, customer_fk) values ('1a754d9d-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'jane'));
insert into orders (sku, customer_fk) values ('1a754d5c-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'jane'));

insert into orders (sku, customer_fk) values ('1a754d44-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'bob'));
insert into orders (sku, customer_fk) values ('1a754d82-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'bob'));

insert into orders (sku, customer_fk) values ('1a754d83-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'michelle'));
insert into orders (sku, customer_fk) values ('1a754d95-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'michelle'));
insert into orders (sku, customer_fk) values ('1a754da7-62b0-11e8-aa39-0242e90c0e19', (select id
                                                      from customers
                                                      where name = 'michelle'));