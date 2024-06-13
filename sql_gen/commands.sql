show databases;
create database db0;
create database db1;
create database db2;
show databases;

use db0;
show tables;
create table account(
  id int, 
  name char(16) unique, 
  balance float, 
  primary key(id)
);
show tables;

execfile "../sql_gen/account00.txt";
execfile "../sql_gen/account01.txt";
execfile "../sql_gen/account02.txt";
execfile "../sql_gen/account03.txt";
execfile "../sql_gen/account04.txt";
execfile "../sql_gen/account05.txt";
execfile "../sql_gen/account06.txt";
execfile "../sql_gen/account07.txt";
execfile "../sql_gen/account08.txt";
execfile "../sql_gen/account09.txt";
select * from account;

select * from account where id = 12500000;
select * from account where id = 12589999;
select * from account where balance = 103.14;
select * from account where balance = 9.28;
select * from account where name = "name56789"; -- t1
select * from account where id < 12509999;
select * from account where id < 12589999;
select * from account where balance < 9.28;
select * from account where balance => 9.28;
select * from account where name < "name09997";
select * from account where name < "name89997";

select id, name from account where balance >= 899.78 and balance < 914.43;
select name, balance from account where balance > 927.95 and id <= 12589991;
select * from account where id < 12515000 and name > "name14500";
select * from account where id < 12500200 and name < "name00100"; -- t5

insert into account values(12589980, "name89980", 722.66);

create index idx01 on account(name);
select * from account where name = "name56789"; -- t2 < t1
select * from account where name = "name45678"; -- t3
select * from account where id < 12500200 and name < "name00100"; -- t6 ? t5
delete from account where name = "name45678";
insert into account values(?, "name45678", ?);
drop index idx01;
select * from account where name = "name45678"; -- t4 > t3

update account set id = 120, balance = 0 where name = "name56789";
select * from account where name = "name56789";
select * from account where id = 120;
select * from account where balance = 0;

delete from account where balance = 0;
select * from account where name = "name56789";
select * from account where id = 120;
delete from account;
select * from account;
drop table account;
show tables;