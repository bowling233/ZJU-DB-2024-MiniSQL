show databases;
create database db0;
create database db1;
create database db2;
show databases;
-- t1;
use db0;
show tables;
create table account(
  id int, 
  name char(16) unique, 
  balance float, 
  primary key(id)
);
show tables;
-- t1;