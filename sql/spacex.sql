drop database spacex;

create database spacex;

use spacex;

create table launchpads (
  id int AUTO_INCREMENT,
  name tinytext,
  status tinytext,
  location tinytext,
  region tinytext,
  details text,
  primary key(id)
);

create table cores (
  serial char(255) NOT NULL,
  block tinytext,
  status tinytext,
  details tinytext,
  water_landing boolean,
  primary key(serial)
);

create table capsules(
  serial char(255) NOT NULL,
  type tinytext,
  status tinytext,
  details text,
  primary key(serial)
);

create table rockets(
  id int auto_increment,
  active boolean,
  stages int,
  booster tinytext,
  cost_per_launch int,
  company tinytext,
  height float,
  diameter float,
  mass float,
  name tinytext,
  description text,
  engines tinytext,
  primary key(id)
);

create table missions(
  id int auto_increment,
  name tinytext,
  manufacturers tinytext,
  description text,
  primary key(id)
);

create table payloads(
  id char(255),
  reused boolean,
  manufacturer tinytext,
  type tinytext,
  mass int,
  orbit tinytext,
  nationality tinytext,
  customers tinytext,
  mission_id int,
  launch_id int,
  primary key(id)
);

create table launches(
  id int auto_increment,
  flight_number int,
  mission_name tinytext,
  upcoming boolean,
  year int,
  timestamp timestamp,
  is_tentative boolean,
  tbd boolean,
  rocket_id int,
  launchpad_id int, 
  details text,
  success boolean,
  capsule_serial char(255),
  core_serial char(255),
  primary key(id)
);


source capsules.sql;
source cores.sql;
source launches.sql;
source launchpads.sql;
source missions.sql;
source payloads.sql;
source rockets.sql;