drop table if exists procedure_orders;
drop table if exists patient_data;

CREATE TABLE procedure_orders (
id serial NOT NULL,
procedure_id int NOT NULL,
encounter_id int NOT NULL,
order_time timestamp NULL DEFAULT NULL,
procedure_name varchar(128) DEFAULT NULL,
order_class varchar(128) DEFAULT NULL,
order_variety varchar(128) DEFAULT NULL,
PRIMARY KEY (id),
CONSTRAINT orders_procedure_id UNIQUE (procedure_id));

create index encounter_id on procedure_orders (encounter_id, order_time);

CREATE TABLE patient_data (
id serial NOT null,
encounter_id int DEFAULT NULL,
admit_time timestamp NULL DEFAULT NULL,
discharge_time timestamp NULL DEFAULT NULL,
discharge_department varchar(128) DEFAULT NULL,
final_drg varchar(128) DEFAULT NULL,
admitting_icd varchar(128) DEFAULT NULL,
PRIMARY KEY (id),
constraint patient_encounter_id unique (encounter_id));