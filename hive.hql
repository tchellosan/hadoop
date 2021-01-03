--------------------------------------------------------------------------------
HIVE
--------------------------------------------------------------------------------
hive = ferramenta de processamento de dados em batch que roda em cima do hadoop
motor = map reduce, spark, outros

sudo -u hdfs hdfs dfsadmin -safemode leave

hdfs dfs -mkdir /user/cloudera/locacao
cd /home/cloudera/Downloads
hdfs dfs -put *.csv /user/cloudera/locacao
hdfs dfs -ls /user/cloudera/locacao
hdfs dfs -rm -r /user/cloudera/locacao/*
hdfs dfs -cat /user/cloudera/locacao/locacao.csv

beeline
!connect jdbc:hive2://

create database locacao;
use locacao;
--drop database locacao cascade;

create external table clientes(
idcliente int,
cnh string,
cpf string,
validadecnh date,
nome string,
datacadastro date,
datanascimento date,
telefone string,
status string
) row format delimited fields terminated by ',' stored as textfile;

create external table veiculos(
idveiculos int,
dataaquisicao date,
ano int,
modelo string,
placa string,
status string,
diaria double
) row format delimited fields terminated by ',' stored as textfile;

create external table despachantes(
iddespachante int,
nome string,
status string,
filial string
) row format delimited fields terminated by ',' stored as textfile;

create external table locacao(
idlocacao int,
idcliente int,
iddespachante int,
idveiculo int,
datalocacao date,
dataentrega date,
total double
) row format delimited fields terminated by ',' stored as textfile;

load data inpath '/user/cloudera/locacao/clientes.csv' into table clientes;
load data inpath '/user/cloudera/locacao/veiculos.csv' into table veiculos;
load data inpath '/user/cloudera/locacao/despachantes.csv' into table despachantes;
load data inpath '/user/cloudera/locacao/locacao.csv' into table locacao;

show tables;
desc formatted clientes;
desc database locacao;

desc database locacao;
show databases;

select a.datalocacao, b.nome from locacao a join despachantes b on a.iddespachante = b.iddespachante;
select a.datalocacao, b.nome from locacao a left join despachantes b on a.iddespachante = b.iddespachante;

create table locacao2 as select * from locacao where iddespachante = 2;
create database teste;
create table teste.locacao2 as select * from locacao where iddespachante = 2;
select * from teste.locacao2;
drop database teste cascade;

--------------------------------------------------------------------------------
INGESTÃO - SQOOP
--------------------------------------------------------------------------------
sqoop list-databases --connect jdbc:mysql://localhost/ --username root --password cloudera
sqoop list-tables --connect jdbc:mysql://localhost/retail_db --username root --password cloudera
sqoop import --connect jdbc:mysql://localhost/retail_db --table costumers --username root --password cloudera --hive-import --create-hive-table --hive-table retail_db.costumers;
sqoop import-all-tables --connect jdbc:mysql://localhost/retail_db --username root --password cloudera --hive-import --hive-overwrite --hive-database retail_db --create-hive-table --m 1
sqoop import --connect jdbc:mysql://localhost/retail_db --table categories --username root -password cloudera --hive-import --hive-database retail_db -- check-column category_id --incremental append --last-value 58

insert overwrite directory '/user/cloudera/locacao2' select * from locacao;
insert overwrite local directory '/home/cloudera/locacao2' row format delimited fields terminated by ';' select * from locacao;

--------------------------------------------------------------------------------
PARTIÇÃO
--------------------------------------------------------------------------------

--PARTITION

set hive.exec.dynamic.partition.mode;
set hive.exec.dynamic.partition.mode=nonscrict;

create table locacaoanalitico(
cliente string,
despachante string,
datalocacao date,
total double
) partitioned by (veiculo string);

insert overwrite table locacaoanalitico partition (veiculo)
select cli.nome, des.nome, loc.datalocacao, loc.total, veic.modelo 
from locacao loc 
join despachantes des on(loc.iddespachante = des.iddespachante)
join clientes cli on(loc.idcliente = cli.idcliente)
join veiculos veic on(loc.idveiculo = veic.idveiculo)

--alter table
alter table veiculos change idveiculos idveiculo int;

--BUCKET
create table locacaoanalitico2(
cliente string,
despachante string,
datalocacao date,
total double,
veiculo string
) clustered by (veiculo) into 4 buckets;

insert overwrite table locacaoanalitico2
select cli.nome, des.nome, loc.datalocacao, loc.total, veic.modelo 
from locacao loc 
join despachantes des on(loc.iddespachante = des.iddespachante)
join clientes cli on(loc.idcliente = cli.idcliente)
join veiculos veic on(loc.idveiculo = veic.idveiculo)

--------------------------------------------------------------------------------
TABELA TEMPORARIA
--------------------------------------------------------------------------------
create temporary table temp_despachantes as select * from despachantes;

--------------------------------------------------------------------------------
VIEW
--------------------------------------------------------------------------------
create view if not exists view_locacao as
select cli.nome cliente, des.nome despachante, loc.datalocacao data_locacao, loc.total total, veic.modelo modelo
from locacao loc 
join despachantes des on(loc.iddespachante = des.iddespachante)
join clientes cli on(loc.idcliente = cli.idcliente)
join veiculos veic on(loc.idveiculo = veic.idveiculo);

--------------------------------------------------------------------------------
ORC
--------------------------------------------------------------------------------
create external table clientes_orc(
idcliente int,
cnh string,
cpf string,
validadecnh date,
nome string,
datacadastro date,
datanascimento date,
telefone string,
status string
) stored as orc;

insert overwrite table clientes_orc select * from clientes;

--------------------------------------------------------------------------------
SUPORTE A TRANSAÇÕES
--------------------------------------------------------------------------------
hive opera sobre o hdfs, o hdfs é um sistema de arquivos distribuídos, ou seja, os dados estão espalhados por diversos nós.
hive suporta transações, porém é preciso ajustar o arquivo xml de configurações e criar as tabelas usando algumas clausulas especificas.

set hive.support.concurrency;

update clientes set nome = 'Marcelo Silva' where idcliente = 1;
==> Attempt to do update or delete using transaction manager that does not support these operations. (state=42000,code=10294)

sudo gedit /etc/hive/conf.dist/hive-site.xml
https://docs.cloudera.com/HDPDocuments/HDP2/HDP-2.6.4/bk_data-access/content/ch02s05s01.html

<property>
     <name>hive.support.concurrency</name>
     <value>true</value>
</property>
<property>
     <name>hive.txn.manager</name>
     <value>org.apache.hadoop.hive.ql.lockmgr.DbTxnManager</value>
</property>
<property>
     <name>hive.compactor.initiator.on</name>
     <value>true</value>
</property>
<property>
     <name>hive.compactor.worker.threads</name>
     <value>1</value>
</property>

sudo service hive-server2 restart

create table clientes_trans(
idcliente int,
cnh string,
cpf string,
validadecnh date,
nome string,
datacadastro date,
datanascimento date,
telefone string,
status string
) 
clustered by (status) 
into 2 buckets 
stored as orc
tblproperties('transactional'='true');

insert into clientes_trans select * from clientes;

--------------------------------------------------------------------------------
OTIMIZACAÇÃO - VETORIZAÇÃO
--------------------------------------------------------------------------------
create external table locacao_orc(
idlocacao int,
idcliente int,
iddespachante int,
idveiculo int,
datalocacao date,
dataentrega date,
total double
) stored as orc;

insert into locacao_orc select * from locacao;

select loc.datalocacao, loc.total, cli.nome
from locacao_orc loc join clientes_orc cli on(loc.idcliente = cli.idcliente);

set hive.vectorized.execution.enabled;

--------------------------------------------------------------------------------
OTIMIZACAÇÃO - CBO - COST-BASED OTIMIZATION
--------------------------------------------------------------------------------

select count(*) from locacao_orc;

set hive.cbo.enable=true;
set hive.compute.query.using.stats=true;
set hive.stats.fetch.column.stats=true;
set hive.stats.fetch.partition.stats=true;

set hive.cbo.enable=false;
set hive.compute.query.using.stats=false;
set hive.stats.fetch.column.stats=false;
set hive.stats.fetch.partition.stats=false;

analyze table locacao_orc compute statistics;

--------------------------------------------------------------------------------
OTIMIZACAÇÃO - SPARK
--------------------------------------------------------------------------------

select loc.datalocacao, loc.total, cli.nome
from locacao_orc loc join clientes_orc cli on(loc.idcliente = cli.idcliente);

set hive.execution.engine=spark;
set hive.execution.engine=mr;

--------------------------------------------------------------------------------
IMPALA
--------------------------------------------------------------------------------

impala-shell

show databases;
invalidate metadata;

select count(*) from clientes;

select cli.nome, des.nome, loc.total, veic.modelo 
from locacao loc 
join despachantes des on(loc.iddespachante = des.iddespachante)
join clientes cli on(loc.idcliente = cli.idcliente)
join veiculos veic on(loc.idveiculo = veic.idveiculo)