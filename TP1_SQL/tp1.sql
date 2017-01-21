
-- requete R0
@etu-synonym

connect bdwa45/bdwa45@ora11

--set autotrace trace explain
set autotrace off

set sqlbl on
set timing off
set linesize 100




select count(*) 
from lineitem 
where l_shipdate <= to_date( '01-01-1998', 'DD-MM-YYYY');

select count (*),avg(l_quantity),l_returnflag,l_linestatus,sum(l_extendedprice*(1-l_tax)) as htt
from lineitem
where  l_shipdate<=to_date('01-01-1998','DD-MM-YYYY') 
group by l_returnflag,l_linestatus;

select s_suppkey,s_name
from region re, supplier su, nation na, part pa
where pa.p_name="T" and pa.p_size="S" and  max(price) and  re.r_regionkey=na.r_regionkey and su.s_nationkey=na.n_nationkey 
order by max(l_price) desc 

select p_size
from Part;

--Question 2 :

select  s_acctbal,s_name,n_name,s_address,s_phone,p_partkey, p_name, p_size 
From Part pa, Region re, Nation na, Supplier sup 
where pa.p_type='STANDARD PLATED NICKEL' and p_size=11 and na.n_name='KENYA'
order by s_acctbal desc;

--Question 3  :
select distinct  l_orderkey,l_extendedprice*(1-l_tax) as ttc
from Customer cus, LineItem li
where cus.c_mktsegment='AUTOMOBILE'and li.l_linestatus='O' and l_shipdate<=to_date('01-01-1998','DD-MM-YYYY')
ORDER by ttc desc;


-- Question 4 :

select o_orderpriority,count(*)
from Orders ord,LineItem lin
where ord.o_orderdate<=to_date('01-03-1998','DD-MM-YYYY') and ord.o_orderdate > to_date('01-01-1998','DD-MM-YYYY') and ord.o_orderkey=lin.l_orderkey and lin.l_receiptdate < lin.l_commitdate 
group by o_orderpriority;


--Question 5 :
select distinct n_name ,sum(o_totalprice)
from Region re, Nation na, Orders ord, Customer cus
where re.r_regionkey=na.n_regionkey and  ord.o_orderdate > to_date('01-01-1997','DD-MM-YYYY') and  ord.o_orderdate <= to_date('01-01-1998','DD-MM-YYYY') and re.r_name='AMERICA'and n_nationkey=c_nationkey and
cus.c_custkey=ord.o_custkey
group by n_name
order by sum(o_totalprice) desc;



-- Question 6 :
select sum(l_extendedprice*l_discount)
from   LineItem lin
where  lin.l_shipdate > to_date('01-01-1997','DD-MM-YYYY') and  lin.l_shipdate  <= to_date('01-01-1998','DD-MM-YYYY') and lin.l_quantity < 10 and lin.l_discount < 0.09 and   lin.l_discount > 0.07;

--Question 7 :
select na1.n_name, na2.n_name, extract(year from l_shipdate),  sum(l_extendedprice*l_discount)
from Supplier sup, Customer cus, Nation na1,Nation na2, Orders ord, LineItem lin
where s_nationkey=na1.n_nationkey and c_nationkey=na2.n_nationkey and cus.c_custkey=ord.o_custkey and lin.l_orderkey=ord.o_orderkey and  lin.l_shipdate > to_date('01-01-1996','DD-MM-YYYY') and  lin.l_shipdate  <= to_date('01-01-1998','DD-MM-YYYY')
group by na1.n_name, na2.n_name, extract(year from l_shipdate) ;


--Question 9 :
select count(p_name)
from part
where p_name LIKE 'khaki%';



select distinct n_name, extract(year from l_shipdate), (l_extendedprice*l_discount-ps_supplycost)as benefice
FROM LineItem lin, Partsupp psup, Part pa, Nation na, Supplier sup 
where pa.p_name LIKE 'khaki%' and sup.s_nationkey=na.n_nationkey and sup.s_suppkey=psup.ps_suppkey and pa.p_partkey=psup.ps_partkey and lin.l_partkey=pa.p_partkey and sup.s_suppkey=lin.l_suppkey 
order by n_name,extract(year from l_shipdate) ;


---Pas sur et certain...

-- Question 11:
select ps_partkey,( ps_supplycost*ps_availqty) 
from Supplier sup, Nation na, Partsupp pas
where  na.n_name='FRANCE' and sup.s_suppkey=pas.ps_suppkey and sup.s_nationkey=na.n_nationkey and ps_supplycost*ps_availqty >
      ( select sum(ps_supplycost*ps_availqty)*0.00001
       from Supplier sup, Nation na, Partsupp pas
       where na.n_name='FRANCE' and sup.s_nationkey=na.n_nationkey and sup.s_suppkey=pas.ps_suppkey)
group by ps_partkey, ( ps_supplycost*ps_availqty)
order by ( ps_supplycost*ps_availqty) desc;


--Je ne sais pas comment résoudre le fait qu'il y est qu'on ne puisse pas faire de sum pour le premier where sum(ps_supply...

--Question 13
select o_comment
from Orders;


select count(o_orderkey), c_custkey
from Orders ord, Customer cus
where ord.o_comment not like 'permanent%' AND ord.o_comment not like 'package%' and ord.o_custkey=cus.c_custke






table ;

desc LineItem; 

