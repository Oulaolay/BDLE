@etu-synonym
connect bdwa45/bdwa45@ora11

select * from part; --unitaire test --tpc-c le plus repandu --tpc-h utilisation informatique décisionelle

column s_name format A10
column p_type format A10
column STF format A10
column STP format A10
column STR format A10

select p_type,s_name,sum(l_extendedprice)
FROM part p,lineitem lin, supplier sup,partsupp ps
WHERE p.p_type LIKE '%COPPER'
AND lin.l_shipdate<='01/01/1993'
AND sup.s_suppkey=ps.ps_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
GROUP BY ROLLUP(p.p_type,sup.s_name);


SELECT s_name,n_name,r_name
FROM nation nat, region rg, supplier sup
WHERE nat.n_nationkey=sup.s_nationkey
AND rg.r_regionkey=nat.n_regionkey
GROUP BY ROLLUP(rg.r_name,nat.n_name,sup.s_name);


SELECT s_name
FROM supplier sup,nation nat
WHERE sup.s_nationkey=nat.n_nationkey
UNION
GROUP BY(sup.s_name)
SELECT n_name
FROM supplier sup,nation nat,region rg
WHERE nat.n_nationkey=sup.s_nationkey
AND rg.r_regionkey=nat.n_regionkey
UNION
SELECT r_name
FROM region rg, nation nat
WHERE  rg.r_regionkey=nat.n_regionkey;

SELECT p_type
FROM part p,partsupp ps
WHERE p.p_type LIKE '%COOPER'
AND p.p_partkey=ps.ps_partkey
GROUP BY (p.p_type)
UNION
SELECT sum(l_extentedprice)
FROM lineitem lin,partsupp ps,part p
WHERE lin.l_shipdate<='01/01/1993'
AND p.p_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
UNION
SELECT s_name
FROM supplier sup,partsupp ps
WHERE sup.s_suppkey=ps.ps_suppkey
GROUP BY(sup.s_name);


--Question 3 :

column s_name format A10
column p_type format A10
column n_name format A10
column r_name format A10
select p.p_type,sup.s_name,na.n_name,reg.r_name
FROM part p,lineitem lin, supplier sup,partsupp ps, region reg, Nation na
WHERE p.p_type LIKE '%COPPER'
AND lin.l_shipdate<='01/01/1993'
AND sup.s_suppkey=ps.ps_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
AND sup.s_nationkey=na.n_nationkey
AND na.n_nationkey=reg.r_regionkey
AND ps.ps_suppkey=lin.l_suppkey
GROUP BY p.p_type, ROLLUP(reg.r_name,na.n_name,sup.s_name);


--Le select doit être de la forme suivante :
column s_name format A10
column p_type format A10
column STF format A10
column STP format A10
column STR format A10
column TM format A10
select p_type, decode(grouping(sup.s_name), 1, 'ttfour', sup.s_name) as STF,
               decode(grouping (na.n_name), 1, 'toupays', na.n_name)  as STP,
	       decode(grouping(reg.r_name), 1, 'toucon',reg.r_name) as STR,
FROM part p,lineitem lin, supplier sup,partsupp ps, region reg, Nation na
WHERE p.p_type LIKE '%COPPER'
AND lin.l_shipdate<='01/01/1993'
AND sup.s_suppkey=ps.ps_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
AND sup.s_nationkey=na.n_nationkey
AND na.n_nationkey=reg.r_regionkey
GROUP BY p.p_type, ROLLUP(reg.r_name,na.n_name,sup.s_name);




--le decode permet d'avoir plus de lisibilité.
--le select renvoie 'tous fournisseurs s'il y a un 1 dans la colonne grouping(s_name), et renvoie s_name sinon. (idem pour les autres attributs).
--le group by doit être : group by p_type, rollup (r_name, n_name, s_name)

desc orders;


--Question 4

Select o_orderdate as date_order from orders Where o_orderdate <= to_date( '01-01-1998', 'DD-MM-YYYY');


with Date_commande  as
(Select o_orderdate as date_order from orders Where o_orderdate <= to_date( '01-01-1998', 'DD-MM-YYYY'))
select date_order , to_char(date_order, 'MM') as mois, to_char(date_order, 'YYYY') as annee
From Date_commande
Order by date_order;


desc lineitem


select p_type,s_name,sum(l_extendedprice) as price, GROUPING(select p_type,s_name,sum(l_extendedprice)
FROM part p,lineitem lin,supplier sup, partsupp
WHERE sup.s_suppkey=ps.ps_suppkey AND sup.s_suppkey=ps.ps_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey)
FROM part p,lineitem lin, supplier sup,partsupp ps
WHERE p.p_type LIKE '%COPPER'
AND lin.l_shipdate<='01/01/1993'
AND sup.s_suppkey=ps.ps_suppkey
AND p.p_partkey=ps.ps_partkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
GROUP BY ROLLUP(p.p_type,sup.s_name);

-- Question 5: Interrogation d'un cube
column continent format A10
column pays format A10
column type_prod format A10
column nom_prod format A10
column montant_ventes format A10
column fournisseur format A10
with T as
(select r_name as continent, n_name as pays, s_name as fournisseur, p_type as type_prod, p_name as nom_prod, sum(l_quantity*l_extendedprice) as montant_ventes
from Lineitem lin, Region reg, Nation na, Supplier sup, Partsupp ps, Part p
WHERE p.p_partkey=ps.ps_partkey
AND sup.s_nationkey=na.n_nationkey
AND reg.r_regionkey=na.n_regionkey
AND ps.ps_suppkey=sup.s_suppkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
group by cube(r_name, n_name, s_name, p_type, p_name))
select sum(montant_ventes)/count(*)
from T
where continent='AFRICA'and pays='ALGERIA';

select sum(l_quantity*l_extendedprice)
from lineItem lin, Region reg, Partsupp ps, Supplier sup, Nation na, Part p
WHERE p.p_partkey=ps.ps_partkey
AND sup.s_nationkey=na.n_nationkey
AND reg.r_regionkey=na.n_regionkey
AND ps.ps_suppkey=sup.s_suppkey
AND ps.ps_partkey=lin.l_partkey
AND ps.ps_suppkey=lin.l_suppkey
AND reg.r_name='AFRICA' and na.n_name='ALGERIA';


select sum(l_quantity*l_extendedprice)
from Lineitem;

--where rownum <=10;




--Il faut créer une vue LEsCubes avec
--create view LesCubes as
--select r_name as continent, n_name as pays, s_name as fournisseur, p_type as type_prod, p_name as nom_prod, sum(l_quantityt*l_extendedprice as montant_ventes
--from...
--where...
--group by cube(r_name, n_name, s_name, p_type, p_name)

--puis on interroge cette vue (question R2). Pour R1, on fait la requête directement sur le schéma.
