set linesize 32000


rem CS 448 SQLPLUS Project1

rem Manmohit Sehgal


--- Query 1---
--- Logic:
--- Get the names of customers who have placed (orders)----
--- Get the names of customers the most expensive orders. ----
--- Please note that an order could include many parts. ---
--- Shipping fees should be included. ---




prompt ==============
prompt QUERY 1 OUTPUT
prompt ==============




create view getShippingPrice AS Select od.ono,p.pno,p.price * od.qty + od.sfee as shippingprice from parts p,odetails od where od.pno=p.pno; 

create view totalPrice AS Select sum(h.shippingprice) as totalprice,h.ono from getShippingPrice h group by h.ono; 

select c.cname from customers c, orders o,totalPrice k where c.cno=o.cno AND o.ono=k.ono AND k.totalprice = (Select MAX(l.totalprice) from totalPrice l);

drop view getShippingPrice;
drop view totalPrice;

prompt

--- Query 2 ---
--- Logic:
--- Get the maximum waiting time for all orders in number of days. ----
--- The waiting time for an order is defined as the difference between the shipped date and the received date. ----

--- Note: Assume that all order events happen at the same time every day, say 12:00AM. This should exclude those orders that have shipped date unconfirmed. ---

prompt ==============
prompt QUERY 2 OUTPUT
prompt ==============


select MAX(o.shipped - o.received) from orders o;

prompt


--- Query 3 ---
--- Logic:
--- Get the numbers and names of workers who have only made a sale to a customer ----
--- living in the same zip code as the workers.----
-- still working on --
prompt ==============
prompt QUERY 3 OUTPUT
prompt ==============



-- Simpler way select w.wno, w.wname from workers w where w.wno not in ((select w.wno from workers w, orders o, customers c where w.zip = c.zip and w.wno = o.wno and c.cno=o.cno) UNION (select w.wno from workers w, orders o, customers c where w.zip != c.zip and w.wno = o.wno and c.cno=o.cno));---



create view madeNoSale as select w.wno from workers w, orders o, customers c where w.zip != c.zip and w.wno = o.wno and c.cno=o.cno;
create view madeSale as select w.wno from workers w, orders o, customers c where w.zip = c.zip and w.wno = o.wno and c.cno=o.cno;
set feedback on
select w.wno, w.wname from workers w where w.wno not in ( (select mns.wno from madeNoSale mns) UNION ( select ms.wno from madeSale ms));
set feedback off

drop view madeSale;
drop view madeNoSale;

prompt

--- Query 4 ---
--- Logic:
--- For each reorder level, print the level number OLEVEL, ----
--- the average quantity on hand, ----
--- the maximum and ----
--- minimum quantity on hand of parts for that level, and the number of parts in that level. ---

prompt ==============
prompt QUERY 4 OUTPUT
prompt ==============


select p.olevel, AVG(p.qoh), MAX(p.qoh), MIN(p.qoh), COUNT(*) from PARTS P group by p.olevel;

prompt

--- Query 5 ---
--- Logic:
--- Find the months of year 1995 ----
--- that witness the lowest sales (including shipping fees). ----
--- The months should be in MON format (i.e. JAN, FEB, etc.) ----

prompt ==============
prompt QUERY 5 OUTPUT
prompt ==============



create view year95OrderPrice as select od.ono, p.pno, p.price * od.qty + od.sfee as OrderPrice, to_char(o.received, 'MON') as month from odetails od, parts p, orders o where od.pno = p.pno and od.ono = o.ono and to_char(o.received, 'YY') = '95';

create view year95MonthlySales as ( select npo.month, SUM(npo.OrderPrice) as monthlysales from year95OrderPrice npo group by npo.month);

select ms.month from year95MonthlySales ms where ms.monthlysales = ( select MIN(yms.monthlysales) from year95MonthlySales yms);

drop view year95OrderPrice;
drop view year95MonthlySales;

prompt

--- Query 6 ---
--- Logic:
--- Print the pairs of parts that are bought together the least. ----
--- A pair of parts is bought together when the same customer (regardless of the time) orders them. ---

prompt ==============
prompt QUERY 6 OUTPUT
prompt ==============


create view partsByCustomers AS select o.cno, od.pno from ORDERS o, ODETAILS od where o.ono = od.ono;
create view partsByPairs AS select pbc.pno as firstpart, pbcc.pno as secondpart, COUNT(*) as partscount from partsByCustomers pbc, partsByCustomers pbcc where pbc.cno = pbcc.cno and pbc.pno < pbcc.pno GROUP BY pbc.pno, pbcc.pno;
select pbp.firstpart, pbp.secondpart from partsByPairs pbp where pbp.partscount = (select MIN(pbp.partscount) from partsByPairs pbp);

drop view partsByPairs;
drop view partsByCustomers;

prompt

--- Query 7 ---
--- Logic:
--- Assumption : We have to print all the costs including 0s 
--- The shipping fee could be 0.0 (free shipping) or greater than 0 (customers pay extra fees). ----
--- Print the PNAME and QOH of part for which customers want to pay shipping fees the least. ----
--- This is defined as the ratio of the number of times that part is ordered with a shipping fee over the total number of times it is ordered. ----
--- Also, print the total shipping cost of all orders of that part. If many parts have the same value, print them all.----




prompt ==============
prompt QUERY 7 OUTPUT
prompt ==============



create view totalOrders as select od.pno, count(*) as totalOrderCount, sum(od.sfee) as totalShippingFee from odetails od group by od.pno;
create view totalShipping as select od.pno, count(*) as totalShippingCount from odetails od where od.sfee > 0 or od.sfee = 0 group by od.pno;
create view shipping as select ot.pno, ts.totalShippingCount/ot.totalOrderCount as shippingRatio, ot.totalShippingFee from totalOrders ot, totalShipping ts where ot.pno = ts.pno;

select p.pname, p.qoh, s.totalShippingFee from shipping s, parts p where s.pno = p.pno and s.shippingRatio = (select MIN(s.shippingRatio) from shipping s);

drop view shipping;
drop view totalShipping;
drop view totalOrders;


prompt

--- Query 8 ---
--- Logic:
--- For each part, "define the downfall time" as the longest number of days between any two consecutive times the part is ordered. ----
--- Print all the PNAME and QUANTITY ON HAND of all parts with their downfall time, ----
--- sorted by their downfall time, ----
--- from highest to lowest. ----
--- Ignore parts that are ordered just one time. ----



--- create view sortingDates AS (select od.pno, o.received from odetails od, orders o where od.ono = o.ono group by od.pno, o.received); ---

prompt ==============
prompt QUERY 8 OUTPUT
prompt ==============


create view sortingDates AS select od.pno, o.received from odetails od, orders o where od.ono = o.ono group by od.pno, o.received;

create view highToLow AS select sd.pno, sd.received from sortingDates sd order by sd.received DESC;

create view totalDownfall AS select htl.pno, MAX(htll.received - htl.received) as total_downfall_time from highToLow htl, highToLow htll where htl.pno = htll.pno group by htl.pno;

select p.pname, p.price, td.total_downfall_time from totalDownfall td, parts p where td.pno = p.pno and td.total_downfall_time > 0 order by td.total_downfall_time DESC;

drop view totalDownfall;
drop view highToLow;
drop view sortingDates;

prompt


--- Query 9 ---
--- Logic:
--- Find the names of parts(get all parts)----
--- ordered by someone who buys more than 2 other parts.----

-- need to fix ---

--- buys more than 2 parts, > 2 or = 2 ----

prompt ==============
prompt QUERY 9 OUTPUT
prompt ==============




create view customerBuysPart AS select o.cno from orders o, odetails od where o.ono = od.ono group by o.cno having count(od.pno) = 4 ;

create view customerHasPart AS select od.pno from customerBuysPart cbp, orders o, odetails od where cbp.cno = o.cno and o.ono = od.ono;

select distinct(p.pname) from parts p, customerHasPart chp where p.pno = chp.pno;

drop view customerBuysPart;
drop view customerHasPart;

prompt



--- Query 10 ---
--- Logic:
--- Find the month that has the lowest average sales over the years. ---
--- The sales of a month should include all orders and shipping fees during that month. ---
--- If many months have the same average sales, print them all. ---
--- The month should be in MON format. ---
--- also print the lowest average sales along with the selected month(s).---

prompt ==============
prompt QUERY 10 OUTPUT
prompt ==============


create view totalYears as select count(to_char(o.received, 'MON')) as years from orders o;

create view totalMonthlySales as select to_char(o.received, 'MON') as month, sum(p.price*od.qty + od.sfee) as sales from odetails od, parts p, orders o where od.pno = p.pno and od.ono = o.ono group by to_char(o.received, 'MON');

create view averageSales as select tms.month, tms.sales / ty.years as average_of_sales from totalMonthlySales tms, totalYears ty;

select sa.month, sa.average_of_sales from averageSales sa where sa.average_of_sales = (select MIN(sa.average_of_sales) from averageSales sa);

drop view totalYears;
drop view totalMonthlySales;
drop view averageSales;

prompt











