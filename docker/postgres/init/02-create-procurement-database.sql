select 'create database procurement_db'
where not exists (
    select 1
    from pg_database
    where datname = 'procurement_db'
)\gexec

\connect procurement_db

create table if not exists suppliers (
    supplier_id integer primary key,
    supplier_name text not null unique,
    category text not null,
    country text not null,
    risk_level text not null,
    preferred boolean not null default false
);

create table if not exists purchase_orders (
    po_number text primary key,
    supplier_id integer not null references suppliers(supplier_id),
    business_unit text not null,
    order_date date not null,
    status text not null,
    total_amount numeric(14, 2) not null
);

insert into suppliers (
    supplier_id,
    supplier_name,
    category,
    country,
    risk_level,
    preferred
) values
    (1, 'Siam Industrial Parts', 'MRO', 'Thailand', 'medium', true),
    (2, 'Bangkok Packaging Group', 'Packaging', 'Thailand', 'low', true),
    (3, 'Mekong Logistics Network', 'Logistics', 'Vietnam', 'high', false),
    (4, 'Penang Precision Tools', 'Machinery', 'Malaysia', 'medium', true),
    (5, 'Jakarta Facility Services', 'Facilities', 'Indonesia', 'high', false),
    (6, 'Seoul Automation Lab', 'Automation', 'South Korea', 'low', true)
on conflict (supplier_id) do update set
    supplier_name = excluded.supplier_name,
    category = excluded.category,
    country = excluded.country,
    risk_level = excluded.risk_level,
    preferred = excluded.preferred;

insert into purchase_orders (
    po_number,
    supplier_id,
    business_unit,
    order_date,
    status,
    total_amount
) values
    ('PO-2026-0001', 1, 'Manufacturing', date '2026-01-08', 'approved', 128400.00),
    ('PO-2026-0002', 2, 'Retail', date '2026-01-12', 'approved', 84250.00),
    ('PO-2026-0003', 3, 'Distribution', date '2026-01-21', 'review', 212300.00),
    ('PO-2026-0004', 4, 'Manufacturing', date '2026-02-02', 'approved', 176900.00),
    ('PO-2026-0005', 5, 'Facilities', date '2026-02-10', 'review', 118750.00),
    ('PO-2026-0006', 1, 'Manufacturing', date '2026-02-18', 'approved', 96300.00),
    ('PO-2026-0007', 6, 'Automation', date '2026-03-03', 'approved', 245600.00),
    ('PO-2026-0008', 3, 'Distribution', date '2026-03-14', 'review', 131950.00),
    ('PO-2026-0009', 4, 'Engineering', date '2026-04-01', 'approved', 90400.00),
    ('PO-2026-0010', 5, 'Facilities', date '2026-04-15', 'blocked', 75500.00),
    ('PO-2026-0011', 2, 'Retail', date '2026-05-05', 'approved', 66700.00),
    ('PO-2026-0012', 6, 'Automation', date '2026-05-28', 'approved', 188200.00)
on conflict (po_number) do update set
    supplier_id = excluded.supplier_id,
    business_unit = excluded.business_unit,
    order_date = excluded.order_date,
    status = excluded.status,
    total_amount = excluded.total_amount;

create or replace view supplier_summary as
select
    s.supplier_name,
    s.category,
    s.country,
    sum(po.total_amount)::numeric(14, 2) as total_spend,
    count(po.po_number)::integer as order_count,
    s.risk_level,
    max(po.order_date) as last_order_date
from suppliers s
join purchase_orders po on po.supplier_id = s.supplier_id
group by
    s.supplier_id,
    s.supplier_name,
    s.category,
    s.country,
    s.risk_level;
