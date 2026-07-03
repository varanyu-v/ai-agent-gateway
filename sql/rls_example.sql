-- The local demo uses public World DB rows and seeded procurement rows, so every
-- tenant can read the same sample data. In production, replace these permissive
-- policies with tenant-scoped predicates on your own tables or views.

alter table city enable row level security;

create policy tenant_read_city
on city
for select
using (current_setting('app.tenant_id', true) is not null);

alter table country enable row level security;

create policy tenant_read_country
on country
for select
using (current_setting('app.tenant_id', true) is not null);

alter table country_language enable row level security;

create policy tenant_read_country_language
on country_language
for select
using (current_setting('app.tenant_id', true) is not null);

alter table country_flag enable row level security;

create policy tenant_read_country_flag
on country_flag
for select
using (current_setting('app.tenant_id', true) is not null);

-- Example policies for the seeded procurement database.

alter table suppliers enable row level security;

create policy tenant_read_suppliers
on suppliers
for select
using (current_setting('app.tenant_id', true) is not null);

alter table purchase_orders enable row level security;

create policy tenant_read_purchase_orders
on purchase_orders
for select
using (current_setting('app.tenant_id', true) is not null);
