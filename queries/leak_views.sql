-- Creates all the necessary leak view

set schema 'api_ws_sample';

drop view if exists v_ai_pipeleak_main_leak cascade;

create view v_ai_pipeleak_main_leak as
	select * from
		(select
			arc_id::bigint as id,
			data::date,
			builtdate::date,
			age::numeric,
			case
				when matcat_id::text in ('ACER', 'INOX', 'Pb', 'NC', 'FOR', 'PRFV', 'B') then 'other'::text
				else matcat_id::text
			end as material,
			pnom::numeric,
			dnom::numeric,
			slope::numeric,
			case 
				when n_connec::integer is null then 0::integer 
				else n_connec::integer
			end,
			case 
				when consum2018::integer is null then 0::integer 
				else consum2018::integer
			end as consum,
			length::numeric,
			expl_id::integer,
			station_id::text,
			norm_id::integer as expl_n_id
		from 
			v_ai_pipeleak_raw_leak
			join
			ai_exploitation
			using (expl_id)) a
	where
		dnom > 0 and
		age > 0.1 and
		expl_n_id is not null and
		data BETWEEN '2007-07-01'::date and '2018-12-31'::date;

create materialized view v_ai_pipeleak_train_leak as
select * from v_ai_pipeleak_main_leak where random() < 0.8;

create materialized view v_ai_pipeleak_valid_leak as
select * from v_ai_pipeleak_main_leak a where not exists (select from v_ai_pipeleak_train_leak where a.id = id);