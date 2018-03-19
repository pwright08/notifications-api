insert into ft_billing (bst_date, template_id, service_id, notification_type, provider, rate_multiplier, international, billable_units, notifications_sent, rate)
select bst_date, template_id, service_id, notification_type, provider, rate_multiplier, international, sum(billable_units) as billable_units, sum(notifications_sent) as notification_sent,
case when notification_type = 'sms' then sms_rate else letter_rate end as rate
from (
	select
	n.id,
		da.bst_date,
		n.template_id,
		n.service_id,
		a.id,
		n.notification_type,
		case
		when notification_type = 'sms' then
			coalesce(sent_by, (select provider[(select floor(random()*2+1))] from (select array['mmg', 'firetext'] as provider) as ff))
		when notification_type = 'letter' then
			coalesce(sent_by, 'dvla')
		else
			coalesce(sent_by, 'ses')
		end as provider,
		coalesce(n.rate_multiplier,1) as rate_multiplier,
		s.crown,
		coalesce((select rates.rate from rates
		where n.notification_type = rates.notification_type and n.sent_at > rates.valid_from order by rates.valid_from desc limit 1), 0) as sms_rate,
		coalesce((select l.rate from letter_rates l where n.rate_multiplier = l.sheet_count and s.crown = l.crown and n.notification_type='letter'), 0) as letter_rate,
		coalesce(n.international, false) as international,
		n.billable_units,
		1 as notifications_sent
	from public.notification_history n
	left join templates t on t.id = n.template_id
	left join dm_datetime da on n.created_at > da.utc_daytime_start and n.created_at < da.utc_daytime_end
	left join services s on s.id = n.service_id
	left join annual_billing a on a.service_id = n.service_id and a.financial_year_start = da.financial_year
	where n.notification_status!='technical-failure'
	and n.key_type!='test'
	and n.notification_status!='created'
	--and n.created_at <= '2018-02-01'
	) as individual_record
group by bst_date, template_id, service_id, notification_type, provider, rate_multiplier, international, sms_rate, letter_rate
order by bst_date