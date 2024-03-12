with enriched_close as (select symbol,
                               date(timestamp),
                               volume,
                               close,
                               avg(close)
                               over (partition by symbol order by timestamp rows between 365 preceding and current row) as mac_2y,
                               avg(volume)
                               over (partition by symbol order by timestamp rows between 365 preceding and current row)       as mav_1y
                        from ohlc.candlesticks),

     ranked_today as (select *,
                             case
                                 when close > mac_2y * 3 then 'over'
                                 when close < mac_2y then 'under'
                                 else 'neither'
                                 end                as under_over,
                             case
                                 when volume > mav_1y * 3 then 'active'
                                 when volume < mav_1y then 'dormant'
                                 else 'present' end as traded

                      from enriched_close
                      where date between date(now() - interval '2 days') and date(now())
                        and not (symbol like '%%3S/%%' or symbol like '%%3L/%%' or symbol like '%%DOWN/%%' or
                                 symbol like '%%2S%%' or symbol like '%%2L%%')
                      order by date desc, under_over desc)

select *
from ranked_today
where traded = 'active'
  and under_over = 'under';