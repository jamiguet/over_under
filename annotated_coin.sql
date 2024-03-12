select symbol,
       date(timestamp),
       volume,
       close,
       avg(close)
       over (partition by symbol order by timestamp rows between (2 * 365) preceding and current row) as mac_2y,
       avg(volume)
       over (partition by symbol order by timestamp rows between 365 preceding and current row)       as mav_1y
from ohlc.candlesticks
where symbol = '{symbol}'