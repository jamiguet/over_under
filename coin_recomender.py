import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import sqlalchemy as sq
import plotly.graph_objs as go
from plotly.subplots import make_subplots


@st.cache_data
def fetch_interesting_coins(_db):
    with open('markets_of_interest.sql', 'r') as sql_file:
        sql_str = sql_file.read()

    assert isinstance(sql_str, str), "SQL query must be a string"
    return pd.read_sql(sql_str, _db)


@st.cache_data
def fetch_annotated_coin(_db, coin, start, end):
    sql_str = f""" with annotate_coin as (select symbol,
       date(timestamp) as date,
       volume,
       close,
       avg(close)
       over (partition by symbol order by timestamp rows between 365 preceding and current row) as mac_2y,
       avg(volume)
       over (partition by symbol order by timestamp rows between 365 preceding and current row) as mav_1y
from ohlc.candlesticks
where symbol = '{coin}')
 select * 
 from annotate_coin
 where date between '{start}' and '{end}'"""
    return pd.read_sql(sql_str, _db)


@st.cache_data
def fetch_time_range(_db, coin):
    sql_str = (f"SELECT symbol, min(date(timestamp)) as earliest, max(date(timestamp)) as latest "
               f"from ohlc.candlesticks "
               f"where symbol = '{coin}' "
               f"group by symbol")
    return pd.read_sql(sql_str, _db)


@st.cache_data
def fetch_all_coins(_db):
    sql_str = f"""select distinct symbol 
    from ohlc.candlesticks
     where not (symbol like '%%3S/%%' or symbol like '%%3L/%%' or symbol like '%%DOWN/%%' or symbol like '%%UP/%%' or
                                 symbol like '%%2S%%' or symbol like '%%2L%%')"""
    return pd.read_sql(sql_str, _db)


st.title("Coin Recommender")


db = st.connection('ohlc', type='sql').connect()

grid_df = fetch_interesting_coins(db)
grid_df.set_index('symbol', inplace=True)

all_coins = st.sidebar.checkbox("Enable all coins", value=False)

if all_coins:
    coins = fetch_all_coins(db)['symbol'].to_list()

else:
    coins = grid_df.index.to_list()

    st.text("Currently attractive coins")
    st.dataframe(grid_df[['volume', 'close', 'under_over', 'traded']], use_container_width=True)


selected_coin = st.sidebar.selectbox("Select coin", options=coins)
start_date = st.sidebar.date_input(label='Start Date',
                                   value=fetch_time_range(db, selected_coin)['earliest'].to_list()[0])
end_date = st.sidebar.date_input(label='End Date', value=fetch_time_range(db, selected_coin)['latest'].to_list()[0])

st.markdown('---')
st.subheader(f"{selected_coin}")
candlesticks_df = fetch_annotated_coin(db, selected_coin, start_date, end_date)
candlesticks_df.set_index('date', inplace=True)

fig = make_subplots(rows=2, cols=1)

trace_close = go.Scatter(
    x=candlesticks_df.index,
    y=candlesticks_df['close'],
    mode='lines',
    name='close'
)

trace_mac_2y = go.Scatter(
    x=candlesticks_df.index,
    y=candlesticks_df['mac_2y'],
    mode='lines',
    name='under'
)

trace_mac_2y5 = go.Scatter(
    x=candlesticks_df.index,
    y=candlesticks_df['mac_2y'] * 3,
    mode='lines',
    name='over'
)

trace_volume = go.Bar(
    x=candlesticks_df.index,
    y=candlesticks_df['volume'],
    name='volume'
)

trace_volume_avg = go.Scatter(
    x=candlesticks_df.index,
    y=candlesticks_df['mav_1y'],
    mode='lines',
    name='volume avg'
)

fig.add_trace(trace_mac_2y5, row=1, col=1)
fig.add_trace(trace_close, row=1, col=1)
fig.add_trace(trace_mac_2y, row=1, col=1)
fig.add_trace(trace_volume, row=2, col=1)
fig.add_trace(trace_volume_avg, row=2, col=1)

fig.update_layout(height=600, width=800, title_text='Close, MAC 2Y, and Volume plot')
st.plotly_chart(fig)
