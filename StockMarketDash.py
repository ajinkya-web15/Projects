import streamlit as st
import pandas as pd
import yfinance as yf
import plotly.graph_objects as go

# --- PAGE CONFIGURATION ---
st.set_page_config(
    page_title="Real-Time Stock Market Dashboard",
    page_icon="💹",
    layout="wide"
)

# --- HEADER SECTION ---
st.title("💹 Real-Time Stock Market Dashboard")
st.write("Track and visualize live stock market data for selected companies.")

# --- SIDEBAR FOR USER INPUT ---
st.sidebar.header("User Input")

TICKERS = [
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'NVDA', 'META', 'JPM', 'V', 'JNJ'
]

ticker_symbol = st.sidebar.selectbox("Select a Stock Ticker", TICKERS, index=0)

period = st.sidebar.selectbox(
    "Select Time Period",
    ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', 'ytd', 'max'],
    index=5
)

if period in ['1d', '5d']:
    interval = st.sidebar.selectbox("Select Interval", ['1m', '2m', '5m', '15m', '30m', '60m', '90m'], index=2)
else:
    interval = st.sidebar.selectbox("Select Interval", ['1d', '5d', '1wk', '1mo', '3mo'], index=0)

show_ma = st.sidebar.checkbox("Show Moving Averages (MA)", value=True)
if show_ma:
    ma_days_1 = st.sidebar.slider("MA 1 (days)", 5, 100, 20)
    ma_days_2 = st.sidebar.slider("MA 2 (days)", 5, 100, 50)

# --- DATA FETCHING AND PROCESSING ---
@st.cache_data(ttl=300)
def load_data(ticker, period, interval):
    try:
        data = yf.download(ticker, period=period, interval=interval, auto_adjust=True)
        if data.empty:
            return None, None

        required_columns = ['Open', 'High', 'Low', 'Close']
        if not all(col in data.columns for col in required_columns):
            return None, None

        try:
            company_info = yf.Ticker(ticker).info
            if not company_info:
                company_info = {}
        except Exception:
            company_info = {}

        return data, company_info

    except Exception:
        return None, None

data, company_info = load_data(ticker_symbol, period, interval)

# --- MAIN DASHBOARD ---
if data is not None and company_info is not None:
    company_name = company_info.get('longName', ticker_symbol)
    st.header(f"{company_name} ({ticker_symbol})")

    sector = company_info.get('sector', 'N/A')
    industry = company_info.get('industry', 'N/A')
    st.write(f"*Sector:* {sector} | *Industry:* {industry}")

    latest_price = data['Close'].iloc[-1]

    if len(data) > 1:
        previous_close = data['Close'].iloc[-2]
        price_change = latest_price - previous_close
        price_change_percent = (price_change / previous_close) * 100
    else:
        price_change = 0
        price_change_percent = 0

    latest_price = float(latest_price)
    price_change = float(price_change)
    price_change_percent = float(price_change_percent)

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Latest Price",
        f"${latest_price:,.2f}",
        f"{price_change:,.2f} ({price_change_percent:.2f}%)"
    )

    market_cap = company_info.get('marketCap', 0)
    col2.metric("Market Cap", f"${market_cap / 1e9:.2f}B" if market_cap else "N/A")

    if 'Volume' in data.columns:
        latest_volume = int(data['Volume'].iloc[-1])
        col3.metric("Volume", f"{latest_volume:,}")
    else:
        col3.metric("Volume", "N/A")

    pe_ratio = company_info.get('trailingPE', 0)
    col4.metric("P/E Ratio", f"{pe_ratio:.2f}" if pe_ratio else "N/A")

    if show_ma:
        data_length = len(data)

        if data_length < ma_days_1:
            st.warning(f"Not enough data for {ma_days_1}-day MA. Using {data_length} days.")
            ma_days_1 = data_length

        if data_length < ma_days_2:
            st.warning(f"Not enough data for {ma_days_2}-day MA. Using {data_length} days.")
            ma_days_2 = data_length

        if ma_days_1 > 0:
            data[f'MA{ma_days_1}'] = data['Close'].rolling(window=ma_days_1).mean()
        if ma_days_2 > 0:
            data[f'MA{ma_days_2}'] = data['Close'].rolling(window=ma_days_2).mean()

    # --- PLOTTING ---
    st.subheader("Price Chart")

    fig = go.Figure()

    fig.add_trace(go.Candlestick(
        x=data.index,
        open=data['Open'],
        high=data['High'],
        low=data['Low'],
        close=data['Close'],
        name='Candlestick'
    ))

    if show_ma:
        if f'MA{ma_days_1}' in data.columns:
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[f'MA{ma_days_1}'],
                mode='lines',
                name=f'{ma_days_1}-Day MA',
                line=dict(color='orange', width=1.5)
            ))
        if f'MA{ma_days_2}' in data.columns:
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[f'MA{ma_days_2}'],
                mode='lines',
                name=f'{ma_days_2}-Day MA',
                line=dict(color='purple', width=1.5)
            ))

    fig.update_layout(
        title=f'{ticker_symbol} Stock Price',
        yaxis_title='Price (USD)',
        xaxis_title='Date',
        xaxis_rangeslider_visible=False,
        template='plotly_dark',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    # --- Additional Info ---
    with st.expander("About the Company"):
        summary = company_info.get('longBusinessSummary', 'No summary available.')
        st.write(summary)
        website = company_info.get('website', 'N/A')
        st.write(f"*Website:* {website}")

    with st.expander("View Raw Data"):
        st.dataframe(data.tail(10))

    with st.expander("Additional Metrics"):
        col1, col2, col3 = st.columns(3)

        high_52 = company_info.get('fiftyTwoWeekHigh', None)
        low_52 = company_info.get('fiftyTwoWeekLow', None)
        col1.metric("52-Week High", f"${high_52}" if high_52 else "N/A")
        col1.metric("52-Week Low", f"${low_52}" if low_52 else "N/A")

        beta = company_info.get('beta', None)
        col2.metric("Beta", f"{beta:.2f}" if beta else "N/A")

        dividend_yield = company_info.get('dividendYield', None)
        col3.metric("Dividend Yield", f"{dividend_yield * 100:.2f}%" if dividend_yield else "N/A")

else:
    st.error(f"Could not retrieve data for the ticker '{ticker_symbol}'.")
    st.info("Possible issues:")
    st.write("- Invalid ticker symbol")
    st.write("- Market is closed or API limit reached")
    st.write("- Network issues or no data returned")

# --- FOOTER ---
st.markdown("---")
st.write("Built with ❤️ using Streamlit, Plotly, and yfinance.")
st.caption("Data provided by Yahoo Finance. This is for educational use only.")
