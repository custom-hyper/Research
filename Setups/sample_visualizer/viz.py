import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates
from datetime import datetime, timedelta

# Connect to SQLite database
db_path = "crypto_data.db"  # Update with the actual database path
connection = sqlite3.connect(db_path)
cursor = connection.cursor()

# Load table `top_bottom_indicators_data`
top_bottom_df = pd.read_sql_query("SELECT * FROM top_bottom_indicators_data", connection)

# Iterate through each row in the table
for index, row in top_bottom_df.iterrows():
    top_symbol = row['top_symbol']  # Extract top_symbol
    print(top_symbol)
    top_date = datetime.strptime(row['top_timestamp'], '%Y-%m-%d %H:%M:%S')  # Adjusted for timestamp format
    bottom_date = datetime.strptime(row['bottom_timestamp'], '%Y-%m-%d %H:%M:%S')

    # Construct the timeseries table name
    timeseries_table = f"daily_indicators_{top_symbol}"

    # Query timeseries data from 30 days before the top date to 90 days after the bottom date
    start_date = (top_date - timedelta(days=30)).strftime('%Y-%m-%d %H:%M:%S')
    end_date = (bottom_date + timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
    query = f"""
        SELECT timestamp, open, high, low, close 
        FROM {timeseries_table}
        WHERE timestamp BETWEEN '{start_date}' AND '{end_date}'
        ORDER BY timestamp
    """

    try:
        timeseries_df = pd.read_sql_query(query, connection)

        # Convert timestamp to datetime and format for candlestick
        timeseries_df['timestamp'] = pd.to_datetime(timeseries_df['timestamp'])
        timeseries_df['timestamp'] = timeseries_df['timestamp'].map(mdates.date2num)

        # Calculate cumulative returns starting at 0% at top_date and stopping at bottom_date
        top_close = timeseries_df.loc[timeseries_df['timestamp'] == mdates.date2num(top_date), 'close'].iloc[0]
        timeseries_df['cumulative_return'] = ((timeseries_df['close'] / top_close) - 1).fillna(0)
        timeseries_df.loc[timeseries_df['timestamp'] > mdates.date2num(bottom_date), 'cumulative_return'] = None

        # Calculate RSI (14-day)
        delta = timeseries_df['close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        timeseries_df['RSI'] = 100 - (100 / (1 + rs))

        # Prepare data for candlestick chart
        ohlc = timeseries_df[['timestamp', 'open', 'high', 'low', 'close']].values

        # Plot the candlestick chart and cumulative returns
        fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), gridspec_kw={'height_ratios': [3, 1, 1]})
        
        # Set black background for all plots
        fig.patch.set_facecolor('black')
        ax1.set_facecolor('black')
        ax2.set_facecolor('black')
        ax3.set_facecolor('black')

        # Candlestick chart
        candlestick_ohlc(ax1, ohlc, width=0.6, colorup='green', colordown='red', alpha=0.8)
        ax1.axvline(mdates.date2num(top_date), color='red', linestyle='--', label='Top Date')
        ax1.axvline(mdates.date2num(bottom_date), color='green', linestyle='--', label='Bottom Date')
        ax1.set_title(f"Candlestick Chart for {top_symbol}", color='white')
        ax1.set_ylabel("Price", color='white')
        ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
        ax1.tick_params(axis='x', colors='white')
        ax1.tick_params(axis='y', colors='white')
        ax1.legend(facecolor='black', edgecolor='white')
        ax1.grid(color='gray', linestyle='--', linewidth=0.5)

        # Cumulative returns
        cumulative_days = range(len(timeseries_df[timeseries_df['cumulative_return'].notna()]))
        cumulative_returns = timeseries_df['cumulative_return'][timeseries_df['cumulative_return'].notna()] * 100
        ax2.plot(cumulative_days, cumulative_returns, color='yellow', label='Cumulative Return (%)')
        ax2.axhline(0, color='white', linewidth=0.5, linestyle='--')
        ax2.axvline(0, color='red', linestyle='--', label='Top Date')
        ax2.axvline(len(cumulative_days) - 1, color='green', linestyle='--', label='Bottom Date')
        ax2.set_title("Cumulative Returns", color='white')
        ax2.set_ylabel("Return (%)", color='white')
        ax2.tick_params(axis='x', colors='white')
        ax2.tick_params(axis='y', colors='white')
        ax2.legend(facecolor='black', edgecolor='white')
        ax2.grid(color='gray', linestyle='--', linewidth=0.5)

        # RSI chart
        ax3.plot(timeseries_df['timestamp'], timeseries_df['RSI'], color='purple', label='RSI (14)')
        ax3.axhline(70, color='red', linestyle='--', label='Overbought')
        ax3.axhline(30, color='green', linestyle='--', label='Oversold')
        ax3.axvline(mdates.date2num(top_date), color='red', linestyle='--', label='Top Date')
        ax3.axvline(mdates.date2num(bottom_date), color='green', linestyle='--', label='Bottom Date')
        ax3.set_title("Relative Strength Index (RSI)", color='white')
        ax3.set_ylabel("RSI", color='white')
        ax3.tick_params(axis='x', colors='white')
        ax3.tick_params(axis='y', colors='white')
        ax3.legend(facecolor='black', edgecolor='white')
        ax3.grid(color='gray', linestyle='--', linewidth=0.5)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))

        plt.xticks(rotation=45, color='white')
        plt.tight_layout()
        plt.show()

    except Exception as e:
        print(f"Error processing {top_symbol}: {e}")

# Close the database connection
connection.close()
