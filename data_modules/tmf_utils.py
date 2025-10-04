#!/usr/bin/env python
# coding: utf-8

# In[ ]:


# For data manipulation
import pandas as pd
import numpy as np
from scipy.stats import norm

# Function to calculate the volume bars
# Input data with columns 'bid', 'ask', 'trade_price', 'trade_size' with timestamp as the index
def volume_bars(threshold, data):
    # Calculate cumulative volume
    data['cumulative_volume'] = data['trade_size'].cumsum()
    # Initialise variables start_time, end_time, volume_sum and volume_bars
    start_time = None
    end_time = None
    volume_sum = 0
    volume_bars_list = []
    # Create a column 'time' to store the trade entry timestamps
    data['time'] = pd.to_datetime(data.index)

    # Iterate over rows
    for index, row in data.iterrows():

        # Check whether the 'volume_sum' variable is equal to zero to confirm start of new volume bar
        if volume_sum == 0:

            # If a new volume bar is starting, 'start_time' variable is set to the value in the 'time' column of the current row
            start_time = row['time']

        # Add volume to sum: Add the value in the 'trade_size' column of the current row to the 'volume_sum' variable
        volume_sum += row['trade_size']

        # Check whether the 'volume_sum' variable is greater than or equal to the 'threshold' value
        if volume_sum >= threshold:

            # If a volume threshold is reached, the 'end_time' variable is set to the value in the 'time' column of the current row
            end_time = row['time']

            # Create a dictionary called 'volume_data' that contains the OHLC data for the current volume bar
            volume_data = {

                # The 'start_time' key is set to the value of 'start_time' variable
                'start_time': start_time,

                # The 'end_time' key is set to the value of 'end_time' variable
                'end_time': end_time,

                # The 'open_price' key is set to the first value in the 'trade_price' column of the row
                # where the 'time' column is equal to the 'start_time' variable
                'open_price': data.loc[data['time'] == start_time, 'trade_price'].iloc[0],

                # The 'high_price' key is set to the maximum value in the 'trade_price' column for rows
                # where the 'time' column is between the 'start_time' and 'end_time' variables
                'high_price': data.loc[start_time:end_time, 'trade_price'].max(),

                # The 'low_price' key is set to the minimum value in the 'trade_price' column for rows
                # where the 'time' column is between the 'start_time' and 'end_time' variables
                'low_price': data.loc[start_time:end_time, 'trade_price'].min(),

                # The 'close_price' key is set to the first value in the 'trade_price' column of the row
                # where the 'time' column is equal to the 'end_time' variable
                'close_price': data.loc[data['time'] == end_time, 'trade_price'].iloc[0],

                # The 'volume' key is set to the value of the 'volume_sum' variable
                'volume': volume_sum
            }

            # Append 'volume_data' dictionary to a list called 'volume_bars_list'
            # which contains the OHLC data for each volume bar.
            volume_bars_list.append(volume_data)

            # Resets the 'start_time', 'end_time', and 'volume_sum' variables to None,
            # which prepares the script to start calculating the next volume bar
            start_time = None
            end_time = None
            volume_sum = 0

    # Total number of volume bars created
    print(
        f'The total number of volume bars created from {len(data)} trades: {len(volume_bars_list)}')

    # Convert the list of volume bars to dataframe
    volume_bars = pd.DataFrame(volume_bars_list)

    # Define columns of the volume_bars dataframe
    volume_bars.columns = ['start_time', 'end_time', 'open', 'high', 'low',
                           'close', 'volume']

    # Set the start time of the volume bars as the index of the volume_bars dataframe
    volume_bars.index = volume_bars.start_time
    return volume_bars


def bvc(data):
    # Calculate price per unit volume
    data['price_per_unit_volume'] = data['close']/data['volume']

    # Calculate change in price per unit volume
    data['price_change'] = data.price_per_unit_volume.diff()

    # Calculate standard deviation of change in price per unit volume
    data['std'] = data['price_change'].expanding().std()

    # Calculate the buy volume and store it in the column 'buy_volume' in the dataframe 'data'
    data['buy_volume'] = data['volume'] * \
        ((data['price_change']/data['std'])
         .apply(lambda x: norm.cdf(x)))

    # Calculate the sell volume and store it in the column 'sell_volume' in the dataframe 'data'
    data['sell_volume'] = data['volume']-data['buy_volume']

    # Calculate the order flow and store it in the column 'order_flow' of the dataframe 'data'
    data['order_flow'] = data['buy_volume']-data['sell_volume']

    # Select columns for the order flow dataframe and drop rows with any missing data
    data = data[['buy_volume', 'sell_volume', 'order_flow']]
    data = data.dropna()

    return data
