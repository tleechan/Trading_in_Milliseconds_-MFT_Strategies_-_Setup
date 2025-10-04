"""
    ############################# DISCLAIMER #############################
    This is a strategy template only and should not be
    used for live trading without appropriate backtesting and tweaking of
    the strategy parameters.
    ######################################################################
"""

# Import libraries
import pandas as pd
import numpy as np
import datetime
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Alpaca libraries
import alpaca_trade_api as tradeapi
from alpaca_trade_api.stream import Stream


# Initialize dataframes and variables
trades = pd.DataFrame()
df_trade = pd.DataFrame()
curr_pos = 0
sym = 'AAPL'
qty = 1

# Enter your API key and Secret key
ALPACA_API_KEY = ''
ALPACA_SECRET_KEY = ''

# REST api for placing orders
api = tradeapi.REST(ALPACA_API_KEY, ALPACA_SECRET_KEY,'https://paper-api.alpaca.markets')

async def print_trade(t):

    try:
        global trades
        global curr_pos
        global df_trade
        global sym
        global qty

        # Append all latest trades to a dataframe
        latest_trade = pd.DataFrame(t, index = [0])
        latest_trade_original_time = latest_trade['t'].iloc[-1]
        latest_trade_new_time = datetime.fromtimestamp(latest_trade_original_time.seconds + latest_trade_original_time.nanoseconds / 1e9)
        latest_trade['t'].iloc[-1] = latest_trade_new_time
        trades = pd.concat([trades, latest_trade], ignore_index=True)
        print((trades['t'].iloc[-1] - trades['t'].iloc[0]))


        # Calculate order flow
        trades['tick_direction'] = trades['p'].diff().apply(np.sign)
        trades['tick_direction'] = trades['tick_direction'].replace(0, method='ffill')
        trades['order_flow'] = trades['tick_direction'] * trades['s']
        trades['cumulative_order_flow'] = trades['order_flow'].cumsum()
        print(trades)


        if((trades['t'].iloc[-1] - trades['t'].iloc[0]) > timedelta(seconds=120)):
            latest_2_mins = trades['t'].iloc[-1] - timedelta(seconds=120)
            trades = trades[trades['t'] > latest_2_mins]
            latest_1_min = trades['t'].iloc[-1] - timedelta(seconds=60)
            df_trade = trades[trades['t'] > latest_1_min]

            # Calculate net order flow for last 1 minute
            order_flow = df_trade['cumulative_order_flow'].iloc[-1] - df_trade['cumulative_order_flow'].iloc[0]
            print('df_trade', df_trade)   

            # Check for entry if there is no open position
            if (curr_pos == 0):
                if (order_flow > 0):

                    # Enter long position
                    print('Entering long position')

                    # Submit order
                    api.submit_order(
                        symbol= sym,
                        qty=qty,
                        side='buy',
                        type='market',
                        time_in_force='ioc'
                        )

                    # Set current position to 1 -> buy order
                    curr_pos = 1
                    print('pos is ', curr_pos)

                elif (order_flow < 0):

                    # Enter long position
                    print('Entering short position')

                    # Submit order
                    api.submit_order(
                        symbol= sym,
                        qty=qty,
                        side='sell',
                        type='market',
                        time_in_force='ioc'
                        )

                    # Set current position to -1 -> sell order
                    curr_pos = -1
                    print('pos is ', curr_pos)

            # Check if there is an open long position
            elif (curr_pos > 0):

                # Check if the order flow has fallen below the exit threshold or risen above the take profit threshold
                if (order_flow <= 0):

                    # Exit long position
                    print('Exiting long')

                    api.close_all_positions()

                    # Set pos to 0 since long trade was exited
                    curr_pos = 0
                    print('pos is ', curr_pos)

            # Check if there is an open short position
            elif (curr_pos < 0):

                # Check if the order flow has risen above the exit threshold or fallen below the take profit threshold
                if (order_flow >= 0):

                    # Exit short position
                    print('Exiting short')

                    api.close_all_positions()


                    # Set pos to 0 since short trade was exited
                    curr_pos = 0
                    print('pos is ', curr_pos)

    except:
        pass



def main():
    # Data Stream for live tick data
    stream = Stream(ALPACA_API_KEY,ALPACA_SECRET_KEY, raw_data=True)
    stream.subscribe_trades(print_trade, 'AAPL')

    stream.run()

if __name__ == "__main__":
    main()


