import yfinance as yf
import pandas as pd
import pandas_ta as ta
import os
import time
import argparse


class StockAnalyzer:
    def __init__(self, output_dir='stock_data'):
        self.output_dir = output_dir
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
    
    def fetch_data(self, symbols, period="1y", interval="1d"):
        """
        Fetch stock data for multiple symbols.
        
        Args:
            symbols (list): List of stock symbols
            period (str): Time period to fetch (default: 1 year)
            interval (str): Data interval (default: daily)
            
        Returns:
            dict: Dictionary of DataFrames with stock data
        """
        stock_data = {}
        
        for symbol in symbols:
            try:
                print(f"Fetching data for {symbol}...")
                stock = yf.Ticker(symbol)
                df = stock.history(period=period, interval=interval)
                
                if df.empty:
                    print(f"No data found for {symbol}")
                    continue
                
                df.columns = [col.lower() for col in df.columns]
                
                df['symbol'] = symbol
                
                stock_data[symbol] = df
                print(f"Successfully fetched data for {symbol}")
                
                time.sleep(0.5)
                
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
        
        return stock_data
    
    def add_technical_indicators(self, df):
        """
        Add technical indicators to the DataFrame.
        
        Args:
            df (DataFrame): Stock price data
            
        Returns:
            DataFrame: DataFrame with added technical indicators
        """
        try:
            df['rsi'] = ta.rsi(df['close'], length=14)
            
            bbands = ta.bbands(df['close'], length=20, std=2)
            df = df.join(bbands)
            
            macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
            df = df.join(macd)
            
            df['sma_20'] = ta.sma(df['close'], length=20)
            df['sma_50'] = ta.sma(df['close'], length=50)
            df['sma_200'] = ta.sma(df['close'], length=200)
            
            df['ema_12'] = ta.ema(df['close'], length=12)
            df['ema_26'] = ta.ema(df['close'], length=26)
            
            df['atr'] = ta.atr(df['high'], df['low'], df['close'], length=14)
            
            df['date'] = df.index.date
            df['year'] = df.index.year
            df['month'] = df.index.month
            df['day'] = df.index.day
            df['day_of_week'] = df.index.dayofweek

            df['daily_return'] = df['close'].pct_change() * 100
            
            df['volume_sma_20'] = ta.sma(df['volume'], length=20)
            
            df['volume_above_avg'] = df['volume'] > df['volume_sma_20']
            
            return df
            
        except Exception as e:
            print(f"Error adding technical indicators: {e}")
            return df
    
    def process_all_stocks(self, symbols, period="1y", interval="1d"):
        """
        Process all stocks and save the data to CSV files.
        
        Args:
            symbols (list): List of stock symbols
            period (str): Time period to fetch
            interval (str): Data interval
            
        Returns:
            str: Path to the combined CSV file
        """
        stock_data = self.fetch_data(symbols, period, interval)
        all_stocks_df = pd.DataFrame()
        
        for symbol, df in stock_data.items():
            df_with_indicators = self.add_technical_indicators(df)
            
            output_path = os.path.join(self.output_dir, f"{symbol}_data.csv")
            df_with_indicators.to_csv(output_path)
            print(f"Saved data for {symbol} to {output_path}")
            
            all_stocks_df = pd.concat([all_stocks_df, df_with_indicators])
        
        combined_output_path = os.path.join(self.output_dir,
                                            "all_stocks_data.csv")
        all_stocks_df.to_csv(combined_output_path)
        print(f"Saved combined data to {combined_output_path}")
        
        self.create_summary_table(stock_data, combined_output_path)
        
        return combined_output_path
    
    def create_summary_table(self, stock_data, combined_output_path):
        """Create a summary table with key metrics for each stock."""
        summary_rows = []
        
        for symbol, df in stock_data.items():
            if df.empty:
                continue

            latest = df.iloc[-1]

            daily_change_pct = ((latest['close'] - df.iloc[-2]['close']) /
                                df.iloc[-2][
                                    'close'] * 100) if len(df) > 1 else 0

            week_52_high = df['close'].max()
            week_52_low = df['close'].min()

            pct_from_high = ((latest['close'] - week_52_high) /
                             week_52_high * 100)
            pct_from_low = ((latest['close'] - week_52_low) /
                            week_52_low * 100)
            
            summary_rows.append({
                'symbol': symbol,
                'last_price': latest['close'],
                'daily_change_pct': daily_change_pct,
                'volume': latest['volume'],
                'week_52_high': week_52_high,
                'week_52_low': week_52_low,
                'pct_from_high': pct_from_high,
                'pct_from_low': pct_from_low,
                'date': latest.name.date()
            })
        
        if summary_rows:
            summary_df = pd.DataFrame(summary_rows)
            summary_path = os.path.join(self.output_dir, "stock_summary.csv")
            summary_df.to_csv(summary_path, index=False)
            print(f"Saved summary data to {summary_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Fetch and analyze stock data for multiple symbols'
    )
    parser.add_argument(
        '--symbols', 
        nargs='+', 
        default=['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'META'],
        help=(
            'Stock symbols to analyze '
            '(default: AAPL MSFT GOOGL AMZN META)'
        )
    )
    parser.add_argument(
        '--period', 
        default='1y', 
        help='Period to fetch (default: 1y)'
    )
    parser.add_argument(
        '--interval',
        default='1d',
        help=(
            'Data interval '
            '(default: 1d)'
        )
    )
    parser.add_argument(
        '--output', 
        default='stock_data', 
        help='Output directory (default: stock_data)'
    )
    
    args = parser.parse_args()
    
    analyzer = StockAnalyzer(output_dir=args.output)
    combined_file = analyzer.process_all_stocks(
        args.symbols, args.period, args.interval
    )
    
    print("\nData processing complete. Files are ready for Power BI.")
    print(f"Main data file: {combined_file}")
    print(f"Individual stock files and summary are in the "
          f"'{args.output}' directory.")




