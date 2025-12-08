import azure.functions as func
import logging
import sys
from datetime import datetime

import yfinance as yf
import pypyodbc
from credential import username, password, server, database

# Connection string using ODBC Driver 17 (pre-installed in Azure Functions)
# Connection string using ODBC Driver 17 (pre-installed in Azure Functions)
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}"



app = func.FunctionApp()

@app.schedule(schedule="0 30 13 * * *", arg_name="mytimer", run_on_startup=True)
def timer_triggered_stock_update(mytimer: func.TimerRequest) -> None:
    if mytimer.past_due:
        logging.info('The timer is past due!')

    logging.info('Python timer trigger function started')
    try:
        update_all()
        logging.info("Timer triggered stock update completed successfully")
    except Exception as e:
        logging.error(f"Error in timer triggered update: {str(e)}")



def get_latest_price_and_name(symbol, market_type):
    query_symbol = f"{symbol}.HK" if market_type == "HK" else symbol
    try:
        ticker = yf.Ticker(query_symbol)
        data = ticker.history(period="1d")
        info = ticker.info
        company_name = info.get('shortName', '')
        if not data.empty:
            price = round(data['Close'].iloc[-1], 2)
            return price, company_name
        else:
            return None, company_name
    except Exception as e:
        logging.error(f"Error fetching from Yahoo Finance for {query_symbol}: {e}")
        return None, None


def update_all(market_type=None):
    """Update all stocks in the DB. market_type can be 'HK', 'US', or None for all."""
    try:
        logging.info(f"Connecting to database...")
        conn = pypyodbc.connect(conn_str)
        cursor = conn.cursor()

        if market_type:
            logging.info(f"Fetching stocks for market: {market_type}")
            cursor.execute("""
                SELECT StockNumber, Symbol, CompanyName, MarketType
                FROM [dbo].[test-myproject-table]
                WHERE MarketType = ?
            """, (market_type,))
        else:
            logging.info("Fetching stocks for all markets")
            cursor.execute("""
                SELECT StockNumber, Symbol, CompanyName, MarketType
                FROM [dbo].[test-myproject-table]
            """)

        rows = cursor.fetchall()
        total = len(rows)
        if total == 0:
            logging.info("No stocks found to update.")
            return

        logging.info(f"Updating {total} stocks (market={market_type or 'ALL'})...")
        updated = 0
        failed = 0

        for stock_id, symbol, existing_name, mkt in rows:
            # For HK, symbol may be stored as zero-padded; ensure correct query
            try:
                price, company_name = get_latest_price_and_name(symbol, mkt)
                if price is None:
                    logging.warning(f"✗ No data for {symbol} ({mkt})")
                    failed += 1
                    continue

                ts = datetime.now()

                # Insert into history table
                cursor.execute("""
                    INSERT INTO StockPriceHistory (StockNumber, Symbol, CompanyName, Price, Timestamp, MarketType)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (stock_id, symbol, company_name, price, ts, mkt))

                # Update main table with latest price and name
                cursor.execute("""
                    UPDATE [dbo].[test-myproject-table]
                    SET Price = ?, Timestamp = ?, CompanyName = ?
                    WHERE StockNumber = ?
                """, (price, ts, company_name, stock_id))

                conn.commit()
                updated += 1
                logging.info(f"✓ Updated {symbol} ({mkt}): {price}")
            except Exception as e:
                logging.error(f"✗ Error processing {symbol} ({mkt}): {e}")
                conn.rollback()
                failed += 1

        logging.info(f"\nFinished. Updated: {updated}, Failed: {failed}, Total: {total}")

    except Exception as e:
        logging.error(f"Database error: {e}")
    finally:
        try:
            cursor.close()
            conn.close()
        except:
            pass