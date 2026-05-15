import asyncio
from core.logging_setup import configure_logging
from db.connection import init_db_pool, close_db_pool
from research.loader import load_trades
from research.indicators import add_all_indicators

configure_logging()


async def main() -> None:
    await init_db_pool()

    df = await load_trades(symbol="BTCUSDT", limit=5000)
    df = add_all_indicators(df)

    print("\n=== Dataset ===")
    print(f"Trades loaded:  {len(df)}")
    print(f"Time range:     {df.index[0]} → {df.index[-1]}")

    print("\n=== Price Summary ===")
    print(f"Low:            {df['price'].min():.2f}")
    print(f"High:           {df['price'].max():.2f}")
    print(f"Current:        {df['price'].iloc[-1]:.2f}")
    print(f"VWAP:           {df['vwap'].iloc[-1]:.2f}")
    print(f"Above VWAP:     {'Yes' if df['above_vwap'].iloc[-1] else 'No'}")

    print("\n=== Returns ===")
    print(f"Mean log return:    {df['log_return'].mean():.6f}")
    print(f"Std log return:     {df['log_return'].std():.6f}")
    print(f"Min return:         {df['log_return'].min():.6f}")
    print(f"Max return:         {df['log_return'].max():.6f}")

    print("\n=== Volatility (last 50 trades) ===")
    print(f"Current:        {df['volatility_50'].iloc[-1]:.6f}")
    print(f"Mean:           {df['volatility_50'].mean():.6f}")
    print(f"Max:            {df['volatility_50'].max():.6f}")

    print("\n=== Momentum (20-trade window) ===")
    print(f"Current:        {df['momentum_20'].iloc[-1]:.2f}")
    direction = "UP" if df['momentum_20'].iloc[-1] > 0 else "DOWN"
    print(f"Direction:      {direction}")

    print("\n=== Stationarity Check ===")
    print("Raw price std:      ", round(df['price'].std(), 4))
    print("Log returns std:    ", round(df['log_return'].std(), 6))
    print("(Returns should be far smaller and more stable than raw prices)")

    await close_db_pool()


if __name__ == "__main__":
    asyncio.run(main())
