# Databricks notebook source
# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/database_connector

# COMMAND ----------

# MAGIC %run /Workspace/Users/jorgegarciaotero@gmail.com/tfm_databricks/config/logger

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import col, lag, avg, stddev, when, log, avg,abs, lit, last, isnan
from pyspark.sql.functions import greatest, least,  row_number, max as spark_max, expr
from datetime import datetime, timedelta
import sys

# COMMAND ----------

def add_basic_stock_metrics(df):
    """
    Adds daily financial and price-based indicators to stock data.
    New columns added:
    - prev_close: Closing price from the previous trading day (per symbol).
    - prev_volume: Volume from the previous trading day.
    - daily_return: Daily return, calculated as (close - open) / open.
    - close_change_pct: Percentage change of close price vs previous day.
    - intraday_volatility: Daily volatility, calculated as (high - low) / open.
    - price_range: Absolute daily price range (high - low).
    - gap_open: Difference between today's open and previous close as a % of previous close.
    - log_return: Natural logarithm of return between close and previous close.
    - volume_change_pct: Volume change vs previous day, as percentage.
    - is_dividend_day: Binary indicator (1 if dividend > 0, else 0).
    - is_stock_split: Binary indicator (1 if stock_splits > 0, else 0).

    Args:
        df_stock_data (DataFrame): Spark DataFrame with historical stock prices and structure similar to:
            ['date', 'symbol', 'open_v', 'high', 'low', 'close_v', 'volume', 'dividends', 'stock_splits']

    Returns:
        DataFrame: Enriched Spark DataFrame with additional technical and financial columns.
    """
    window_spec = Window.partitionBy("symbol").orderBy("date")

    df = df \
    .withColumn("prev_close", lag("close_v", 1).over(window_spec)) \
    .withColumn("prev_volume", lag("volume", 1).over(window_spec)) \
    .withColumn("daily_return", (col("close_v") - col("open_v")) / col("open_v")) \
    .withColumn("close_change_pct", ((col("close_v") - col("prev_close")) / col("prev_close"))) \
    .withColumn("intraday_volatility", (col("high") - col("low")) / col("open_v")) \
    .withColumn("price_range", col("high") - col("low")) \
    .withColumn("gap_open", (col("open_v") - col("prev_close")) / col("prev_close")) \
    .withColumn("log_return",
        when(
            col("close_v").isNotNull() & col("prev_close").isNotNull() &
            (col("close_v") > 0) & (col("prev_close") > 0),
            log(col("close_v") / col("prev_close"))
        ).otherwise(lit(None))
    ) \
    .withColumn("volume_change_pct",
        when(
            col("prev_volume").isNotNull() & (col("prev_volume") != 0),
            (col("volume") - col("prev_volume")) / col("prev_volume")
        ).otherwise(lit(0))
    ) \
    .withColumn("is_dividend_day", when(col("dividends") > 0, 1).otherwise(0)) \
    .withColumn("is_stock_split", when(col("stock_splits") > 0, 1).otherwise(0))

    df.show()
    return df 


# COMMAND ----------

def add_technical_stock_metrics(df):
    """
    Adds technical indicators to stock price data.

    Includes:
    - SMA 5 and 20 (simple moving averages)
    - RSI 14 (Relative Strength Index)
    - Bollinger Bands (upper/lower based on 20-day SMA and StdDev)
    - Relative volume (vs 5-day average)

    Args:
        df (DataFrame): Spark DataFrame with stock price data, including at least:
            ['date', 'symbol', 'open_v', 'close_v', 'high', 'low', 'volume']

    Returns:
        DataFrame: Spark DataFrame with new technical indicator columns.
    """

    window_spec = Window.partitionBy("symbol").orderBy("date")
    
    # SMA 5 y 20
    df = df.withColumn("sma_5", avg("close_v").over(window_spec.rowsBetween(-4, 0)))
    df = df.withColumn("sma_20", avg("close_v").over(window_spec.rowsBetween(-19, 0)))

    # RSI 14
    delta = col("close_v") - lag("close_v", 1).over(window_spec)
    gain = when(delta > 0, delta).otherwise(0)
    loss = when(delta < 0, -delta).otherwise(0)

    df = df.withColumn("delta", delta)
    df = df.withColumn("gain", gain)
    df = df.withColumn("loss", loss)

    rsi_window = window_spec.rowsBetween(-13, 0)
    avg_gain = avg("gain").over(rsi_window)
    avg_loss = avg("loss").over(rsi_window)

    rs = avg_gain / avg_loss
    rsi = when(avg_gain == 0, 0).when(avg_loss == 0, 100).otherwise(100 - (100 / (1 + rs)))
    df = df.withColumn("rsi_14", rsi)

    # Bandas de Bollinger
    sma_20 = avg("close_v").over(window_spec.rowsBetween(-19, 0))
    std_20 = stddev("close_v").over(window_spec.rowsBetween(-19, 0))

    df = df.withColumn("bollinger_upper", sma_20 + 2 * std_20)
    df = df.withColumn("bollinger_lower", sma_20 - 2 * std_20)

    # Volumen relativo (protegido contra null o cero)
    vol_avg_5 = avg("volume").over(window_spec.rowsBetween(-4, 0))
    df = df.withColumn(
        "rel_volume",
        when(vol_avg_5.isNotNull() & (vol_avg_5 != 0), col("volume") / vol_avg_5).otherwise(0)
    )

    return df


# COMMAND ----------

def add_macd_metrics(df):
    """
    Adds the MACD technical indicator to the stock data.

    - ema_12: 12-day Exponential Moving Average of the closing price.
    - ema_26: 26-day Exponential Moving Average of the closing price.
    - macd_line: Difference between ema_12 and ema_26.
    - macd_signal: 9-day EMA of the macd_line.
    - macd_histogram: Difference between macd_line and macd_signal.

    Args:
        df (DataFrame): Spark DataFrame containing at least the columns ['date', 'symbol', 'close_v'].
    
    Returns:
        DataFrame: Spark DataFrame enriched with MACD-related columns.
    """
    window_spec = Window.partitionBy("symbol").orderBy("date")

    # EMA approximation with rolling average (Spark limitation)
    df = df.withColumn("ema_12", avg("close_v").over(window_spec.rowsBetween(-11, 0)))
    df = df.withColumn("ema_26", avg("close_v").over(window_spec.rowsBetween(-25, 0)))

    # MACD line
    df = df.withColumn("macd_line", when(
        col("ema_12").isNotNull() & col("ema_26").isNotNull(),
        col("ema_12") - col("ema_26")
    ).otherwise(0))

    # Signal line: 9-period average of macd_line
    df = df.withColumn("macd_signal", avg("macd_line").over(window_spec.rowsBetween(-8, 0)))

    # MACD histogram
    df = df.withColumn("macd_histogram", when(
        col("macd_line").isNotNull() & col("macd_signal").isNotNull(),
        col("macd_line") - col("macd_signal")
    ).otherwise(0))

    return df


# COMMAND ----------

def add_atr_metrics(df):
    """
    Adds Average True Range (ATR) to the DataFrame.

    - true_range: max(high - low, abs(high - prev_close), abs(low - prev_close))
    - atr_14: average true range over 14 days

    Args:
        df (DataFrame): Spark DataFrame with ['symbol', 'date', 'high', 'low', 'close_v']

    Returns:
        DataFrame: With 'true_range' and 'atr_14' columns.
    """

    window_spec = Window.partitionBy("symbol").orderBy("date")

    df = df.withColumn("prev_close", lag("close_v", 1).over(window_spec))

    df = df.withColumn("tr_1", col("high") - col("low")) \
           .withColumn("tr_2", when(col("prev_close").isNotNull(), abs(col("high") - col("prev_close"))).otherwise(lit(0))) \
           .withColumn("tr_3", when(col("prev_close").isNotNull(), abs(col("low") - col("prev_close"))).otherwise(lit(0))) \
           .withColumn("true_range", greatest(col("tr_1"), col("tr_2"), col("tr_3"))) \
           .withColumn("atr_14", avg("true_range").over(window_spec.rowsBetween(-13, 0)))

    return df


# COMMAND ----------

def add_candlestick_features(df):
    """
    Adds basic Japanese candlestick features to the DataFrame.

    - candle_body: Absolute difference between close and open.
    - upper_wick: High minus the higher of open or close.
    - lower_wick: Lower of open or close minus the low.
    - candle_color: "green" if close > open, otherwise "red".

    Args:
        df (DataFrame): Spark DataFrame with columns ['open_v', 'close_v', 'high', 'low'].

    Returns:
        DataFrame: With candlestick feature columns.
    """
    df = df.withColumn("candle_body", abs(col("close_v") - col("open_v")))
    df = df.withColumn("upper_wick", col("high") - greatest(col("open_v"), col("close_v")))
    df = df.withColumn("lower_wick", least(col("open_v"), col("close_v")) - col("low"))
    df = df.withColumn("candle_color", when(col("close_v") > col("open_v"), "green").otherwise("red"))
    return df

# COMMAND ----------

def add_momentum_metrics(df):
    """
    Adds momentum and rate of change (ROC) indicators to the stock data.

    - momentum_10: Difference between today's close and close 10 days ago.
    - roc_10: Rate of change (% return over the last 10 days)

    Args:
        df (DataFrame): Spark DataFrame with at least ['symbol', 'date', 'close_v'].

    Returns:
        DataFrame: With columns 'momentum_10' and 'roc_10'.
    """
    window_spec = Window.partitionBy("symbol").orderBy("date")

    prev_close_10 = lag("close_v", 10).over(window_spec)

    df = df.withColumn("momentum_10", col("close_v") - prev_close_10)
    df = df.withColumn(
        "roc_10",
        when(prev_close_10.isNotNull() & (prev_close_10 != 0),
             (col("close_v") - prev_close_10) / prev_close_10
        ).otherwise(lit(0))
    )

    return df


# COMMAND ----------

def add_lagged_returns(df):
    """
    Adds past return features for fixed time horizons:
    1d, 7d, 1m, 3m, 1y, 5y and total since first available date.
    
    Args:
        df (DataFrame): Spark DataFrame with at least ['symbol', 'date', 'close_v']

    Returns:
        DataFrame: Enriched DataFrame with new columns:
        - ret_past_1d
        - ret_past_7d
        - ret_past_1m
        - ret_past_3m
        - ret_past_6m
        - ret_past_1y
        - ret_past_5y
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import lag, first, when, col, lit

    horizons = {
        "1d": 1,
        "7d": 7,
        "1m": 21,
        "3m": 63,
        "6m": 126,
        "1y": 252,
        "5y": 1260
    }

    window = Window.partitionBy("symbol").orderBy("date")

    # Profitability from t-N to t
    for label, days in horizons.items():
        lag_close = lag("close_v", days).over(window)
        df = df.withColumn(
            f"ret_past_{label}",
            when(
                lag_close.isNotNull() & (lag_close != 0),
                (col("close_v") - lag_close) / lag_close
            ).otherwise(lit(None))
        )

    # Profitability from first available date to today
    base_close = first("close_v").over(window.rowsBetween(Window.unboundedPreceding, Window.currentRow))
    df = df.withColumn(
        "ret_total",
        when(
            base_close.isNotNull() & (base_close != 0),
            (col("close_v") - base_close) / base_close
        ).otherwise(lit(None))
    )

    return df


# COMMAND ----------

def compute_var(df, confidence_level=0.95, window=100):
    """
    Calculates rolling Value at Risk (VaR) using historical simulation method.

    Args:
        df (DataFrame): Spark DataFrame with 'symbol', 'date', and 'daily_return'
        confidence_level (float): Confidence level (e.g., 0.95)
        window (int): Rolling window size in days

    Returns:
        DataFrame: with additional column 'var_95' (or similar)
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, expr, percentile_approx

    w = Window.partitionBy("symbol").orderBy("date").rowsBetween(-window + 1, 0)

    var_col = f"var_{int(confidence_level * 100)}"

    df = df.withColumn(var_col, percentile_approx("daily_return", 1 - confidence_level).over(w))

    return df


# COMMAND ----------

def remove_initial_days_per_symbol(df, min_days=20):
    """
    Removes the first `min_days` rows per symbol based on date order.
    
    Args:
        df (DataFrame): Spark DataFrame with at least ['symbol', 'date']
        min_days (int): Number of initial rows to drop per symbol

    Returns:
        DataFrame: Cleaned DataFrame with initial rows removed
    """
    from pyspark.sql.window import Window
    from pyspark.sql.functions import row_number

    w = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("row_num", row_number().over(w))
    df = df.filter(col("row_num") > min_days).drop("row_num")
    return df


# COMMAND ----------

def main(storage_account, container, database_name, date_value, period,time_window,logger):

    if period=='complete':
        start_date=None
        end_date=None
    else:
        date_obj = datetime.strptime(date_value, "%Y-%m-%d")
        window_days = int(time_window)
        start_date = (date_obj - timedelta(days=window_days)).strftime("%Y-%m-%d")
        end_date = date_value
    print(f"start_date: {start_date}")
    print(f"end_date: {end_date}")

    db_connector = DatabaseConnector()
    df_stock_data = db_connector.read_table_from_sql("stock_data", start_date=start_date, end_date=end_date)
    print(f"stock_data count: {df_stock_data.count()}")

    #Addition of features
    df_stock_data = add_basic_stock_metrics(df_stock_data)
    df_stock_data = add_technical_stock_metrics(df_stock_data)
    df_stock_data = add_macd_metrics(df_stock_data)
    df_stock_data = add_atr_metrics(df_stock_data)
    df_stock_data = add_candlestick_features(df_stock_data)
    df_stock_data = add_momentum_metrics(df_stock_data)
    df_stock_data= compute_var(df_stock_data, confidence_level=0.95, window=100)
    df_stock_data = add_lagged_returns(df_stock_data)
    
    df_stock_data.printSchema()
    print(f"count: {df_stock_data.count()}")
    
    if period=='complete':
        print("complete")
        db_connector.save_table( df_stock_data,container,database_name, storage_account,'stock_data')
        
    else:
        print("daily")
        df_stock_data_today = df_stock_data.filter(col("date") == lit(date_value))
        db_connector.save_table( df_stock_data_today,container,database_name, storage_account,'stock_data',date_value)
    

    
    #Other data
    if (
    start_date is None or
    datetime.strptime(start_date, "%Y-%m-%d").day == 1
    ):
        df_company_info = db_connector.read_table_from_sql("company_info")
        df_cashflow = db_connector.read_table_from_sql("cashflow_data")
        df_dividend = db_connector.read_table_from_sql("dividend_data")
        df_financial = db_connector.read_table_from_sql("financial_data")
        df_recommendations = db_connector.read_table_from_sql("recommendations_data")
        df_split = db_connector.read_table_from_sql("split_data")

        df_company_info.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/company_info")
        df_cashflow.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/cashflow_data")
        df_dividend.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/dividend_data")
        df_financial.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/financial_data")
        df_recommendations.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/recommendations_data")
        df_split.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/split_data")

    logger.info(f"Data saved to {container}/{database_name}/stock_data/{date_value}")
    display(df_stock_data)

    #Convert delta to parquet for Azure ML integration
    df = spark.read.format("delta").load("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/stock_data")
    df=remove_initial_days_per_symbol(df, min_days=20)
    df.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/stock_data_parquet")



# COMMAND ----------


if __name__ == '__main__':

    dbutils.widgets.removeAll()
    logger = get_logger(name="normalization", level="INFO", log_file="normalization.log")
    logger.info("Starting ...")   

    # Creates the input widgets and sets the default values
    dbutils.widgets.text("storage_account", "smartwalletjorge", "Storage Account")
    dbutils.widgets.text("container", "smart-wallet-dl", "Container")
    dbutils.widgets.text("database", "smart_wallet", "Database")
    dbutils.widgets.text("period", "complete", "Period")
    dbutils.widgets.text("date", "", "Date")
    dbutils.widgets.text("time_window", "1", "time_window")

    storage_account = dbutils.widgets.get("storage_account")
    container = dbutils.widgets.get("container")
    database_name = dbutils.widgets.get("database")
    date_value = dbutils.widgets.get("date")
    period = dbutils.widgets.get("period")
    time_window = dbutils.widgets.get("time_window")

    main(storage_account, container, database_name, date_value, period,time_window,logger)
    

   



    

# COMMAND ----------

#Convert delta to parquet
df = spark.read.format("delta").load("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/stock_data")
df=remove_initial_days_per_symbol(df, min_days=20)
df.coalesce(1).write.mode("overwrite").format("parquet").save("abfss://smart-wallet-dl@smartwalletjorge.dfs.core.windows.net/smart_wallet/stock_data_parquet")
