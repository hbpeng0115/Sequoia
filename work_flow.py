# -*- encoding: UTF-8 -*-

import datetime
import logging

import akshare as ak
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import data_fetcher
import push
import settings
from strategy import (
    enter,
    backtrace_ma250,
    breakthrough_platform,
    high_tight_flag,
    keep_increasing,
    low_backtrace_increase,
    parking_apron,
    turtle_trade,
    climax_limitdown,
)


def get_retry_session():
    """Create a requests session with retry logic for robust handling of network issues."""
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def fetch_data_with_retry():
    """Fetch stock data with retry logic."""
    session = get_retry_session()
    try:
        return ak.stock_zh_a_spot_em()  # Fetch real-time stock data
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        raise


def prepare():
    """Initialize data processing and execute stock strategies."""
    logging.info("************************ process start ***************************************")
    all_data = fetch_data_with_retry()  # Fetch all stock data
    subset = all_data[['代码', '名称']]
    stocks = [tuple(row) for row in subset.values]

    statistics(all_data, stocks)  # Generate initial statistics

    # Define trading strategies
    strategies = {
        '放量上涨': enter.check_volume,
        '均线多头': keep_increasing.check,
        '停机坪': parking_apron.check,
        '回踩年线': backtrace_ma250.check,
        '突破平台': breakthrough_platform.check,
        '无大幅回撤': low_backtrace_increase.check,
        '海龟交易法则': turtle_trade.check_enter,
        '高而窄的旗形': high_tight_flag.check,
        '放量跌停': climax_limitdown.check,
    }

    # Adjust strategy for Mondays
    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    process(stocks, strategies)  # Process stocks with strategies
    logging.info("************************ process end ***************************************")


def process(stocks, strategies):
    """Process stocks using defined strategies and analyze signals."""
    stocks_data = data_fetcher.run(stocks)  # Fetch detailed stock data
    strategy_results = {strategy: check(stocks_data, strategy, func) for strategy, func in strategies.items()}

    analyze_signals(strategy_results)  # Analyze signals from strategies


def analyze_signals(strategy_results):
    """Analyze and classify stock signals from strategy results."""
    signals = {
        "强烈趋势信号": strong_trend_signal(strategy_results),
        "回调低吸信号": pullback_buy_signal(strategy_results),
        "短线突破机会": short_term_breakout_signal(strategy_results),
    }

    for signal_name, stocks in signals.items():
        if stocks:
            classified = classify_by_exchange(stocks, all_data=fetch_data_with_retry())
            push.strategy(f"{signal_name}的股票分类：\n{classified}")
        else:
            push.strategy(f"{signal_name}的股票不存在。")


def check(stocks_data, strategy, strategy_func):
    """Check stocks against a specific strategy and return matching stocks."""
    end = settings.config['end_date']
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = dict(filter(m_filter, stocks_data.items()))

    if results:
        push.strategy(
            f'**************"{strategy}"**************\n{list(results.keys())}\n**************"{strategy}"**************\n'
        )
    return list(results.keys())


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    """Create a filter function to check stock entry criteria."""

    def end_date_filter(stock_data):
        if end_date and end_date < stock_data[1].iloc[0].日期:
            logging.debug(f"{stock_data[0]}在{end_date}时还未上市")
            return False
        return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)

    return end_date_filter


def strong_trend_signal(strategy_results):
    """Identify stocks with strong trend signals."""
    return list(set(strategy_results.get('放量上涨', [])) &
                set(strategy_results.get('均线多头', [])) &
                set(strategy_results.get('突破平台', [])))


def pullback_buy_signal(strategy_results):
    """Identify stocks suitable for pullback buying."""
    return list(set(strategy_results.get('回踩年线', [])) &
                set(strategy_results.get('均线多头', [])) &
                set(strategy_results.get('无大幅回撤', [])))


def short_term_breakout_signal(strategy_results):
    """Identify stocks with short-term breakout opportunities."""
    return list(set(strategy_results.get('放量上涨', [])) &
                set(strategy_results.get('停机坪', [])) &
                set(strategy_results.get('高而窄的旗形', [])))


def classify_by_exchange(stocks, all_data):
    """Classify stocks by exchange based on their codes."""
    classified = {
        "上交所": [],
        "深交所": [],
        "北交所": [],
        "科创板/创业板": [],
    }

    stock_info = {row['代码']: row['名称'] for _, row in all_data.iterrows()}

    for stock_code in stocks:
        # Unpack the stock code safely
        stock_code = stock_code[0] if isinstance(stock_code, tuple) else stock_code
        name = stock_info.get(stock_code, "未知名称")

        # Classify based on stock code prefix
        if stock_code.startswith("60"):
            classified["上交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("00"):
            classified["深交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("8"):
            classified["北交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith(("688", "300")):
            classified["科创板/创业板"].append(f"{stock_code} ({name})")

    return classified


def statistics(all_data, stocks):
    """Generate and push statistical data about stock performance."""
    limitup = len(all_data[all_data['涨跌幅'] >= 9.5])  # Count limit up stocks
    limitdown = len(all_data[all_data['涨跌幅'] <= -9.5])  # Count limit down stocks
    up5 = len(all_data[all_data['涨跌幅'] >= 5])  # Count stocks with >5% increase
    down5 = len(all_data[all_data['涨跌幅'] <= -5])  # Count stocks with < -5% decrease

    # Prepare the statistics message
    msg = (f"涨停数：{limitup}   跌停数：{limitdown}\n"
           f"涨幅大于5%数：{up5}  跌幅大于5%数：{down5}")
    push.statistics(msg)  # Push statistics message