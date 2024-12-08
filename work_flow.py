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


# Retry-enabled session
def get_retry_session():
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
    """Fetch data with retry logic for robust handling of network issues."""
    session = get_retry_session()
    try:
        return ak.stock_zh_a_spot_em()
    except Exception as e:
        logging.error(f"Failed to fetch data: {e}")
        raise


def prepare():
    logging.info("************************ process start ***************************************")
    all_data = fetch_data_with_retry()
    subset = all_data[['代码', '名称']]
    stocks = [tuple(x) for x in subset.values]
    statistics(all_data, stocks)

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

    process(stocks, strategies)
    logging.info("************************ process end ***************************************")


def process(stocks, strategies):
    stocks_data = data_fetcher.run(stocks)
    strategy_results = {strategy: check(stocks_data, strategy, func) for strategy, func in strategies.items()}

    # Filter combination signals
    analyze_signals(strategy_results)


def analyze_signals(strategy_results):
    signals = {
        "强烈趋势信号": strong_trend_signal(strategy_results),
        "回调低吸信号": pullback_buy_signal(strategy_results),
        "短线突破机会": short_term_breakout_signal(strategy_results),
    }

    for signal_name, stocks in signals.items():
        if stocks:
            classified = classify_by_exchange([(code,) for code in stocks], all_data=fetch_data_with_retry())
            push.strategy(f"{signal_name}的股票分类：\n{classified}")
        else:
            push.strategy(f"{signal_name}的股票不存在。")


def check(stocks_data, strategy, strategy_func):
    end = settings.config['end_date']
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = dict(filter(m_filter, stocks_data.items()))

    if results:
        push.strategy(
            f'**************"{strategy}"**************\n{list(results.keys())}\n**************"{strategy}"**************\n'
        )
    return list(results.keys())


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(stock_data):
        if end_date and end_date < stock_data[1].iloc[0].日期:
            logging.debug(f"{stock_data[0]}在{end_date}时还未上市")
            return False
        return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)

    return end_date_filter


def strong_trend_signal(strategy_results):
    return list(set(strategy_results.get('放量上涨', [])) &
                set(strategy_results.get('均线多头', [])) &
                set(strategy_results.get('突破平台', [])))


def pullback_buy_signal(strategy_results):
    return list(set(strategy_results.get('回踩年线', [])) &
                set(strategy_results.get('均线多头', [])) &
                set(strategy_results.get('无大幅回撤', [])))


def short_term_breakout_signal(strategy_results):
    return list(set(strategy_results.get('放量上涨', [])) &
                set(strategy_results.get('停机坪', [])) &
                set(strategy_results.get('高而窄的旗形', [])))


def classify_by_exchange(stocks, all_data):
    classified = {
        "上交所": [],
        "深交所": [],
        "北交所": [],
        "科创板": [],
    }

    stock_info = {row['代码']: row['名称'] for _, row in all_data.iterrows()}

    for stock_tuple in stocks:
        # If stock_tuple is a nested tuple, extract the inner tuple
        if isinstance(stock_tuple, tuple) and len(stock_tuple) == 1 and isinstance(stock_tuple[0], tuple):
            stock_tuple = stock_tuple[0]
        # Unpack tuple safely
        stock_code, _ = stock_tuple

        name = stock_info.get(stock_code, "未知名称")
        if stock_code.startswith("60"):
            classified["上交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith(("00", "30")):
            classified["深交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("8"):
            classified["北交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("68"):
            classified["科创板"].append(f"{stock_code} ({name})")
    return classified


def statistics(all_data, stocks):
    limitup = len(all_data[all_data['涨跌幅'] >= 9.5])
    limitdown = len(all_data[all_data['涨跌幅'] <= -9.5])
    up5 = len(all_data[all_data['涨跌幅'] >= 5])
    down5 = len(all_data[all_data['涨跌幅'] <= -5])

    msg = f"涨停数：{limitup}   跌停数：{limitdown}\n涨幅大于5%数：{up5}  跌幅大于5%数：{down5}"
    push.statistics(msg)
