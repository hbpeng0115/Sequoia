# -*- encoding: UTF-8 -*-

import datetime
import logging
import time

import akshare as ak
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

import data_fetcher
import push
import settings
import strategy.enter as enter
from strategy import backtrace_ma250
from strategy import breakthrough_platform
from strategy import high_tight_flag
from strategy import keep_increasing
from strategy import low_backtrace_increase
from strategy import parking_apron
from strategy import turtle_trade, climax_limitdown


# Retry-enabled session
def get_retry_session():
    session = requests.Session()
    retries = Retry(
        total=5,
        backoff_factor=1,  # Exponential backoff: 1s, 2s, 4s, etc.
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"]  # Replaces `method_whitelist`
    )
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session


def fetch_data_with_retry():
    """
    Fetches data with retry logic for robust handling of network issues.
    """
    session = get_retry_session()
    try:
        all_data = ak.stock_zh_a_spot_em()
        return all_data
    except Exception as e:
        logging.error(f"Failed to fetch data with retry logic: {e}")
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

    if datetime.datetime.now().weekday() == 0:
        strategies['均线多头'] = keep_increasing.check

    process(stocks, strategies)

    logging.info("************************ process   end ***************************************")


def process(stocks, strategies):
    stocks_data = data_fetcher.run(stocks)
    strategy_results = {}

    # Execute each strategy and save results
    for strategy, strategy_func in strategies.items():
        strategy_results[strategy] = check(stocks_data, strategy, strategy_func)
        time.sleep(2)

    # Filter combination signals
    strong_trend_stocks = strong_trend_signal(strategy_results)
    if strong_trend_stocks:
        classified = classify_by_exchange([(code,) for code in strong_trend_stocks], all_data=fetch_data_with_retry())
        push.strategy("强烈趋势信号的股票分类：\n{}".format(classified))
    else:
        push.strategy("强烈趋势信号的股票不存在。")

    pullback_stocks = pullback_buy_signal(strategy_results)
    if pullback_stocks:
        classified = classify_by_exchange([(code,) for code in pullback_stocks], all_data=fetch_data_with_retry())
        push.strategy("回调低吸信号的股票分类：\n{}".format(classified))
    else:
        push.strategy("回调低吸信号的股票不存在。")

    breakout_stocks = short_term_breakout_signal(strategy_results)
    if breakout_stocks:
        classified = classify_by_exchange([(code,) for code in breakout_stocks], all_data=fetch_data_with_retry())
        push.strategy("短线突破机会的股票分类：\n{}".format(classified))
    else:
        push.strategy("短线突破机会的股票不存在。")


def check(stocks_data, strategy, strategy_func):
    end = settings.config['end_date']
    m_filter = check_enter(end_date=end, strategy_fun=strategy_func)
    results = dict(filter(m_filter, stocks_data.items()))
    if len(results) > 0:
        push.strategy(
            '**************"{0}"**************\n{1}\n**************"{0}"**************\n'.format(
                strategy, list(results.keys())
            )
        )
    return list(results.keys())


def check_enter(end_date=None, strategy_fun=enter.check_volume):
    def end_date_filter(stock_data):
        if end_date is not None:
            if end_date < stock_data[1].iloc[0].日期:  # 该股票在end_date时还未上市
                logging.debug("{}在{}时还未上市".format(stock_data[0], end_date))
                return False
        return strategy_fun(stock_data[0], stock_data[1], end_date=end_date)

    return end_date_filter


def strong_trend_signal(strategy_results):
    volume_up = set(strategy_results.get('放量上涨', []))
    ma_up = set(strategy_results.get('均线多头', []))
    platform_break = set(strategy_results.get('突破平台', []))
    return list(volume_up & ma_up & platform_break)


def pullback_buy_signal(strategy_results):
    backtrace_ma = set(strategy_results.get('回踩年线', []))
    ma_up = set(strategy_results.get('均线多头', []))
    low_backtrace = set(strategy_results.get('无大幅回撤', []))
    return list(backtrace_ma & ma_up & low_backtrace)


def short_term_breakout_signal(strategy_results):
    volume_up = set(strategy_results.get('放量上涨', []))
    parking_apron = set(strategy_results.get('停机坪', []))
    flag_shape = set(strategy_results.get('高而窄的旗形', []))
    return list(volume_up & parking_apron & flag_shape)


def classify_by_exchange(stocks, all_data):
    classified = {
        "上交所": [],
        "深交所": [],
        "北交所": [],
        "科创板": []
    }

    # Create a dictionary that maps stock codes to stock names
    stock_info = {row['代码']: row['名称'] for _, row in all_data.iterrows()}

    for stock_tuple in stocks:
        # If stock_tuple is a nested tuple, extract the inner tuple
        if isinstance(stock_tuple, tuple) and len(stock_tuple) == 1 and isinstance(stock_tuple[0], tuple):
            stock_tuple = stock_tuple[0]

        # Ensure that each stock in `stocks` is a tuple with exactly two elements
        if len(stock_tuple) != 2:
            logging.warning(f"Skipping invalid stock data: {stock_tuple}")
            continue

        # Unpack tuple safely
        stock_code, _ = stock_tuple

        # Look up the stock name from the stock_info dictionary
        name = stock_info.get(stock_code, "未知名称")
        if stock_code.startswith("60"):
            classified["上交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("00") or stock_code.startswith("30"):
            classified["深交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("8"):
            classified["北交所"].append(f"{stock_code} ({name})")
        elif stock_code.startswith("68"):
            classified["科创板"].append(f"{stock_code} ({name})")

    return classified


def statistics(all_data, stocks):
    limitup = len(all_data.loc[(all_data['涨跌幅'] >= 9.5)])
    limitdown = len(all_data.loc[(all_data['涨跌幅'] <= -9.5)])

    up5 = len(all_data.loc[(all_data['涨跌幅'] >= 5)])
    down5 = len(all_data.loc[(all_data['涨跌幅'] <= -5)])

    msg = "涨停数：{}   跌停数：{}\n涨幅大于5%数：{}  跌幅大于5%数：{}".format(limitup, limitdown, up5, down5)
    push.statistics(msg)
