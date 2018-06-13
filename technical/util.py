"""
    defines utility functions to be used
"""
from pandas import DatetimeIndex, merge, DataFrame, to_datetime


def ticker_to_dataframe(ticker: list) -> DataFrame:
    """
    builds a dataframe based on the given ticker

    :param ticker: See exchange.get_ticker_history
    :return: DataFrame
    """
    cols = ['date', 'open', 'high', 'low', 'close', 'volume']
    frame = DataFrame(ticker, columns=cols)

    frame['date'] = to_datetime(frame['date'],
                                unit='ms',
                                utc=True,
                                infer_datetime_format=True)

    # group by index and aggregate results to eliminate duplicate ticks
    frame = frame.groupby(by='date', as_index=False, sort=True).agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'max',
    })
    frame.drop(frame.tail(1).index, inplace=True)  # eliminate partial candle
    return frame


def resample_to_interval(dataframe, interval):
    """
        resamples the given dataframe to the desired interval. Please be aware you need to upscale this to join the results
        with the other dataframe

    :param dataframe: dataframe containing close/high/low/open/volume
    :param interval: to which ticker value in minutes would you like to resample it
    :return:
    """

    df = dataframe.copy()
    df = df.set_index(DatetimeIndex(df['date']))
    ohlc_dict = {
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last',
        'volume': 'sum'
    }
    df = df.resample(str(interval) + 'min').agg(ohlc_dict).dropna()
    df['date'] = df.index

    return df


def resampled_merge(original, resampled):
    """
    this method merges a resampled dataset back into the orignal data set

    :param original: the original non resampled dataset
    :param resampled:  the resampled dataset
    :return: the merged dataset
    """

    interval = int((original['date'] - original['date'].shift()).min().seconds / 60)
    resampled_interval = int((resampled['date'] - resampled['date'].shift()).min().seconds / 60)

    # no point in interpolating these colums
    resampled = resampled.drop(columns=['date', 'volume'])

    # rename all the colums to the correct interval
    for header in list(resampled):
        # store the resampled columns in it
        resampled['resample_{}_{}'.format(resampled_interval, header)] = resampled[header]

    # drop columns which should not be joined
    resampled = resampled.drop(columns=['open', 'high', 'low', 'close'])

    resampled = resampled.resample(str(interval) + 'min').interpolate(method='nearest')
    resampled['date'] = resampled.index
    resampled.index = range(len(resampled))
    dataframe = merge(original, resampled, on='date', how='left')
    return dataframe
