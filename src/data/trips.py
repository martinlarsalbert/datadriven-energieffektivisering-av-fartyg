import pandas as pd
import numpy as np

def get_starts_and_ends(df:pd.DataFrame, trip_separator='0 days 00:00:20')->(pd.DataFrame, pd.DataFrame):
    """get start and end of complete trips from df.

    Parameters
    ----------
    df : pd.DataFrame
        Time series for all trips
    trip_separator : str, optional
        Time windows in the dataframe exceeding this time will divide the trips, by default '0 days 00:00:20'

    Returns
    -------
    (pd.DataFrame, pd.DataFrame)
        df_starts,df_ends
        dataframes with rows from df with starts and ends.
    """
    
    df.sort_index(inplace=True)

    mask = df.index.to_series().diff() > trip_separator
    
    df_starts = df.loc[mask].copy()
    
    mask = np.roll(mask,-1)
    mask[-1] = False
    df_ends = df.loc[mask].copy()
    
    # Removing end of first incomplete trip
    if df_ends.index[0] < df_starts.index[0]:
        df_ends=df_ends.iloc[1:].copy()
    
    # Removing start of last incomplete trip
    if df_starts.index[-1] > df_ends.index[-1]:
        df_starts=df_starts.iloc[0:-1].copy()
    
        
    assert len(df_starts) == len(df_ends)

    return df_starts,df_ends

def numbering(df:pd.DataFrame, df_starts:pd.DataFrame, df_ends:pd.DataFrame)->pd.DataFrame:
    """Add a trip number to each row in df

    Parameters
    ----------
    df : pd.DataFrame
        [description]
    df_starts : pd.DataFrame
        [description]
    df_ends : pd.DataFrame
        [description]

    Returns
    -------
    pd.DataFrame
        df
        new copy of df with numbering.
    """

    df = df.copy()

    df_starts['trip_no'] = np.arange(len(df_starts),dtype=int)
    for (start_time, start), (end_time, end) in zip(df_starts.iterrows(), df_ends.iterrows()):
        
        mask = ((start_time <= df.index) & 
                (df.index <= end_time)
               )
        
        df.loc[mask,'trip_no'] = start['trip_no']
        
    df.dropna(subset=['trip_no'], inplace=True)  # drop unfinnished trips

    return df

def divide(df:pd.DataFrame, trip_separator='0 days 00:00:20')->pd.DataFrame:
    """Divide into trips and number

    * get_starts_and_ends
    * numbering

    Parameters
    ----------
    df : pd.DataFrame, trip_separator, optional
        [description], by default '0 days 00:00:20')->pd.DataFrame

    Returns
    -------
    [type]
        [description]
    """

    df_starts, df_ends = get_starts_and_ends(df=df, trip_separator=trip_separator)
    df_2 = numbering(df=df, df_starts=df_starts, df_ends=df_ends)
    
    groups = df_2.groupby(by='trip_no')
    trip_time = groups['trip_no'].transform(lambda x : x.index - x.index[0] )
    df_2['trip_time'] = pd.TimedeltaIndex(trip_time)

    return df_2