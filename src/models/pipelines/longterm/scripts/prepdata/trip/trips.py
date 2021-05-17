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
    
    
    def starts(df):
        mask = df['time'].diff() > pd.Timedelta(trip_separator)
        df_starts = df.loc[mask]

        return df_starts

    def ends(df):

        mask = df['time'].diff() > pd.Timedelta(trip_separator)
        mask = np.roll(mask,-1)
        mask[-1] = False
        df_ends = df.loc[mask]

        return df_ends


    df_starts = df.map_partitions(func=starts).compute()
    df_ends = df.map_partitions(func=ends).compute()
    
    # Removing end of first incomplete trip
    if df_ends.iloc[0]['time'] < df_starts.iloc[0]['time']:
        df_ends=df_ends.iloc[1:]
    
    # Removing start of last incomplete trip
    if df_starts.iloc[-1]['time'] > df_ends.iloc[-1]['time']:
        df_starts=df_starts.iloc[0:-1]
            
    assert len(df_starts) == len(df_ends)

    return df_starts,df_ends

def numbering(df:pd.DataFrame, start_number:int, trip_separator='0 days 00:00:20')->pd.DataFrame:
    """Add a trip number to each row in df

    Parameters
    ----------
    df : pd.DataFrame
        data (all data or partition of data)
    start_number : 
        start of the numbering (not 0 if this is not the first dask partion)

    Returns
    -------
    pd.DataFrame
        df
    """
    
    df_starts, df_ends = get_starts_and_ends(df=df, trip_separator=trip_separator)

    end_number = start_number + len(df_starts)
    df_starts['trip_no'] = np.arange(start_number, end_number,dtype=int)
    for (start_time, start), (end_time, end) in zip(df_starts.iterrows(), df_ends.iterrows()):
        
        mask = ((start_time <= df.index) & 
                (df.index <= end_time)
               )
        
        df.loc[mask,'trip_no'] = start['trip_no']
        
    #df = df.dropna(subset=['trip_no'])  # drop unfinnished trips

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

    df_2 = numbering(df=df, trip_separator=trip_separator)
    
    groups = df_2.groupby(by='trip_no')
    trip_time = groups['trip_no'].transform(lambda x : x['time'] - x.iloc[0]['time'] )
    df_2['trip_time'] = pd.TimedeltaIndex(trip_time)

    df_2 = redefine_heading(df_2)  # Note!
    

    # 0 : Helsingör -> Helsingborg
    # 1 : Helsingör <- Helsingborg
    df_2['trip_direction'] = groups['longitude'].transform(lambda x : 0 if(x[0] < 12.65) else 1)   



    return df_2

def redefine_heading(df:pd.DataFrame)->pd.Series:
    """The double ended ferry is run in reverse direction half of the time.
    This means that the "heading" measures by the compas is 180 degrees wrong, compared to course over ground from GPS.
    This method makes this 180 degrees heading shift whenever needed.

    Parameters
    ----------
    trip : pd.DataFrame
        [description]

    Returns
    -------
    pd.Series
        corrected heading
    """

    trips = df.groupby(by='trip_no')

    for trip_no, trip in trips:
        
        heading = trip['heading']
        cog = trip['cog']

        if ((cog - heading).mean() > 90):
            df.loc[trip.index,'heading'] = np.mod(heading + 180, 360)
            df.loc[trip.index,'reversing'] = True
        else:
            df.loc[trip.index,'reversing'] = False
            
    
    return df


