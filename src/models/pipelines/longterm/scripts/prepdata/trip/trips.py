import pandas as pd
import numpy as np
from pyproj import Proj

def get_starts(df:pd.DataFrame, trip_separator='0 days 00:01:00', separator_max_speed=1.0, initial_speed_separator=0.05)->pd.DataFrame:
    """get start and end of complete trips from df.

    Parameters
    ----------
    df : pd.DataFrame
        Time series for all trips
    trip_separator : str, optional
        Time windows in the dataframe exceeding this time will divide the trips, by default '0 days 00:00:20'
    initial_speed_separator : float
        if the initial speed is lower than this, this is considered as a trip start also.

    Returns
    -------
    (pd.DataFrame, pd.DataFrame)
        df_starts,df_ends
        dataframes with rows from df with starts and ends.
    """
    
    df.sort_index(inplace=True)
    mask = (df.index.to_series().diff() > trip_separator) 

    ## First part can be a begining of a trip:
    if df.iloc[0]['sog'] < initial_speed_separator:
        mask[0] = True

    df_starts = df.loc[mask]
        
    ## Also checking that the speed is low enough at the start:
    mask = df_starts['sog'] < separator_max_speed
    df_starts = df_starts.loc[mask]

    return df_starts.copy()


def numbering(df:pd.DataFrame, start_number:int, trip_separator='0 days 00:00:20', initial_speed_separator=0.05)->pd.DataFrame:
    """Add a trip number to each row in df

    Parameters
    ----------
    df : pd.DataFrame
        data (all data or partition of data)
    start_number : 
        start of the numbering (not 0 if this is not the first dask partion)
    trip_separator : str, optional
        Time windows in the dataframe exceeding this time will divide the trips, by default '0 days 00:00:20'
    initial_speed_separator : float
        if the initial speed is lower than this, this is considered as a trip start also.

    Returns

    Returns
    -------
    pd.DataFrame
        df
    """
    
    df_starts = get_starts(df=df, trip_separator=trip_separator, initial_speed_separator=initial_speed_separator)
    
    end_number = start_number + 1 + len(df_starts)
    trip_numbers = np.arange(start_number + 1, end_number,dtype=int)

    if 'trip_no' in df:
        df['trip_no'] = None 

    df.loc[df_starts.index,'trip_no'] = trip_numbers
    df['trip_no'] = df['trip_no'].fillna(method='ffill')
    df['trip_no'] = df['trip_no'].fillna(start_number)

    return df

def process(df, add_XY=True):

    groups = df.groupby(by='trip_no')
    trip_time = groups['trip_no'].transform(lambda x : x.index - x.index[0] )
    df['trip_time'] = pd.TimedeltaIndex(trip_time).total_seconds()

    df = redefine_heading(df)  # Note!
    

    # 0 : Helsingör -> Helsingborg
    # 1 : Helsingör <- Helsingborg
    df['trip_direction'] = groups['longitude'].transform(lambda x : 0 if(x[0] < 12.65) else 1)

    if add_XY:
        df = add_XY_from_lat_lon(df)


    return df

def add_XY_from_lat_lon(df:pd.DataFrame)->pd.DataFrame:

    
    pp = Proj(proj='utm',zone=33,ellps='WGS84', preserve_units=False)
    xx, yy = pp(df["longitude"].values, df["latitude"].values)

    df["X"] = xx
    df["Y"] = yy 
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

        if ((cog - heading).abs().mean() > 90):
            df.loc[trip.index,'heading'] = np.mod(heading + 180, 360)
            df.loc[trip.index,'reversing'] = True
        else:
            df.loc[trip.index,'reversing'] = False
            
    
    return df




