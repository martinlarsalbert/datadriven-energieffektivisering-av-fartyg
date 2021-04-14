import folium
import pandas as pd

def plot_map(df:pd.DataFrame, time_step='30S'):


    df_ = df.resample(time_step).mean()
    df_.dropna(subset=['latitude','longitude'], inplace=True)

    mask = df_['cog'] < 150
    df_out = df_.loc[mask]
    df_home = df_.loc[~mask]

    my_map = folium.Map(location=(df_['latitude'].mean(),df_['longitude'].mean()), zoom_start=14)

    points = df_[['latitude','longitude']].to_records(index=False)
    colors = list(df_['cog'].values)
    colormap = ['red','green']
    line = folium.ColorLine(points, colors, colormap=colormap, opacity = 0.30, popup='out', weight=1.0)
    line.add_to(my_map)


    return my_map