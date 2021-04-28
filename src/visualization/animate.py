import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from matplotlib.gridspec import GridSpec
from src.visualization import plot_ship

from ipywidgets import Layout, interact, IntSlider, interactive, Play
import ipywidgets as widgets

def plot_thruster(ax, x,y,cos,sin, power, scale=20):
    
    l = power*scale
    ax.arrow(x=y, y=x, dx=-l*sin, dy=-l*cos, head_width=l/5, head_length=l/5)
    #ax.arrow(x=y, y=x, dx=l*sin, dy=-l*cos, head_width=l/5, head_length=l/5)
    
def plot_thrust_allocation(row, trip, lpp=50, beam=20, scale=30):
    
    #fig,axes=plt.subplots(ncols=2)
    fig = plt.figure(constrained_layout=True)
    fig.set_size_inches(19,8)
    
    gs = GridSpec(2, 2, figure=fig)
    ax1 = fig.add_subplot(gs[:, 0])
    # identical to ax1 = plt.subplot(gs.new_subplotspec((0, 0), colspan=3))
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1:, 1])
    
    ax = ax1
    
    """
    Thruster 1 – NV
    Thruster 2 – SV
    Thruster 3 – NE
    Thruster 4 - SE
    """
    
    ##As given by Anna:
    #positions = np.array([
    #    [1,-1],  # NV
    #    [-1,-1], # SV
    #    [1,1],   # NE
    #    [-1,1],  # SE
    #    
    #])
    #
    #Martins guess:
    positions = np.array([
        [-1,-1], # SV
        [-1,1],  # SE
        [1,-1],  # NV
        [1,1],   # NE

        
    ])
    
    
    
    
    coordinates = np.array([positions[:,0]*lpp/2, positions[:,1]*beam/2] ).T
    
    for i,position in enumerate(coordinates):
        
        n=i+1
        sin_key = 'sin_pm%i' % n
        cos_key = 'cos_pm%i' % n
        power_key = 'power_em_thruster_%i' % n
            
        plot_thruster(ax=ax, x=position[0], y=position[1], cos=row[cos_key], sin=row[sin_key], power=row[power_key], scale=scale)
        ax.text(position[1], position[0], ' Thruster %i' % n)

        
    # velocity
    l=row['sog']
    if row['reversing']:
        direction = np.deg2rad(row['cog'] - row['heading'] - 180)
    else:
        direction = np.deg2rad(row['cog'] - row['heading'])
        
    dx = l*np.cos(direction)
    dy = l*np.sin(direction)
    ax.arrow(x=0, y=0, dx=dy, dy=dx, head_width=l/5, head_length=l/5, color='green')
    
    ax.set_xlim(np.min(coordinates[:,1])-scale,np.max(coordinates[:,1])+scale)
    ax.set_ylim(np.min(coordinates[:,0])-scale,np.max(coordinates[:,0])+scale)
    #ax.set_aspect('equal', 'box')
    
    if row['reversing']:
        # Rotate the view
        ax.invert_yaxis()
        ax.invert_xaxis()
        
    
    ## Second plot:
    ax = ax2
    trip.plot(x='longitude', y='latitude', ax=ax, style='k--')
    #ax.plot(row['longitude'], row['latitude'], 'o', ms=15)
    x = row['latitude']
    y = row['longitude']
    psi = np.deg2rad(row['cog'])
    
    
    # Define a ship size in lat/lon:
    N_scale = 20
    lpp=50
    beam=20
    scale =  1/lpp/N_scale*np.sqrt((trip['latitude'].max() - trip['latitude'].min())**2 + (trip['longitude'].max() - trip['longitude'].min())**2)
    lpp_ = lpp*scale
    beam_ = beam*scale

    plot_ship.plot(x, y, psi, lpp = lpp_, beam = beam_, ax=ax, color='b', alpha=0.5)
    
    ax.set_ylim(trip['latitude'].min(), trip['latitude'].max())
    ax.set_xlim(trip['longitude'].min(), trip['longitude'].max())
    
    ax.set_aspect('equal')
    
    ## Third plot:
    trip.plot(x='trip_time_s', y='sog', ax=ax3, style='k--')
    ax3.plot(row['trip_time_s'], row['sog'], 'o', ms=15)
    ax3.set_xlabel('Time [s]')
    ax3.set_ylabel('Ship speed [m/s]')
    
def create_animator(trip):
    trip = trip.copy()
        
    def animate(i=0):
        
        index = int(i)
        row = trip.iloc[index]
        plot_thrust_allocation(row=row, trip=trip)
        
    return animate

def widget(trip):

    trip=trip.copy()

    ## Preprocess:
    trip['trip_time_s'] = pd.TimedeltaIndex(trip['trip_time']).total_seconds()

    ## Normalizing:
    power_columns = ['power_em_thruster_%i' % i for i in range(1,5)]
    trip[power_columns]/=trip['power_em_thruster_total'].max()/4

    ## Resample:
    trip = trip.resample('2S').mean()

    animator = create_animator(trip=trip)

    play = Play(
    value=0,
    min=0,
    max=len(trip)-1,
    step=1,
    interval=100,
    description="Press play",
    disabled=False
    )

    slider = IntSlider(0,0,len(trip)-1,1, layout=Layout(width='70%'))
    widgets.jslink((play, 'value'), (slider, 'value'))
    animation = interactive(animator, i = slider); 
    return widgets.VBox([play, slider, animation])