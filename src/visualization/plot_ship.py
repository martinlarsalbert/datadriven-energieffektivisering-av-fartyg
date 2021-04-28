import numpy as np
import matplotlib.pyplot as plt

def track_plot(time,x,y,psi,lpp,beam,ax,N=7,line_style = 'y',alpha = 1):

    indexes = np.linspace(0, len(time) - 1, N).astype(int)

    for i, index in enumerate(indexes):
        if i == 0:
            color = 'g'
            alpha_= 0.8
        elif i == (len(indexes) - 1):
            color = 'r'
            alpha_= 0.8
        else:
            color = line_style
            alpha_=0.5

        plot_ship(x[index], y[index], psi[index], lpp = lpp, beam = beam, ax=ax, color=color, alpha=alpha*alpha_)

def plot(x,y,psi,ax,lpp,beam,color = 'y',alpha = 0.1):
    """Plot a simplified contour od this ship"""
    recalculated_boat = get_countour(x,y,psi,lpp = lpp, beam = beam)
    x = recalculated_boat[1]
    y = recalculated_boat[0]

    ax.plot(x,y,color,alpha=alpha)
    ax.fill(x, y, color, zorder=10,alpha=alpha)

def get_countour(x, y, psi,lpp,beam):
    # (Old Matlab boat.m)
    tt1 = lpp / 2
    tt2 = 0.9
    tt3 = beam / 4
    tt4 = 0.8
    tt5 = 3 * beam / 8
    tt6 = 0.6
    tt7 = beam / 2
    tt8 = 1.85 * beam / 4
    boat = np.matrix([[tt1, tt2 * tt1, tt4 * tt1, tt6 * tt1, -tt4 * tt1, -tt2 * tt1, -tt1, -tt1, -tt2 * tt1,
                       -tt4 * tt1, tt6 * tt1, tt4 * tt1, tt2 * tt1, tt1],
                      [0, -tt3, -tt5, -tt7, -tt7, -tt8, -tt5, tt5, tt8, tt7, tt7, tt5, tt3, 0]])
    delta = np.array([[x], [y]])

    rotation = np.matrix([[np.cos(psi), -np.sin(psi)],
                          [np.sin(psi), np.cos(psi)]])
    rows, columns = boat.shape
    rotated_boat = np.matrix(np.zeros((rows, columns)))
    for column in range(columns):
        rotated_boat[:, column] = rotation * boat[:, column]
    recalculated_boat = np.array(rotated_boat + delta)
    return recalculated_boat