import os.path
path = os.path.join(os.path.split(os.path.split(os.path.dirname(__file__))[0])[0],'data')


external = os.path.join(path,'processed')
interim = os.path.join(path,'interim')
processed = os.path.join(path,'processed')
raw = os.path.join(path,'raw')


path_ships = r'\\sspa.local\gbg\Projekt\2020\41209668-CONNECT-(M3)\03_Project\080_Research\41209668-02-D2E2F\data\forsea\chalmers-forsea-data-2021-03-22'

path_aurora = os.path.join(path_ships, '2020-01-01-till-2021-02-28-aurora.csv')
path_tycho = os.path.join(path_ships, '2020-01-01-till-2021-02-28-tycho-brahe.csv')
