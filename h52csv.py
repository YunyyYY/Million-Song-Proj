import pandas as pd
from hdf5_getters import *
from utils import get_all_files


def h5_to_csv(flist):
    df = pd.DataFrame(columns=['title', 'artist', 'album', 'hotness', 'duration', 'energy', 'tempo', 'year'])
    index = 0
    for fi in flist:
        h5 = open_h5_file_read(fi)
        df.loc[index] = [get_title(h5).decode('UTF-8'), get_artist_name(h5).decode('UTF-8'), 
                     get_release(h5).decode('UTF-8'), get_song_hotttnesss(h5), get_duration(h5), 
                     get_energy(h5), get_tempo(h5), get_year(h5)]
        index += 1
    return df


def main():
	# get absolute file address lists
	# Specify ROOT PATH for h5 files here
	paths = get_all_files('A')
	df = h5_to_csv(paths)
	df.to_csv('A_ABC.csv')

if __name__ == '__main__':
	main()
