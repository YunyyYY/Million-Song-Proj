from hdf5_getters import *
import pandas as pd
import numpy as np

def h5_to_csv(flist):
    df = pd.DataFrame(columns=['duration', 'energy', 'tempo', 'year', 
                               'analysis_sample_rate', 'danceability', 'end_of_fade_in', 'key', 
                               'key_confidence', 'loudness', 'mode', 'mode_confidence', 'start_of_fade_out', 
                               'tempo', 'time_sig', 'seg_start', 'seg_pitch', 'seg_timbre', 'seg_loud', 
                           'seg_loud_time', 'seg_loud_start', 'section', 'beats', 'bars', 'tatum', 'weight'])
    index = 0
    for fi in flist:
        h5 = open_h5_file_read(fi)
        term = [i.decode('UTF-8') for i in get_artist_terms(h5)]
        weight = 0
        if 'jazz' in term:
            weight = get_artist_terms_weight(h5)[term.index('jazz')]
        df.loc[index] = [get_duration(h5), get_energy(h5), get_tempo(h5), get_year(h5),
                get_analysis_sample_rate(h5), get_danceability(h5), get_end_of_fade_in(h5), get_key(h5), 
                get_key_confidence(h5), get_loudness(h5), get_mode(h5),get_mode_confidence(h5), 
                get_start_of_fade_out(h5), get_tempo(h5), get_time_signature(h5),
                np.average(get_segments_start(h5)), np.average(get_segments_pitches(h5)),
                np.average(get_segments_timbre(h5)), np.average(get_segments_loudness_max(h5)),
                np.average(get_segments_loudness_max_time(h5)), np.average(get_segments_loudness_start(h5)),
                np.average(get_sections_start(h5)), np.average(get_beats_start(h5)), 
                np.average(get_bars_start(h5)), np.average(get_tatums_start(h5)), weight]
        index += 1
        h5.close()
    return df

if __name__ == '__main__':
    paths = get_all_files('A')
    a = h5_to_csv(paths)
    a.dropna(axis=0, how='any', inplace=True)
    a.to_csv("ge.csv", header=None)
