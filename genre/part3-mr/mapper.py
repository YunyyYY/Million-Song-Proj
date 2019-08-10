import pandas as pd
import numpy as np
from hdf5_getters import *
import sys


def mapper():
    for line in sys.stdin:
        line = line.strip()
        if line=="":
            continue
        # print(line)

        h5 = open_h5_file_read(line)
        term = [i.decode('UTF-8') for i in get_artist_terms(h5)]
        weight = 0
        if 'jazz' in term:
            weight = get_artist_terms_weight(h5)[term.index('jazz')]
        l = [get_duration(h5), get_energy(h5), get_tempo(h5), get_year(h5),
                get_analysis_sample_rate(h5), get_danceability(h5), get_end_of_fade_in(h5), get_key(h5), 
                get_key_confidence(h5), get_loudness(h5), get_mode(h5),get_mode_confidence(h5), 
                get_start_of_fade_out(h5), get_tempo(h5), get_time_signature(h5),
                np.average(get_segments_start(h5)), np.average(get_segments_pitches(h5)),
                np.average(get_segments_timbre(h5)), np.average(get_segments_loudness_max(h5)),
                np.average(get_segments_loudness_max_time(h5)), np.average(get_segments_loudness_start(h5)),
                np.average(get_sections_start(h5)), np.average(get_beats_start(h5)), 
                np.average(get_bars_start(h5)), np.average(get_tatums_start(h5)), weight]
        h5.close()
        print(*l, sep = ", ")


if __name__ == '__main__':
    mapper()