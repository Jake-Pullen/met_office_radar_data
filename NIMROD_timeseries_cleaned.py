from __future__ import division, print_function
import numpy as np
import glob

# Configuration
asc_path = "asc_files/"
asc_wildcard_file = "*.asc"
asc_mult_source = asc_path + asc_wildcard_file

five_min_rainfall_spatial_timeseries_name = 'timeseries_data.txt'

things = [
    # loc name, loc id, x loc,   y loc,  resolution
    [1725  , 2175  , 608500   , 216500   , 1000  , -1  ], # 'BRICSC', 'TM0816'
    [1725  , 2175  , 568500   , 342500   , 1000  , -1  ], # 'HEACSC', 'TF6842'
    [1725.0, 2175.0, -404500.0, -624500.0, 1000.0, -1.0]# example
]

def read_ascii_header(ascii_raster_file):
    """Reads header information from an ASCII DEM"""
    with open(ascii_raster_file) as f:
        header_data = [float(f.__next__().split()[1]) for x in range(6)]
    return header_data

def calculate_crop_coords(basin_header, radar_header):
    """Calculate crop coordinates based on header data"""
    y0_radar = radar_header[3]
    x0_radar = radar_header[2]
    
    y0_basin = basin_header[3]
    x0_basin = basin_header[2]
    
    nrows_radar = radar_header[1]
    
    nrows_basin = basin_header[1]
    ncols_basin = basin_header[0]

    cellres_radar = radar_header[4]
    cellres_basin = basin_header[4]
    
    xp = x0_basin - x0_radar
    yp = y0_basin - y0_radar
    
    xpp = ncols_basin * cellres_basin
    ypp = nrows_basin * cellres_basin
    
    start_col = np.floor( xp / cellres_radar )
    end_col = np.ceil( (xpp + xp) / cellres_radar )
    
    start_row = np.floor(nrows_radar - ( (yp + ypp)/cellres_radar ))
    end_row = np.ceil(nrows_radar - (yp/cellres_radar))
    
    print(start_col, start_row, end_col, end_row)
    return int(start_col), int(start_row), int(end_col), int(end_row)


def extract_cropped_rain_data():
    """Extract cropped rain data and create rainfall timeseries"""
    rainfile = []
    #basin_header = read_ascii_header(basinsource)
    basin_header = things[0] # just BRICSC for now

    for f in glob.iglob(asc_mult_source):
        print(f)
        radar_header = read_ascii_header(f)
#        print(radar_header)

        start_col, start_row, end_col, end_row = calculate_crop_coords(basin_header, radar_header)
        
        start_col = int(round(start_col))
        start_row = int(round(start_row) )
        end_col = int(round(end_col) )
        end_row = int(round(end_row) )
        
        cur_rawgrid = np.genfromtxt(f, skip_header=6, filling_values=0.0, loose=True, invalid_raise=False)
        
        cur_croppedrain = cur_rawgrid[start_row:end_row, start_col:end_col]
        print(cur_croppedrain)
        # Flatten the cropped rain data into a 1D array
        cur_rainrow = cur_croppedrain.flatten()
        print(cur_rainrow)
        rainfile.append(cur_rainrow)
    
    rainfile_arr = np.vstack(rainfile)
    np.savetxt(five_min_rainfall_spatial_timeseries_name, rainfile_arr, delimiter=' ', fmt='%1.1f')


if __name__ == '__main__':
    extract_cropped_rain_data()