# Goal: Do the first step of downloading the gridmet data
# Prior to this script you must run project_init.R
# Andie Creel modifying Jude Bayhams directory
# Started June 13th, 2023

# -----------------------------------------------------------------------------
# Set up variables and years (from variable_reference.csv)
# -----------------------------------------------------------------------------

# These are the variables Iwant (it's not all of them)
# pr: precipitation 
# rmin: Minimum Near-Surface Relative Humidity
# rmax: Maximum Near-Surface Relative Humidity
# srad: Surface Downwelling Solar Radiation
# tmmx: Maximum Near-Surface Air Temperature
# tmmn: Minimum Near-Surface Air Temperature
# vs: Wind speed at 10 m

#Define folders (variables) to extract 
folder.names <- c("pr","rmin","rmax","srad","tmmx","tmmn", "vs")

#Define set of years (ATUS years are 2003 onward)
# filter.years <- seq.int(2003,2022,1) 
filter.years <- seq.int(2021,2022,1)


# -----------------------------------------------------------------------------
# Download gridmet data (only needs to be done once)
# -----------------------------------------------------------------------------

# puts all .nc data in inputs/data/ folders for each variable of interest
gridmetr_download(variables = folder.names,
                  years = filter.years)

