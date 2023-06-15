# Goal: Do the third step of manipulating the gridmet data and storing as csvs
# Prior to this script you must run project_init.R and 01.___.R
# Andie Creel modifying Jude Bayhams directory
# Started June 13th, 2023

# TODO: parallelize going through years!!

rm(list = ls())
library(arrow)
library(dplyr)
library(purrr)
library(parallel)
library(vroom)

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------

myGetParquetFiles <- function(i){
  
  # read in parquet file from where it's stored (may need to adjust this path if moved)
  temp <- read_parquet(paste0("cache/", 
                              folder.names[i], 
                              "/",
                              folder.names[i],
                              "_",
                              year,
                              ".parquet"))
  
  # cleaning
  temp <- as.data.frame(temp) %>%
    rename(!!folder.names[i] := value) %>%
    select(-var)
  
  # store
  # results_list[[i]] <- temp
  temp
}

# -----------------------------------------------------------------------------
# define variables and years
# -----------------------------------------------------------------------------

#Define folders (variables) to extract 
folder.names <- c("pr","rmin","rmax","srad","tmmx","tmmn", "vs")


#Define set of years (ATUS years are 2003 onward)
myYears <- seq.int(2021,2022,1)

# -----------------------------------------------------------------------------
# loop through each year, write final csv
# -----------------------------------------------------------------------------

for (yr in myYears) {

  year <- yr
  
  # Do it for one year
  results_list <- vector("list", length = length(folder.names))
  
  # not running this in parallel bc it's already fast. 
  results_list <- lapply(1:length(folder.names), myGetParquetFiles)
  
  
  # Extract the common columns that you don't want to duplicate
  common_columns <- c("county", "date")
  
  # Combine the unique columns of the data frames
  final_df <- results_list %>%
    purrr::map(select, -one_of(common_columns)) %>%
    bind_cols()
  
  # Add the common columns to the combined data frame
  final_df <- bind_cols(select(results_list[[1]], one_of(common_columns)), final_df)
  
  # write
  vroom_write(final_df, 
              paste0("myOutput/", year, "_county_all.csv"), 
              delim = ",")

}


# test
test <- vroom("myOutput/2021_county_all.csv")
