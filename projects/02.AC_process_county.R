# Goal: Do the second step of processing the gridmet data
# Prior to this script you must run project_init.R and 01.___.R
# Andie Creel modifying Jude Bayhams directory
# Started June 13th, 2023

# -----------------------------------------------------------------------------
# Get some intial stuff set up 
# -----------------------------------------------------------------------------
library(tidyr)
library(dplyr)
library(stringr)

#Define folders (variables) to extract 
folder.names <- c("pr","rmin","rmax","srad","tmmx","tmmn", "vs")

#Define set of years (ATUS years are 2003 onward)
# filter.years <- seq.int(2003,2022,1)
filter.years <-2023

#All gridmet files are on the same grid of lat and lons so grabbing one
file.names <- list.files("inputs/data",recursive = T,pattern = ".nc",full.names = T)

#Open the connection to the netCDF file
nc <- nc_open(file.names[1])

#Extract lat and lon vectors
nc_lat <- ncvar_get(nc = nc, varid = "lat")
nc_lon <- ncvar_get(nc = nc, varid = "lon")

#Use the lat and lon vectors to create a grid represented as two vectors (note:
#lon must go first to match with netcdf data)
nc.coords <- expand.grid(lon=nc_lon,lat=nc_lat) %>% 
  mutate(lon = round(lon,5),
         lat = round(lat,5),
         cells=row_number())


# -----------------------------------------------------------------------------
# Use GIS tools to aggregate data by chosen geography
# -----------------------------------------------------------------------------

#Choose a projection to be used by all geographic files
readin.proj=4326 #because it works with the lat and lons provided

#Converting nc coordinates from vector form to simple feature (sf)
g.nc.coords <- st_as_sf(nc.coords, coords = c("lon","lat")) %>% 
  st_set_crs(readin.proj)

#ensure that county polygons are also in 4326
us_co <- st_transform(us_co,4326)

#Attaching geographic data to netcdf grid -- ignore warning
bridge.county <- st_join(g.nc.coords,us_co,left=T) %>%
  dplyr::select(county=geoid,cells) %>%
  sfc_as_cols(.,names = c("lon","lat")) %>%
  st_set_geometry(NULL)


# -----------------------------------------------------------------------------
# Setting up nc files to process to parquet 
# -----------------------------------------------------------------------------

#if all files were downloaded but not processed, this would be the file list
file.list <- expand.grid(folder.names,filter.years,stringsAsFactors = F) %>% 
  rename(var=Var1,year=Var2) %>%
  mutate(var=str_to_lower(var),
         file.name = str_c(var,"_",year,".nc")) %>% 
  arrange(var) %>%
  mutate(file.name_temp = str_remove(file.name, "\\.nc$"))

# check .nc files have already been processed to .parquet
existing_files <- list.files("cache", full.names = TRUE, recursive = TRUE) %>%
  as.data.frame() 

existing_files$files <- existing_files$. 

existing_files <- existing_files %>%
  mutate(files = str_remove(files, "\\.parquet$")) %>%
  mutate(existing_files = str_remove(files, "cache/.*/")) %>%
  select(existing_files) %>%
  mutate(file_exist = 1)

file.list <- left_join(file.list, existing_files, by = c("file.name_temp" = "existing_files")) 

file.list_1 <- file.list %>%
  filter(is.na(file_exist)) %>% # keep NAs
  select(-file_exist)

# Check if .nc file is downloaded. 
existing_nc <- list.files("inputs/data", full.names = TRUE, recursive = TRUE) %>%
  as.data.frame() 

existing_nc$files <- existing_nc$. 

existing_nc <- existing_nc %>%
  mutate(files = str_remove(files, "\\.nc$")) %>%
  mutate(existing_nc = str_remove(files, "inputs/data/.*/")) %>%
  select(existing_nc) %>%
  mutate(file_exist = 1)

file.list_2 <- left_join(file.list_1, existing_nc, by = c("file.name_temp" = "existing_nc")) 

file.list_final <- file.list_2 %>%
  filter(!is.na(file_exist)) %>% # drop NAs
  select(-file_exist, -file.name_temp)

file.list <- paste0("inputs/data/",file.list_final$var,"/",file.list_final$file.name) 

fy = file.list[1]

rm(file.list_final, file.list_1, file.list_2)

# -----------------------------------------------------------------------------
# Begin loop over variables (folders)
# AC: need to have cache folder set up before this step
# creates parquet files in cache
# -----------------------------------------------------------------------------

plan(multisession(workers = 4)) #AC: I changed this to 4 for my computer
future_walk(
  file.list,
  function(fy){
    message(str_c("Beginning ",fy,"..."))
    
    vname=str_split(fy,"/")[[1]][3]
    
    #Construct the dataframe with all days and cells
    nc <- nc_open(fy)
    var.id=names(nc$var)
    date.vector <- as_date(nc$dim$day$vals,origin="1900-01-01")
    
    nc.data <- ncvar_get(nc = nc, varid = var.id)[,,]
    
    nc.data.df <- array(nc.data,dim=c(prod(dim(nc.data)[1:2]),dim(nc.data)[3])) %>%
      as_tibble(.name_repair = "universal") %>%
      rename_all(~str_c(date.vector))
    
    nc_lat <- ncvar_get(nc = nc, varid = "lat")
    nc_lon <- ncvar_get(nc = nc, varid = "lon")
    nc.coords <- expand.grid(lon=nc_lon,lat=nc_lat) %>% 
      mutate(lon = round(lon,5),
             lat = round(lat,5),
             cells=row_number())
    
    nc.df <- bind_cols(nc.coords,nc.data.df)
    
    #Join gridmet data with bridge 
    var.block <- inner_join(bridge.county %>% select(county,cells),
                            nc.df %>% select(-c(lat,lon)),
                            by=c("cells")) %>%
      drop_na(county) %>%
      select(-c(cells)) %>%
      as.data.table() %>% 
      melt(., id.vars = c("county"),
           variable.name = "date",
           value.name = "value",
           variable.factor=FALSE) 
    
    out <- var.block[,.(value=base::mean(value,na.rm=T)),by=.(county,date)][,`:=`(var=vname)]
    
    dir_name = str_c("cache/",vname)
    if(!dir.exists(dir_name)) dir.create(dir_name)
    write_parquet(out,paste0(dir_name,"/",vname,"_",str_sub(fy,-7,-4),".parquet"))
    
  },.progress = T)
