# Goal: Do the second step of processing the gridmet data
# Prior to this script you must run project_init.R and 01.___.R
# Andie Creel modifying Jude Bayhams directory
# Started June 13th, 2023

# -----------------------------------------------------------------------------
# Get some intial stuff set up 
# -----------------------------------------------------------------------------

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
# Something with setting up file structure
# -----------------------------------------------------------------------------

file.list <- expand.grid(folder.names,filter.years,stringsAsFactors = F) %>% 
  rename(var=Var1,year=Var2) %>%
  mutate(var=str_to_lower(var),
         file.name = str_c(var,"_",year,".nc")) %>% 
  arrange(var) 

file.list <- paste0("inputs/data/",file.list$var,"/",file.list$file.name)


fy = file.list[1]

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