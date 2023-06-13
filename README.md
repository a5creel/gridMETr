# gridMETr
A suite of utilities to download and process weather data from gridMET (Abatzoglou, 2013).

The basic operations involve choosing and downloading the raw data.  

## Quick Start

Use the `gridmetr_download()` to download the data to a folder called data/_variable_name.

```
gridmetr_download(variables = c("pdsi","erc"),
                  years = seq.int(2000,2002))
```

## Andie's READ_ME

Next steps: 

- Download gridmet data from 2003 - 2022 for my variables (just need to run)
- process (just need to run)
- write to csvs (need to write to loop over years) 





### Jude's ToDo

- Determine why future (parallel) fails within the download function

- Determine steps to convert to package

- Provide some visualization utilities






# References

Abatzoglou, J. T. (2013), Development of gridded surface meteorological data for ecological applications and modelling. Int. J. Climatol., 33: 121–131.
