Welcome to the BESdata wiki! The BESdata R package gives users at the City of Portland's Bureau of Environmental Services access to environmental monitoring data resources distributed across several data systems. The BESdata API consists of functions that map to objects available from these systems. Each function returns one or more objects, stored in a [tibble](https://tibble.tidyverse.org/), one object per row. Objects that represent the same resource are standardized so that they all have the same core set of attributes even though these systems rarely store the data with standard attributes.

# Data Sources
BESdata data sources are used by establishing a connection to a BES data system and using the API to extract resources. The BESdata package allows access to the Janus, Aquarius, Neptune, and Watershed data systems via these functions:
* `janus()`
* `aquarius()`    
* `neptune()`          
* `watershed()`        

# API
The BESdata API has the following core functions which accept a connection object and allow filtering by any attribute of the object:
* `monitoring_project(x, ...)` returns one or more [[Monitoring Projects|Project]].
* `monitoring_location(x, ...)` returns one or more [[Monitoring Locations|Monitoring-Location-Object]].
* `parameter(x, ...)` returns one or more [[Parameters|Parameter]].
* `timeseries(x, ...)` returns one or more [[Timeseries|Timeseries]].
* `samples(x, ...)` returns one or more [[Samples|Sample]].
* `analyses(x, ...)` returns one or more [[Analyses|Analysis]].
* `events(x, ...)` returns one or more [[Events|Event]]. 

Each function returns a tibble



# Resources by Data Source
## Janus
Janus is the system that stores analytical chemistry data and temporary flow monitoring data collected by the Field Operations team. The resources available from Janus include:
* `monitoring_projects` 
* `monitoring_locations` 
* `samples`
* `events`
* `analyses`
* `timeseries`

## Neptune
Neptune is the name of the data system that stores data from our collection system and rain gauge network collected by the DANS team. The functions available for interacting with Neptune include:
* `monitoring_locations` returns one or more [[Monitoring Location|Monitoring-Location-Object]] objects.
* `timeseries` returns one or more [[Time Series|Time-Series]] objects

## Watershed
* `events` returns one or more [[Event|Time-Series]] objects
* `occurrences` returns one or more [[Occurrence|Occurrence]] objects

## Aquarius
* `monitoring_locations` [[Monitoring Location|Monitoring-Location-Object]]
* `field_visit` (Event)
* `timeseries` (Time Series)

