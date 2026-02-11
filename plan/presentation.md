# Tools Used
- Open Street Map (OSM), https://www.openstreetmap.org/
- Nominatim
- US Census TIGER data, https://tinyurl.com/2jp6uubm
- US Address Python Library
- RapidFuzz Python Library
- Open Source Routing Machine (OSRM), http://project-osrm.org/
- Leaflet JS Library

# Interesting Characteristics of the Data
- Currrently have 4 years of data, 2021 to 2024
- Daily simultanous routes/drivers
- Planned and actual routes, times, and stop durations
- Stops at bins are demand driven
- Stops at individuals homes can be scheduled flexibly
- Trucks are Capacity Constrained
- Some stops inserted after the route is planned
- Dropoffs at Savers are interspersed with pickups
- Multiple schedulers, with different methodologies
- Variation between planned and actual stop durations time
- Some additional data in unstructured formats (notes, images)

# Research Questions
- How far from optimal are the ...
    - Individual routes?
    - Daily routes?
    - Weekly routes?
- Use inverse optimization to find "soft constraints" that explain the observed routes.
    - Differences between schedulers?
    - Predicatability from drivers?
- Is this enough data to predict actual travel time (traffic) between locations?

# Short Term Next Steps
- Continue cleaning
    - Make the geocoding process more robust
    - Filtering techniques to find bad geocode
- Define a format for a problem instance, and convert all data to that format
- Incorporate pickup/dropoff volumes
- ID Stops that were inserted after route was planned
- Get traffic data
- Can we incorporate data from the images and notes?
    - We would need to get the images from BBBS or optimo route

# Tools that I May Use in the Future
- Google Maps API (Pay)
- FFIEC Geocoding API, https://geomap.ffiec.gov/ffiecgeomap/
- MMQGIS Geocoding Plugin, but built on OSM and Nominatim