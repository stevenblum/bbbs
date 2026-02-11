# Interesting Characteristics of the Data
- 4 years of data, 2021 to 2024
- Several daily routes/drivers
- Planned and actual routes, times, and stop durations
- Stops at bins are demand driven
- Stops at individuals homes can be scheduled flexibly
- Trucks are Capacity Constrained (how frequently does that imact the route?)
- Some stops inserted after the route is planned
- Variation between planned and actual stop durations time
- Dropoffs at Savers are interspersed with pickups
- Multiple schedules, with different methodologies
- Some additional data in unstructured formats (notes, images)

# Further Data Processing
- Continue cleaning
    - Make the geocoding process more robust
    - Filtering techniques for bad geocode
- Define a format for a problem instance, and convert all data to that format
- Incorporate pickup/dropoff volumes
- ID Stops that were inserted after route was planned
- Get traffic data
- There are some notes and images in the data, are these valuable?
    - We would need to get the images from BBBS or optimo route


# Research Questions
- How far from optimal are the ...
    - Individual routes?
    - Daily routes?
    - Weekly routes?
- Use inverse optimization to find "soft constraints" that explain the observed routes.
    - Differences between schedulers?
    - Predicatability from drivers?
- Is this enough data to predict actual travel time (traffic) between locations?