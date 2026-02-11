# Geocoding Case Study
5 MARIE DR, BRISTOL, 2809 - Matches to same street in Bristol County, MA
75 Bay View Ave, Bristol, 2809 - Matches to same street in Bristol County, MA

# Improved Geocoding Process 
- Prevent Result Checker from Looking at County for City Match
- Make Fuzzy Street Match A Little Stricter
- (Refine Idea/Purpose?) Validate City, State, Zip. If mis-matched
      - Candididate City by Zip, Fuzzy Match
- Points of Interest
- Intersections
- Create "intermediate result" to help process logic?
- Result score to compare results?
- Execute Tiger Snap or Extrapolate if a result is accepted but large
- If Tiger Snap or Extrapolte fails, select position
- Refine the metadata for Tiger extrapolate/snap

# Update Visulaization Scipts
The visulaizations have not been udpated since the folder system has been reorganized. So we need to go into each "create" data script and make sure they are pointed at the csv data file in the "latest" folder. Also, I removed the previous script that create route maps with straight lines and replaced it the script that uses osrm to plot actual routes on roads. This scrpt needs to be addaped to create the correct visualizations for the route_dashboard.html. Also, we need to make sure it is pointed at the correct csv file in the "latest" folder. Finally, the create all create_all_data_viz.py needs to be update to make sure it is calling the correct sciripts and organizing them the right way.

# Strategic Intro
You are an expert computer progrogrammer and data scientist. You are perfroming an an analysis of vehicle routing data from a small company that has approximatly 4 drivers, 4 days per week in Rhode Island and Massachussetts. The data you have is stop level data that includes user writen addresses, drivers, date, time, planned order, and actual order. The addresses are not geocoded, and are not standardized. Many of them are well formed with street number and name, city, state, and zip code. For a high percentage of the data, the zip code's leading "0" was removed, and there is very inconsistent state information. For a smaller subset of the data there are misspellings, improper town names, and some stops defined by the interseection of two streets. You have started using open source tools open street map, nominatim for geocode searching, TIGER for relationships between geography and postal regions, usaddress python library to tag addresses, and rapidfuzz for fuzzy string searching. In general, the zip codes are more accurate than the town so you have developed a process that leverages the zip codes. The geocoding search process is implemented in the NominatimSearch class located in the nominatim_search.py script inside the data_geocode folder. Nominatim is running in docker on port 8080. The geocoding process is now relativly acurate, you suspect 70% of the locations are accurate to the address number, 25% are accurate to the block of the correct street, 3% are accurate within 1 mile and on the correct street, 1% are ommited because a suitable match was not found, and 1% probably has larger than a 1 mile error. Efforts to improve the geocoding process are ongoing. After the stop addresses are geocoded into latitute and longitude, you want get driving distance and time between points using the open source routing machine (OSRM). OSRM runs in docker on the local machine at port 5000. OSRM goves upi the ability to get the actual on road distances and estimates driving times between stops. This allows you to better visualize the actual routes, as well as run different routing algorithms to try to find more efficent routes. In the future you plan to try linear programming, bias random key genetic algorithms, and reinforcement learning to try to find more efficient routes.


# Geocoding Pipeline Description
First you manually correct the zip codes that are only 4 digits long with custom regex patterns, next you tag the raw address with usaddress, and then query nominatim with street number, street name, and zip code. If you return a valid result you are done. Since there are many street names with mispellings, if you dont get a result on that first search, you then get a list of all of the street in the zip code from TIGER and try find a good fuzzy string match to the raw street name. If you find a close match to the raw string name, you then perfrom a second nominatim search with the street number, matched street name, and zip. If that does not return a result, if the first search fails. After the stop addresses are geocoded into latitute and longitude, you want get driving distance and time between points using the open source routing machine (OSRM). OSRM runs in docker on the local machine at port 5000. Not you have the ability to get the actual on road distances and estimates driving times between stops. This allows you to better visualize the actual routes, as well as run different routing algorithms to try to find more efficent routes. In the future you plan to try linear programming, bias random key genetic algorithms, and reinforcement learning to try to find more efficient routes.

# Geocoding Pipeline
## Tagging Process:
- Lookup Bad Address
- Lookup Cache
- Fix Standardize State Name to Abreviations
- Fix Towns w Directional Abbreviations
- Fix Zip Code Repair
- Fix, Add State Abreviation BEFORE TAGS, to fix USADDRESS duplicate tag issue
- Fix, Add State Abreviation AFTER TAGS, to help with serach
- Tag, usaddress python library
- Tag, Expand All Abreviations in Tags
- *** If no State, Reverse Search for State with number, street, city.
## Search Process:
- Search: Street Number, Street Name, Zip Code
- Check Results
- Search: Street Number, Street Name, City, State
- Check Results
- Find all streets in Zip, candidates
- Fuzzy match street name to candidates
- Search: Street Number, Fuzzy Street Name Match, Zip Code
- Check Result
- Find TIGER rows with exact ZIP and like road name
- Find location by extrapolating or snapping TIGER rows
- Check Result
        - Builds a Display Name wit ", Tiger extrapolate/snap" at the end.
## Result Check Logic
    - Match City or Zip
    - Longest Dimension < 1609 meters
    - OSM Place Rank > 26 
        - 24/25 is "Locality/Suburb"
        - 26/27 "Specific Street Names"
        - 28 House Number Interpolations (Make this my Extrapolations Search Too)
        - 30 Points of Interests, Shops, Cafes, Monuments


$$$$$$$$$$$$$$$$
# $$$ ARCHIVE $$$
$$$$$$$$$$$$$$$$$

# New TIGER Snap Search
Ok, I want to create another type of search after the matched street name search, it will be called "tiger_extrapolate_snap". This search is going to need its own function in the class, because we are going to query the TIGER table directly. This search is going to look in the tiger table for a an address range row that has the exact zip code and LIKE street name, and then the range number closest to address number. The street name we will use is the fuzzy matched one. 

Here is an example SQL search that will return all of the TIGER rows for a given zip code, and like a given street name. This should be incoporated in its own method in the class.

SELECT
  t.*,
  p.name::text AS road_name_text,
  p.class AS road_class,
  p.type  AS road_type
FROM location_property_tiger t
JOIN placex p ON p.place_id = t.parent_place_id
WHERE t.postcode = :'zip'
  AND p.name::text ILIKE '%' || :'street' || '%'
ORDER BY
  p.name::text,
  LEAST(t.startnumber, t.endnumber),
  GREATEST(t.startnumber, t.endnumber);


We are going to need to apply some logic to these reults. If the desired address is between two ranges, when we will extrapolate between the end lat long points. If the address number is just beyond the end of one of the ranges, then we will snap to the lat long of that end of the range. You will also need to handle the sutation when no rows are returned. When you use this search, make sure you are using the street name from the fuzzy match. Inside the method, you are will need to process all of the information so we get dictionaries similar metadata as what we get from the other nominatim searches. We might not have all the same data, but please fill in none in those cases to try to keep the data as similar as possible. Also, create a new metadata search object for whether the tiger extrapolate search was attempted, and record whether it was extrapolated or snapped, or unsucessful.

# Is Nominatim using TIGER
Nominatim is running in a docker container, and you are using it for searches. Many of the searches are returning full roads, but I would expect it would use TIGER to interpolate the house numbrs and try to get a more accurate position. What commands can I run to see if nominatim is using tiger.

# Update NominatimSearch Metadata
You are going to make many improvements to the way metadata is stored in NominatimSearch class, one of the primary objective is to make it clear why results are rejected in the check results step. Take your time, plan you edits to the code in a comprehensive manner, and these changes will be complicated. Do not add variable checks for try/except statements. First, we are going to break up the current process metadata attribute into two seperate attributes. tag_metadata, which is all the information related to tagging (parsing) the raw address into the usaddress tagging format. And then there will be a search_metatdata attribute, which includes everything about the searches. The seach metadata should include the information already stored, plus a key that is search_details, search details is a list of dictionaries, one for each search. It should include the specific information from that search, search_name (lets change the name of method name to search name), elapsed time, query, number of results, and results. Results is a list of dictionaries for each results, with each results dicationary including the display name, rejection reason, and rejecction logic. All of this needs to get built as the search method is executing and is passsed as an return variable from the search method. We need to improve the check results helper function so that is outputs a short rejected reason, like it currently does, but then also a more verbose rejection logic string that actually shows what the values were that did not bet met. Plan you work and then execute. Ask questions if you need more details.

# OSRM Implementation
After  you have geocoded the stop addresses into latitute and longitude, you want to get the driving distance and time between points using the open source routing machine (OSRM). You want OSRM to run your local machine in docker. You have the northeast OSM map files saved locally already. You plan to have several ways to use OSRM. First, as a test, you will want to just plot the route of a driver on the road, in the actual order the driver executed the road. Second, you will want to aggregate all the stops for a day, find the on road distance between all stops (or maybe a filtered set of stops, lets say the 5 closest stops by straight line distance) so that you can then analyze the routes and see if there was a more efficient way to schedule the stops of a particular day. Finally you will aggregate the stops by week, and see how much more efficiently you can plan routes if you consider all of the stops at once. Right now, lets just focus on getting OSRM up and running. Can you reccomend a series of steps to bring up and use a OSRM docker for this purpsoe.

# Stop Level Data
You are an expert computer progrogrammer and data scientist. You want to take the data csv file that is a stop level data for a vehicle routing comapny, and you want to create visulizations of the stop level data in the script viz_stop_data.py, which will create an html file, stop_dashboard.html of the visulaizations. Have a constant at the top that points to the csv file, the current on is, data_geocode_20260203_30KNotFound.csv. Make the output similar to the route_dashboart.html. Below the title, include the number of unique stops, for unique stops, link them through the address_raw and the address_nominatim feature; so any stops that match in either of these are considered the same stop. Include a histogram of the number  Include a histogram of planned stop duration and the actual stop 

# Location Data
You are an expert computer progrogrammer and data scientist. You want to take the data csv file,data_geocode_20260203_30KNotFound.csv, that is a stop level data for a vehicle routing company, and you create data about the locations. There are many locations that the compnay makes stops at on a regular basis, while the majority of locations they might only have stopped at once. You will create the python script, create_location_data.py, which will ouput location_data.csv. First, you are going to find all the unique locations, my linking all stops through the address_nominatim feature. If these features match, than a stop is considered at that unique location.  The output csv should contain a row for each unique location. Create a unique stop ID for each stop, and note there could be multiple raw addresses for each stop, if they were entered differently into the system. Use the address_nominatim and the libpostal to find the street number, street name, city, state, and zip code; create features for each of these. Then create features for the total_number_of_stops at that location, the total_planned_stop_duration, which is the sum of all planned stop durations at that location. Using that create the average_planned_stop_duration, which is the average of the planned duration. Create the total_actual_stop_duration, which is the sum of all actual stop durations at that location, and the average_actual_top_duration. Create a feature for the average_stop_delay, which is the average of actual minus planned stop duration for all stops at that location.

Now, you want to create visualization of the location data. Create a python script called viz_location_data.py, which will create an html file, location_dashboard.html. Use the existing visualization, route_dashboard.html, as an exmple of the desired format. At the top include the total number of unique locations, the number of locations with 2 different raw addresses, 3 raw addresses, and 4 or more. Create a histogram of the total_number_of_stops at each location. THen create a histogram of the total_number_of_stops, but exclude locations with fewer than 3 stops. Using the latitude and longitude, create a small folium map plot with all locations that have been stopped at more than 5 times. Using the latitude and longitude, create a heatmap over a folium map of all of the stops. Create a hitogram of the average_planned_stop_duration. Create a scatter plot of the average_planned_stop_duravtion versus the average_actual_stop_duration. Create a histogram of the average_stop_delay at each location.

# Route Data
You are an expert computer progrogrammer and data scientist. You want to take the data csv file that is a stop level data for a vehicle routing comapny and conver it into route level data. Individual routes can generally be found by filtering by a specific date and a specific driver. So you will create a python script called create_route_data.py, that looks at each route, and creates the same summary statistics for each. Here are the metrics I would like to you to create. Here are the features I would like you to calculate and save in a csv, route_data.csv. The number_of_stops in the route. The first_stop_planned_time, look at the "Planned Time" of the "Planned Stop Number". Then similarly, the first_stop_actual_time, is "Actual_Time" of the first "Actual Stop Number". Then create a last_stop_actual_time and last_stop_planned_time in a similar manner. Create a total_planned_stop_duation, whic is the sum of the planned stop duration and a total_actual_stop_duration, which is the sum of the "actual duration". Next, create helper function that can recieve a list of latitude and longitudes and calculate the distance. Then calculate "route_distance_straight_line_planned" based on the availile latidudes and longidues and  "Planned Stop Number".  Then calculate "route_distance_straight_line_planned" based on the availile latidudes and longidues and  "Planned Stop Number". Then create a helper function that accepts two lists of integers, which represent the planned stop number and actual stop number. Determine how many of the edges were executed as planned. A quick way to calculate this is for each node, determine if the predecessor is the same in both lists.

# Commit and Push
Please make a commit with all files that have been updated or added to the repo, make a short description based on the things you have dont recently, and push to remote at https://github.com/stevenblum/bbbs.git. Do not ask me any questions, just do it now.

# Coding Intro
You are advising on a vehicle‑routing data cleanup project focused on geocoding raw stop addresses. Input data: `clean_data/agg_data.csv` (raw address column). Geocoding stack: local Nominatim at `http://localhost:8080` with Postgres on port `5433`, libpostal installed, and `usaddress` available. Core logic lives in `clean_data/nominatim_search.py` (class `NominatimSearch`) and is invoked by `clean_data/data_add_geocode.py` in a multi‑threaded run that streams `data_geocode.csv` and `addresses_not_found.csv`. Search flow is fixed: 1) parsed number + street + ZIP query, 2) if not found, pull TIGER street names by ZIP from Postgres, fuzzy‑match (custom scorer in `clean_data/rapidfuzz_scorer.py`, abbreviation expansion in `clean_data/expand_abbreviations_in_road.py`), then re‑query with matched road. ZIP repair happens pre‑parse (`clean_data/zip_reapir.py`). There is also a bad‑address override map in `clean_data/nominatim_search_bad_address_cache.csv`. I want strategic guidance on improving geocode quality, performance, and validation/metrics.

# Nominatim Geocode Lookup
You are an experienced coder. You are building an app the optimizes vehicle routing. You have an existing data set that you need to clean. One of the steps is to geocode the stop data, transfroming raw addresses that a scheduler has recorded into latitude and longitude. The nominatim library is runnin on a docker server on port 8080, ready for use. The c library libpostal is installed with the python bindings. in the clean_data folder, the data_add_geocode.py is a script that attemps to geocode the locations. Starting with the agg_data.csv and eventually saving a data_geocode.csv, geocode_address_cache.csv, and geocode_report.csv. Right now, the code goes through several different search attempts, which I dont think is optimal. Instead, I would like to just to try on search. Please adjust is so that it immeditaly parses the raw address with libpostal. Then for the serach it it should be structures like f"{unit}, {road}, {city}, {state}". This will by default become the address_cache. Please update the script to reflect this.

search_trace:
- raw_address: string
- bad_address_lookup_used: boolean
- address_cache_used: boolean
- fix_zip_repair: boolean
- fix_state_abbreviation: boolean
- fix_town_directional: boolean
- address_tags: dictionary
- address_tags_expanded: dictionary
- missing_street_number: boolean
- missing_street_name: boolean
- missing_city: boolean
- missing_state: boolean
- missing_zip: boolean
- search_attempts: list of method names in order of attempted search
    { method_name: string,
      attempted: boolean,
      result_status: string, "returned", "none_found", "error",
      error: string or none,
      number_results: int or none,
      result_check: string, "accepted", "rejected", or none,
      result_check_reason: string or none,
      elapsed_ms: int
    }
- search_method_accepted: string, {method_name} or "none"
- street_match_in_zip_attempted: boolean
- street_match_in_zip_number_candidates: int
- street_match_in_zip_top_score: float
- street_match_in_zip_top_accepted: boolean
- street_match_in_zip_elapsed_ms: int
- search_successful: boolean
- search_method_accepted: string, {method_name} or "none"
- final_error: string or none
- elapsed_ms: int