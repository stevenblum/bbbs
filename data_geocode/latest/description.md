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
- *** Find TIGER rows with exact ZIP and like road name
- *** Find location by extrapolating or snapping TIGER rows
- *** ? Check Result (Not necessary, but good for metadata smilarity)
        - Builds a Display Name wit ", Tiger extrapolate/snap" at the end.
## Result Check Logic
    - Match City or Zip
    - Longest Dimension < 1609 meters
    - OSM Place Rank > 26 
        - 24/25 is "Locality/Suburb"
        - 26/27 "Specific Street Names"
        - 28 House Number Interpolations
        - 30 Points of Interests, Shops, Cafes, Monuments