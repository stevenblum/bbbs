# Geocoding Pipeline
## Tagging Process:
- Lookup Bad Addresses
- Lookup Cache
- Fix, Standardize State Name to Abreviations
- Fix, Towns w Directional Abbreviations
- Fix, Zip Code Repair
- Save Repaired Address
- Tag, usaddress python library
  -- If tag is unsucessfull, try to improve the repaired address.
  -- Improve, Add State Abreviation 
  -- Improve, Add Comma After Street Name
  -- Try to Tag Again
- Save Improved Address
- Fix, Add State Abreviation AFTER TAGS, to help with serach
- Tag, Expand All Abreviations in Tags
- *** If no State, Reverse Search for State with number, street, city.
## Search Process:
- Search: Address Repaired
- Check Results
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
        - Builds a Display Name with ", Tiger extrapolate/snap" at the end.
## Result Check Logic
    - Match Zip or (City and State)
    - Longest Dimension < 1609 meters
    - OSM Place Rank > 26 
        - 24/25 is "Locality/Suburb"
        - 26/27 "Specific Street Names"
        - 28 House Number Interpolations (Make this my Extrapolations Search Too)
        - 30 Points of Interests, Shops, Cafes, Monuments