# sumologic-search-api
Python script to run sumologic search job api queries 

## Config Files
```
sumologic_environment:
  api_base_url: https://api.us2.sumologic.com/api
  api_access_id: TBD
  api_access_key: TBD

sumologic_search:
  type: records                        # records (aggregated results) or messages 
  query: |                  
    _sourceCategory=*
    | count as Total by _collector, _source, _sourceCategory
    | order by Total
    | Limit 20
  from: -24h                           # exact date like "2024-05-03T16:31:50", or relative ones using -15m, -2h, -5d, -2w
  to: now                              # exact date like "2024-05-03T16:31:50", or relative ones using -15m, -2h, -5d, -2w, or the string "now"
  timeZone: UTC                        # UTC, America/New_York, America/Los_Angeles, etc
  byReceiptTime: false                 # true or false
  autoParsingMode: Manual              # Manual or AutoParse

processing:
  debug:  false                        # true/false
  timeout: 120                         # in seconds before killing query
  batch: 1000                          # 1 to 10,000  batch records to download
  output_type: csv                     # screen, csv
  #screen_max_cell_width: 60           # xx
  output_destination: output.csv       # test.csv
```

## Execution
```
python3 ./sumologic-search-api.py -c config/config_slps_1.yaml
```
<img width="993" alt="image" src="https://github.com/Julien-Bernard/sumologic-search-api/assets/1272790/3fabc61a-a784-42b8-bf39-7cf7101318a5">
