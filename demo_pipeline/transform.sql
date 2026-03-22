CREATE OR REPLACE TABLE `project-a3416167-bd30-4a48-987.nyc_taxi_analytics.yellow_taxi_partitioned`
PARTITION BY DATE(tpep_pickup_datetime)
CLUSTER BY VendorID, PULocationID
AS
SELECT 
    t.*,
    z.Borough,
    z.Zone
FROM `project-a3416167-bd30-4a48-987.nyc_taxi_analytics.yellow_taxi_raw` t
LEFT JOIN `project-a3416167-bd30-4a48-987.nyc_taxi_analytics.taxi_zones` z
    ON t.PULocationID = z.LocationID
WHERE z.Borough NOT IN ('N/A', 'Unknown', 'EWR')
AND z.Borough IS NOT NULL