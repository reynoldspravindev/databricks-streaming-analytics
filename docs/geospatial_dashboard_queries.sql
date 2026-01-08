-- ============================================================================
-- Geospatial Dashboard Queries - Quick Reference
-- ============================================================================
-- Use these queries in Databricks AI/BI Dashboards for geospatial visualizations
-- Tested with retail_analytics catalog on GCP Databricks
-- Date: January 7, 2026
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1. STORE LOCATION MAP (with Performance Color Coding)
-- ----------------------------------------------------------------------------
-- Purpose: Show all stores on a map with color-coded performance
-- Visualization: Scatter map with color by performance_category
-- Use: Operations dashboard homepage

SELECT
  store_id,
  latitude,
  longitude,
  brand,
  region,
  CASE 
    WHEN health_score >= 90 THEN 'Excellent'
    WHEN health_score >= 70 THEN 'Good'
    WHEN health_score >= 50 THEN 'Fair'
    ELSE 'Poor'
  END as performance_category,
  health_score,
  current_dt_wait_time_sec as drive_thru_wait_sec,
  current_dt_accuracy_pct as order_accuracy_pct
FROM retail_analytics.gold.gold_store_health
WHERE store_category = 'fast_food'
ORDER BY health_score ASC;

-- ----------------------------------------------------------------------------
-- 2. DRIVE-THROUGH PERFORMANCE HEAT MAP
-- ----------------------------------------------------------------------------
-- Purpose: Heat map of average OTD (Order-To-Delivery) time by location
-- Visualization: Heat map with gradient based on avg_otd_seconds
-- Use: Identify geographic performance clusters

SELECT
  store_id,
  latitude,
  longitude,
  brand,
  region,
  AVG(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
      THEN value END) as avg_otd_seconds,
  COUNT(*) as sample_count,
  MAX(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
      THEN value END) as max_otd_seconds
FROM retail_analytics.gold.gold_store_performance_5min
WHERE store_category = 'fast_food'
  AND window_start >= current_date()
  AND metric_name = 'drive_through_total_experience_time_sec'
GROUP BY store_id, latitude, longitude, brand, region
HAVING sample_count >= 10
ORDER BY avg_otd_seconds DESC;

-- ----------------------------------------------------------------------------
-- 3. EVENT DENSITY MAP
-- ----------------------------------------------------------------------------
-- Purpose: Show density of events by location
-- Visualization: Bubble map with size based on event_count
-- Use: Identify areas with recurring issues

SELECT
  s.latitude,
  s.longitude,
  s.store_id,
  s.brand,
  s.region,
  COUNT(e.event_type) as event_count,
  SUM(CASE WHEN e.is_critical THEN 1 ELSE 0 END) as critical_count,
  SUM(CASE WHEN e.event_category = 'drive_through' THEN 1 ELSE 0 END) as dt_issues,
  SUM(CASE WHEN e.event_category = 'kitchen_ops' THEN 1 ELSE 0 END) as kitchen_issues
FROM retail_analytics.gold.gold_store_events e
JOIN retail_analytics.gold.dim_stores s ON e.store_id = s.store_id
WHERE e.event_timestamp >= current_timestamp() - INTERVAL 7 DAYS
  AND s.store_category = 'fast_food'
GROUP BY s.latitude, s.longitude, s.store_id, s.brand, s.region
HAVING event_count > 0
ORDER BY critical_count DESC, event_count DESC;

-- ----------------------------------------------------------------------------
-- 4. NEARBY STORES COMPARISON
-- ----------------------------------------------------------------------------
-- Purpose: Find stores near a specific location and compare performance
-- Visualization: Table with distance and performance metrics
-- Use: Root cause analysis - why is this store underperforming vs nearby?

-- Parameters: Replace with actual store coordinates
DECLARE OR REPLACE VARIABLE target_lat DOUBLE DEFAULT 34.052235;
DECLARE OR REPLACE VARIABLE target_lon DOUBLE DEFAULT -118.243683;
DECLARE OR REPLACE VARIABLE radius_km DOUBLE DEFAULT 10;

WITH target_location AS (
  SELECT target_lat as lat, target_lon as lon
),
nearby_stores AS (
  SELECT
    s.store_id,
    s.latitude,
    s.longitude,
    s.brand,
    -- Haversine distance formula
    6371 * 2 * ASIN(SQRT(
      POW(SIN(RADIANS(s.latitude - t.lat) / 2), 2) +
      COS(RADIANS(t.lat)) * COS(RADIANS(s.latitude)) *
      POW(SIN(RADIANS(s.longitude - t.lon) / 2), 2)
    )) as distance_km
  FROM retail_analytics.gold.dim_stores s
  CROSS JOIN target_location t
  WHERE s.store_category = 'fast_food'
)
SELECT
  n.store_id,
  n.latitude,
  n.longitude,
  n.brand,
  ROUND(n.distance_km, 2) as distance_km,
  h.health_score,
  h.current_dt_wait_time_sec,
  h.current_dt_accuracy_pct,
  h.event_count_1h,
  h.critical_event_count_1h
FROM nearby_stores n
JOIN retail_analytics.gold.gold_store_health h ON n.store_id = h.store_id
WHERE n.distance_km <= radius_km
ORDER BY n.distance_km;

-- ----------------------------------------------------------------------------
-- 5. TOP/BOTTOM PERFORMERS BY REGION WITH COORDINATES
-- ----------------------------------------------------------------------------
-- Purpose: Show best and worst stores per region on a map
-- Visualization: Dual scatter map (top in green, bottom in red)
-- Use: Regional performance tracking

WITH regional_performance AS (
  SELECT
    store_id,
    latitude,
    longitude,
    region,
    brand,
    AVG(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
        THEN value END) as avg_otd,
    ROW_NUMBER() OVER (PARTITION BY region 
                       ORDER BY AVG(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
                                        THEN value END) ASC) as rank_best,
    ROW_NUMBER() OVER (PARTITION BY region 
                       ORDER BY AVG(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
                                        THEN value END) DESC) as rank_worst
  FROM retail_analytics.gold.gold_store_performance_5min
  WHERE store_category = 'fast_food'
    AND window_start >= current_date() - INTERVAL 7 DAYS
    AND metric_name = 'drive_through_total_experience_time_sec'
  GROUP BY store_id, latitude, longitude, region, brand
)
SELECT
  store_id,
  latitude,
  longitude,
  region,
  brand,
  ROUND(avg_otd, 2) as avg_otd_seconds,
  CASE 
    WHEN rank_best <= 3 THEN 'Top Performer'
    WHEN rank_worst <= 3 THEN 'Bottom Performer'
  END as performance_tier
FROM regional_performance
WHERE rank_best <= 3 OR rank_worst <= 3
ORDER BY region, rank_best;

-- ----------------------------------------------------------------------------
-- 6. MARKET SATURATION ANALYSIS
-- ----------------------------------------------------------------------------
-- Purpose: Identify oversaturated and underserved markets
-- Visualization: Bubble map with size = nearby_store_count
-- Use: Strategic planning for new store locations

WITH store_proximity AS (
  SELECT
    a.store_id,
    a.latitude,
    a.longitude,
    a.brand,
    a.region,
    COUNT(DISTINCT b.store_id) as nearby_same_brand_count,
    COUNT(DISTINCT CASE WHEN c.brand != a.brand THEN c.store_id END) as nearby_competitor_count
  FROM retail_analytics.gold.dim_stores a
  LEFT JOIN retail_analytics.gold.dim_stores b
    ON a.brand = b.brand
    AND a.store_id != b.store_id
    AND a.store_category = 'fast_food'
    AND b.store_category = 'fast_food'
    AND 6371 * 2 * ASIN(SQRT(
          POW(SIN(RADIANS(b.latitude - a.latitude) / 2), 2) +
          COS(RADIANS(a.latitude)) * COS(RADIANS(b.latitude)) *
          POW(SIN(RADIANS(b.longitude - a.longitude) / 2), 2)
        )) <= 5  -- Within 5 km
  LEFT JOIN retail_analytics.gold.dim_stores c
    ON a.brand != c.brand
    AND a.store_category = 'fast_food'
    AND c.store_category = 'fast_food'
    AND 6371 * 2 * ASIN(SQRT(
          POW(SIN(RADIANS(c.latitude - a.latitude) / 2), 2) +
          COS(RADIANS(a.latitude)) * COS(RADIANS(c.latitude)) *
          POW(SIN(RADIANS(c.longitude - a.longitude) / 2), 2)
        )) <= 5  -- Within 5 km
  WHERE a.store_category = 'fast_food'
  GROUP BY a.store_id, a.latitude, a.longitude, a.brand, a.region
)
SELECT
  store_id,
  latitude,
  longitude,
  brand,
  region,
  nearby_same_brand_count,
  nearby_competitor_count,
  CASE 
    WHEN nearby_same_brand_count >= 5 THEN 'Oversaturated'
    WHEN nearby_same_brand_count >= 3 THEN 'Competitive'
    WHEN nearby_same_brand_count >= 1 THEN 'Normal'
    ELSE 'Isolated'
  END as market_density
FROM store_proximity
ORDER BY nearby_same_brand_count DESC;

-- ----------------------------------------------------------------------------
-- 7. HOURLY PERFORMANCE BY TIME PERIOD (with Location)
-- ----------------------------------------------------------------------------
-- Purpose: Show how performance varies by time period across locations
-- Visualization: Multiple maps (one per time period) or animated map
-- Use: Peak hour optimization by location

SELECT
  store_id,
  latitude,
  longitude,
  region,
  brand,
  CASE
    WHEN HOUR(window_start) BETWEEN 6 AND 9 THEN 'Breakfast (6-9 AM)'
    WHEN HOUR(window_start) BETWEEN 11 AND 13 THEN 'Lunch (11-1 PM)'
    WHEN HOUR(window_start) BETWEEN 17 AND 19 THEN 'Dinner (5-7 PM)'
    ELSE 'Off-Peak'
  END as time_period,
  AVG(CASE WHEN metric_name = 'drive_through_total_experience_time_sec' 
      THEN value END) as avg_otd_seconds,
  AVG(CASE WHEN metric_name = 'drive_through_cars_per_hour' 
      THEN value END) as avg_throughput
FROM retail_analytics.gold.gold_store_performance_5min
WHERE store_category = 'fast_food'
  AND window_start >= current_date()
  AND metric_name IN ('drive_through_total_experience_time_sec', 'drive_through_cars_per_hour')
GROUP BY store_id, latitude, longitude, region, brand, 
         CASE WHEN HOUR(window_start) BETWEEN 6 AND 9 THEN 'Breakfast (6-9 AM)'
              WHEN HOUR(window_start) BETWEEN 11 AND 13 THEN 'Lunch (11-1 PM)'
              WHEN HOUR(window_start) BETWEEN 17 AND 19 THEN 'Dinner (5-7 PM)'
              ELSE 'Off-Peak' END
ORDER BY time_period, avg_otd_seconds DESC;

-- ----------------------------------------------------------------------------
-- 8. EQUIPMENT FAILURE CLUSTERS
-- ----------------------------------------------------------------------------
-- Purpose: Identify geographic patterns in equipment failures
-- Visualization: Heat map of equipment-related events
-- Use: Identify systemic issues (e.g., bad hardware batch in specific region)

SELECT
  s.latitude,
  s.longitude,
  s.store_id,
  s.brand,
  s.region,
  e.event_type,
  COUNT(*) as failure_count,
  MIN(e.event_timestamp) as first_occurrence,
  MAX(e.event_timestamp) as last_occurrence,
  DATEDIFF(day, MIN(e.event_timestamp), MAX(e.event_timestamp)) as days_span
FROM retail_analytics.gold.gold_store_events e
JOIN retail_analytics.gold.dim_stores s ON e.store_id = s.store_id
WHERE e.event_type IN ('LOOP_SENSOR_MALFUNCTION', 'KDS_SCREEN_DOWN', 
                       'POS_NETWORK_TIMEOUT', 'HME_SYSTEM_REBOOT',
                       'EQUIPMENT_FAILURE')
  AND e.event_timestamp >= current_timestamp() - INTERVAL 30 DAYS
  AND s.store_category = 'fast_food'
GROUP BY s.latitude, s.longitude, s.store_id, s.brand, s.region, e.event_type
HAVING failure_count >= 3
ORDER BY failure_count DESC, region, event_type;

-- ----------------------------------------------------------------------------
-- 9. DISTANCE TO NEAREST COMPETITOR
-- ----------------------------------------------------------------------------
-- Purpose: Calculate competitive pressure by proximity
-- Visualization: Map with gradient based on nearest_competitor_distance
-- Use: Understand competitive dynamics

WITH competitor_distances AS (
  SELECT
    a.store_id,
    a.latitude,
    a.longitude,
    a.brand,
    a.region,
    MIN(
      6371 * 2 * ASIN(SQRT(
        POW(SIN(RADIANS(b.latitude - a.latitude) / 2), 2) +
        COS(RADIANS(a.latitude)) * COS(RADIANS(b.latitude)) *
        POW(SIN(RADIANS(b.longitude - a.longitude) / 2), 2)
      ))
    ) as nearest_competitor_km
  FROM retail_analytics.gold.dim_stores a
  CROSS JOIN retail_analytics.gold.dim_stores b
  WHERE a.store_id != b.store_id
    AND a.brand != b.brand
    AND a.store_category = 'fast_food'
    AND b.store_category = 'fast_food'
  GROUP BY a.store_id, a.latitude, a.longitude, a.brand, a.region
)
SELECT
  cd.store_id,
  cd.latitude,
  cd.longitude,
  cd.brand,
  cd.region,
  ROUND(cd.nearest_competitor_km, 2) as nearest_competitor_km,
  CASE 
    WHEN cd.nearest_competitor_km < 1 THEN 'High Pressure (<1km)'
    WHEN cd.nearest_competitor_km < 3 THEN 'Medium Pressure (1-3km)'
    WHEN cd.nearest_competitor_km < 10 THEN 'Low Pressure (3-10km)'
    ELSE 'Isolated (>10km)'
  END as competitive_pressure,
  h.health_score,
  h.current_dt_wait_time_sec
FROM competitor_distances cd
JOIN retail_analytics.gold.gold_store_health h ON cd.store_id = h.store_id
ORDER BY cd.nearest_competitor_km;

-- ----------------------------------------------------------------------------
-- 10. REGIONAL PERFORMANCE SUMMARY (Aggregated)
-- ----------------------------------------------------------------------------
-- Purpose: Show aggregated metrics by region with geographic center
-- Visualization: Bubble map with large bubbles per region
-- Use: Executive summary dashboard

SELECT
  region,
  AVG(latitude) as center_latitude,
  AVG(longitude) as center_longitude,
  COUNT(DISTINCT store_id) as store_count,
  AVG(health_score) as avg_health_score,
  AVG(current_dt_wait_time_sec) as avg_wait_time_sec,
  AVG(current_dt_accuracy_pct) as avg_accuracy_pct,
  SUM(event_count_1h) as total_events_1h,
  SUM(critical_event_count_1h) as total_critical_events_1h
FROM retail_analytics.gold.gold_store_health
WHERE store_category = 'fast_food'
GROUP BY region
ORDER BY avg_health_score DESC;

-- ============================================================================
-- USAGE NOTES
-- ============================================================================
-- 1. Replace 'retail_analytics' with your actual catalog name if different
-- 2. Adjust time ranges (INTERVAL X DAYS) based on your data volume
-- 3. For large datasets, add appropriate WHERE filters to limit results
-- 4. Distance calculations use Haversine formula (spherical approximation)
-- 5. For production, consider materializing complex queries as views or tables
-- 6. Use Databricks AI/BI Dashboard filters for interactive exploration
-- ============================================================================

