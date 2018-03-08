CREATE TEMP FUNCTION aggCounts(maps ARRAY<STRING>)
RETURNS ARRAY<STRUCT<source STRING, count STRING>>
LANGUAGE js AS """
var map = {};
while (maps.length) {
  var pairs = JSON.parse(maps.pop())
  for (i in pairs) {
    if (pairs[i].count !== null) {
      map[pairs[i].source] = (map[pairs[i].source] || 0) + pairs[i].count;
    }
  }
}
var ret = [];
for (k in map) {
  ret.push({source:k, count:map[k].toFixed()});
}
return ret;
""";

WITH main_summary AS (
  SELECT "relud" as client_id, [STRUCT(STRING(NULL) AS engine, "a" AS source, NULL AS count)] AS search_counts
  UNION ALL
  SELECT "relud" as client_id, [STRUCT(STRING(NULL) AS engine, "a" AS source, 1 AS count)] AS search_counts
  UNION ALL
  SELECT "relud" as client_id, [STRUCT(STRING(NULL) AS engine, "a" AS source, 2 AS count)] AS search_counts
  UNION ALL
  SELECT "relud" as client_id, [STRUCT(STRING(NULL) AS engine, "b" AS source, 2 AS count), STRUCT(STRING(NULL) AS engine,"d" AS source, 4 AS count)] AS search_counts
)

SELECT client_id, aggCounts(ARRAY_AGG(TO_JSON_STRING(search_counts))) as search_counts
FROM main_summary
GROUP BY client_id;
