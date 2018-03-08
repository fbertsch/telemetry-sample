CREATE TEMP FUNCTION aggMapFirst(maps ARRAY<STRING>)
RETURNS ARRAY<STRUCT<key STRING, value STRING>>
LANGUAGE js AS """
var map = {};
while (maps.length) {
  var pairs = JSON.parse(maps.pop())
  for (i in pairs) {
    if (pairs[i].value !== null) {
      map[pairs[i].key] = pairs[i].value;
    }
  }
}
var ret = [];
for (k in map) {
  ret.push({key:k, value:map[k]});
}
return ret;
""";

WITH main_summary AS (
  SELECT "relud" AS client_id, [STRUCT("a" AS key, STRING(NULL) AS value)] AS experiments, 1 AS time
  UNION ALL
  SELECT "relud" AS client_id, [STRUCT("a" AS key, "1" AS value)] AS experiments, 2 AS time
  UNION ALL
  SELECT "relud" AS client_id, [STRUCT("a" AS key, "2" AS value)] AS experiments, 3 AS time
  UNION ALL
  SELECT "relud" AS client_id, [STRUCT("b" AS key, "2" AS value),STRUCT("d" AS key, "4" AS value)] AS experiments, 4 AS time
)

SELECT client_id, aggMapFirst(ARRAY_AGG(TO_JSON_STRING(experiments) order by time)) AS experiments
FROM main_summary
GROUP BY client_id;
