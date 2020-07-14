library(data.table)
library(ROracle)
library(RODBC)
library(data.table)
library(lubridate)
pullInit <- function(){
  # date YYYY-MM-DD
  drv <- dbDriver('Oracle')
  host <- "mtrapcp.mtdbodap-scan"
  port <-1521
  service <- "mtrapcp"
  connect.string <- '
  (DESCRIPTION=
  (ADDRESS_LIST = (ADDRESS = (PROTOCOL = TCP)(HOST = edc1-exa-scan)(PORT = 1521)))
  (CONNECT_DATA = (SERVER = DEDICATED)
  (SERVICE_NAME =  mtrapcp.mc.local))
  )'

  con <- dbConnect(drv, username = getOption('oracle.user.uid'), password = getOption('oracle.user.pwd'),
                   dbname = connect.string)
  
  query <- "
  
  SELECT
  a.event_no,
  a.opd_date,
  b.line_id,
  b.block_id,
  b.pattern_direction,
  a.stop_id,
  c.long_name,
  a.vehicle_id,
  a.act_arr_time,
  a.act_dep_time,
  a.nom_dep_time,
  a.passenger_in_org,
  a.passenger_out_org
  FROM
  veh_stop_passenger a INNER JOIN
  veh_trip b ON a.event_no_trip = b.event_no INNER JOIN
  nom_stop c ON a.stop_id = c.stop_id 
  WHERE
  a.opd_date >= TO_DATE('11/01/2019','MM/DD/YYYY') AND
  a.opd_date <= TO_DATE('01/01/2020','MM/DD/YYYY') AND
  a.opd_date >= valid_from AND a.opd_date <= c.valid_until 
  "
  res <- dbSendQuery(con, query, prefect = TRUE, bulk_read = 2L,
                     data2 <- data.table())
  apc <- data.table(fetch(res, n = -1))
  names(apc) <- tolower(names(apc))
  setnames(apc, "event_no", "veh_stop_passenger_event_no")
  setkey(apc, opd_date, block_id, vehicle_id, act_arr_time)
  return(apc)
  
}

pullArinc <- function(){
  arincQuery <-   "
    SELECT 
  a.record_id,
  b.sched_calendar_id_date,
  a.depart_date,
  a.train_block,
  a.route,
  b.trip_direction,
  a.revised_station_id,
  b.site_id,
  b.trip_start_datetime,
  b.revised_consist_size,
  b.revised_consist,
  b.sched_depart_datetime,
  a.arrive_datetime,
  a.depart_datetime,
  a.depart_time,
  c.revised_car,
  c.car_sequence
  FROM 
  lrt_arinc_avl a INNER JOIN
  lrt_arinc_block_trip b ON a.record_id = b.record_id INNER JOIN
  lrt_arinc_car c ON a.record_id = c.record_id
  WHERE 
  b.sched_calendar_id_date >= '2019-11-01' AND  
  b.sched_calendar_id_date <= '2020-01-01' 
  ORDER BY sched_calendar_id_date, train_block, revised_car, sched_depart_datetime
  "
  
  channel <- odbcConnect('db_prod_rail_scada', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
  arinc <- data.table(sqlQuery(channel, arincQuery, stringsAsFactors = FALSE))
  arinc[, `:=`(arrive_datetime = ymd_hms(arrive_datetime, tz = "America/Chicago"),
               depart_datetime = ymd_hms(depart_datetime, tz = "America/Chicago"),
               sched_depart_datetime = ymd_hms(sched_depart_datetime, tz = "America/Chicago"),
               sched_calendar_id_date = ymd(sched_calendar_id_date, tz = "America/Chicago"))]
  arinc[revised_station_id == "TF1I" , depart_datetime := arrive_datetime]
  arinc[revised_station_id == "UNDP" & trip_direction == "EB", depart_datetime := arrive_datetime]
  arinc[revised_station_id == "MAAM" & trip_direction == "SB", depart_datetime := arrive_datetime]
  
  # set station as a factor
  arinc[, revised_station_id := factor(revised_station_id,
                                       levels =  c("TF1O", "TF1I", "WARE", "5SNI", "GOVT", "USBA", "WEBK",
                                                   "EABK", "STVI", "PSPK", "WGAT", "RAST", "FAUN", "SNUN",
                                                   "HMUN", "LXUN", "VIUN", "UNDA", "WEUN", "UNRI", "ROST", 
                                                   "10CE", "CNST", "UNDP", "CDRV", "FRHI", "LAHI", "38HI",
                                                   "46HI", "50HI", "VAMC", "FTSN", "LIND", "HHTE", "AM34",
                                                   "BLCT", "28AV", "MAAM"))]
  return(arinc)
  
}

apc <- pullInit()
arinc <- pullArinc()

apc[pattern_direction == "North", trip_direction := "NB"]
apc[pattern_direction == "South", trip_direction := "SB"]
apc[pattern_direction == "East", trip_direction := "EB"]
apc[pattern_direction == "West", trip_direction := "WB"]


# create a rollTime
apc[, sched_calendar_id_date := floor_date(opd_date, 'day')]
apc[, apc_depart_datetime := opd_date + dseconds(act_dep_time) ]
apc[, apc_arrive_datetime := opd_date + dseconds(act_arr_time)]
apc[, rollTime := as.numeric(apc_depart_datetime)]
apc[, site_id := as.integer(stop_id)]
apc[, route := as.integer(line_id)]
apc[, line_id := NULL]

# rename columns
setnames(apc, c("vehicle_id"), c("revised_car"))

# fix site_id issue at Nicollet
# set 56331 to 51243
apc[site_id == 56331, site_id := 51423]

# set 56337 to 51408
apc[site_id == 56337, site_id := 51408]

# for eastbound trips at undp or sb at the mall, replace depart with arrive time
apc[, depart_datetime_new := apc_depart_datetime]
apc[pattern_direction == "East" & long_name == "Union Depot Station", depart_datetime_new := apc_arrive_datetime]
apc[pattern_direction == "South" & long_name == "MOA Transit Station", depart_datetime_new := apc_arrive_datetime]



# match two different ways
apc[, rollTime := as.numeric(apc_depart_datetime)]
arinc[, rollTime := as.numeric(depart_datetime)]

# join apc to arinc data
apc[, rollTime := as.numeric(depart_datetime_new)]
arinc[, rollTime := as.numeric(depart_datetime)]

setkey(apc, sched_calendar_id_date, revised_car, route, site_id, trip_direction, rollTime)
setkey(arinc, sched_calendar_id_date, revised_car, route, site_id, trip_direction, rollTime)
apcArinc <- arinc[apc, roll = 'nearest']

# calculate the difference between arinc and init times
apcArinc[, diff := as.numeric(difftime(depart_datetime_new, depart_datetime, units = 'secs'))]

# create data to match wiht the dunn data
apcArinc <- apcArinc[abs(diff) < 600, .(event_no = veh_stop_passenger_event_no, site_id, matt_record_id = record_id, revised_station_id,
                         matt_trip_direction = trip_direction, matt_variance = diff,
                         matt_route = route, matt_init_depart = depart_datetime_new,
                               matt_arinc_depart = depart_datetime, matt_arinc_arrive = arrive_datetime,
                         matt_vehicle_id = revised_car)]

# create a site id, revised station id lookup (THIS WILL BE USEFUL LATER)
siteLookup <- unique(apcArinc, by = c("site_id", "revised_station_id"))
siteLookup <- siteLookup[!is.na(revised_station_id), .(site_id, revised_station_id)]

# pull dunn matches
channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
dunnMatch <- data.table(sqlQuery(channel, "select * from stg_init_avl_match WHERE INIT_DEPART_DATETIME >= '2019-11-01' AND 
                                 INIT_DEPART_DATETIME < '2020-01-01'", stringsAsFactors = FALSE))
names(dunnMatch) <- tolower(names(dunnMatch))
dunnMatch[, depart_date := ymd(depart_date, tz = "America/Chicago")]
names(dunnMatch) <- paste0("dunn_", tolower(names(dunnMatch)))
dunnMatch[, event_no := dunn_event_no]

# merge in site_id to the dunn matches
dunnMatch[, site_id := dunn_site_id]
setkey(dunnMatch, site_id)
setkey(siteLookup, site_id)
dunnMatch <- merge(dunnMatch, siteLookup, all.x = TRUE)

setkey(dunnMatch, event_no)
setkey(apcArinc, event_no)
matchTest <- merge(apcArinc[, .(event_no, matt_record_id, matt_arinc_arrive, matt_arinc_depart, matt_variance,
                                matt_trip_direction, matt_vehicle_id, matt_route,
                                matt_init_depart)], dunnMatch, all.y = TRUE)
matchTest[, depart_date := ymd(dunn_depart_date, tz = "America/Chicago")]
matchTest <- matchTest[dunn_depart_date > '2019-11-03' & dunn_depart_date < '2019-12-20']
matchTest[dunn_record_id == matt_record_id | matt_variance == dunn_time_variance, .N] / matchTest[!is.na(matt_record_id), .N]


# badMatch <- matchTest[dunn_record_id != matt_record_id, .(N = .N), keyby = .(revised_station_id, dunn_site_id, trip_direction)]
mattBetter <- matchTest[dunn_record_id != matt_record_id & abs(matt_variance) < abs(dunn_time_variance),
                        .(event_no, matt_record_id, dunn_record_id, revised_station_id, vehicle_id = matt_vehicle_id,
                          matt_site_id = site_id, matt_arinc_depart, matt_arinc_arrive,
                          matt_variance, dunn_time_variance, matt_trip_direction, matt_init_depart, dunn_init_depart_datetime)]
dunnBetter <- matchTest[dunn_record_id != matt_record_id & abs(dunn_time_variance) < abs(matt_variance),
                        .(event_no, matt_record_id, dunn_record_id, revised_station_id, vehicle_id = matt_vehicle_id,
                          dunn_depart_date, dunn_init_depart_datetime,
                          matt_init_depart)]
sameRecord <- matchTest[dunn_record_id != matt_record_id & dunn_time_variance == matt_variance,
                        .(event_no, matt_record_id, dunn_record_id, revised_station_id, vehicle_id = matt_vehicle_id,
                          dunn_depart_date, dunn_init_depart_datetime,
                          matt_init_depart)]
noMatchMat <- matchTest[is.na(matt_variance)]
noMatchDunn <- matchTest[is.na(dunn_time_variance)]

# # segment records where the times don't match
# badInitTime <- mattBetter[matt_init_depart != dunn_init_depart_datetime, .(event_no, matt_init_depart)]
# setkey(badInitTime, event_no)
# apc[, event_no := veh_stop_passenger_event_no]
# setkey(apc, event_no)
# badInitTime <- merge(badInitTime, apc[, .(event_no, act_dep_time)])
# 
# # pull stewarts staging data
# channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
# close(channel)
# stg_lrt_init_match <- data.table(sqlQuery(channel, "select * FROM stg_lrt_init_match WHERE opd_date > '2019-10-25'",
#                                           stringsAsFactors = FALSE))
# names(stg_lrt_init_match) <- tolower(names(stg_lrt_init_match))
# setkey(stg_lrt_init_match, event_no)
# setkey(badInitTime, event_no)
# test <- merge(stg_lrt_init_match, badInitTime)
# 
# # add vehicle id to matched column
# setkey(mattBetter, event_no)
# setkey(stg_lrt_init_match, event_no)
# mattBetter <- merge(mattBetter, stg_lrt_init_match[, .(event_no, vehicle_id)])

arincQuery <- "
SELECT
  record_id,
  depart_date,
  vehicle_id,
  site_id,
  trip_direction,
  arrive_datetime,
  depart_datetime,
  arinc_depart_datetime
FROM  STG_LRT_ARINC_MATCH
WHERE
  depart_date > '2019-10-01'
"
channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
stg_lrt_arinc_match <- data.table(sqlQuery(channel,
                                           arincQuery,
                                           stringsAsFactors = FALSE))
# dunn_stg_lrt_arinc_match <- copy(stg_lrt_arinc_match)
# names(dunn_stg_lrt_arinc_match) <- paste0("dunn_", names(stg_lrt_arinc_match))

matt_stg_lrt_arinc_match <- copy(stg_lrt_arinc_match)
names(matt_stg_lrt_arinc_match) <- paste0("stg_lrt_", names(stg_lrt_arinc_match))

mattBetter[, stg_lrt_vehicle_id := vehicle_id]
mattBetter[, stg_lrt_record_id := matt_record_id]
setkey(mattBetter, stg_lrt_record_id, stg_lrt_vehicle_id)
setkey(matt_stg_lrt_arinc_match, stg_lrt_record_id, stg_lrt_vehicle_id)
mattBetterCompare <- merge(mattBetter[, .(event_no, matt_record_id, revised_station_id, matt_site_id,
                             matt_trip_direction, matt_arinc_arrive, matt_arinc_depart, 
                             matt_vehicle_id = vehicle_id, dunn_record_id, stg_lrt_record_id, stg_lrt_vehicle_id)],
              matt_stg_lrt_arinc_match, all.x = TRUE)
mattBetterCompare <- mattBetterCompare[, .(event_no, matt_record_id, stg_lrt_record_id, dunn_record_id, revised_station_id,
                             matt_vehicle_id, stg_lrt_vehicle_id,
                             matt_site_id, stg_lrt_site_id,
                             matt_trip_direction, stg_lrt_trip_direction,
                             matt_arinc_arrive, stg_lrt_arrive_datetime,
                             matt_arinc_depart, stg_lrt_arinc_depart_datetime)]
write.csv(mattBetterCompare, file = "nov_dec_match_disagreement.csv", row.names = FALSE)

p <- ggplot(data = apcArincAll[abs(diff) < 3600 & revised_station_id == "UNDP"]) +
  geom_histogram(aes(x = diff), binwidth = 30) +
  facet_grid(type ~ revised_station_id) +
  coord_cartesian(xlim = c(-600, 600)) +
  scale_x_continuous("APC - ARINC (seconds)") +
  theme_bw()
print(p)
ggsave(p, file = "undp_eb_matching.png", width = 16, height = 9)
setkey(arincApc, sched_calendar_id_date, route, train_block, revised_car, sched_depart_datetime)
