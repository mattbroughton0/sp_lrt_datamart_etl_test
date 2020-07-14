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
  a.opd_date >= TO_DATE('11/04/2019','MM/DD/YYYY') AND
  a.opd_date <= TO_DATE('11/06/2019','MM/DD/YYYY') AND
  a.opd_date >= valid_from AND a.opd_date <= c.valid_until AND
  b.pattern_direction IN ('East', 'South') AND
  c.long_name IN ('Union Depot Station', 'MOA Transit Station')  
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
  b.sched_calendar_id_date >= '2019-11-04' AND  
  b.sched_calendar_id_date <= '2019-11-06' AND
  a.revised_station_id in ('MAAM', 'UNDP') AND
  b.trip_direction in ('EB', 'SB')
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


# create a rollTime
apc[, sched_calendar_id_date := floor_date(opd_date, 'day')]
apc[, apc_depart_datetime := sched_calendar_id_date + dseconds(act_dep_time) ]
apc[, apc_arrive_datetime := sched_calendar_id_date + dseconds(act_arr_time)]
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

setkey(apc, sched_calendar_id_date, revised_car, route, site_id, rollTime)
setkey(arinc, sched_calendar_id_date, revised_car, route, site_id, rollTime)
apcArincOld <- arinc[apc, roll = 'nearest']
apcArincOld[, type := "Match Depart Datetimes"]

apc[, rollTime := as.numeric(depart_datetime_new)]
arinc[, rollTime := as.numeric(depart_datetime)]

setkey(apc, sched_calendar_id_date, revised_car, route, site_id, rollTime)
setkey(arinc, sched_calendar_id_date, revised_car, route, site_id, rollTime)
apcArincNew<- arinc[apc, roll = 'nearest']
apcArincNew[, type := "Match Arrive Datetime"]
setkey(apcArincNew, opd_date, revised_car)
check <- apcArincNew[revised_car == 204, .(opd_date, revised_car, route, site_id, long_name, apc_arrive_datetime, apc_depart_datetime,
                arrive_datetime, depart_datetime, pattern_direction, trip_direction)]
setkey(check, revised_car, opd_date, apc_depart_datetime)

apcArincAll <- rbind(apcArincOld, apcArincNew)
apcArincAll[type == "Match Depart Datetimes", diff := as.numeric(difftime(apc_depart_datetime, depart_datetime, units = 'secs'))]
apcArincAll[type == "Match Arrive Datetime", diff := as.numeric(difftime(depart_datetime_new, depart_datetime, units = 'secs'))]
p <- ggplot(data = apcArincAll[abs(diff) < 3600 & revised_station_id == "UNDP"]) +
  geom_histogram(aes(x = diff), binwidth = 30) +
  facet_grid(type ~ revised_station_id) +
  coord_cartesian(xlim = c(-600, 600)) +
  scale_x_continuous("APC - ARINC (seconds)") +
  theme_bw()
print(p)
ggsave(p, file = "undp_eb_matching.png", width = 16, height = 9)
setkey(arincApc, sched_calendar_id_date, route, train_block, revised_car, sched_depart_datetime)
