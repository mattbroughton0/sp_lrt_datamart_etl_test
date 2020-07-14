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
  a.opd_date >= TO_DATE('10/25/2019','MM/DD/YYYY') AND
  a.opd_date <= TO_DATE('12/01/2019','MM/DD/YYYY') AND
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
apc <- pullInit()


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

# pull the dunn records over the same time period
apcTime <- apc[, .(event_no = veh_stop_passenger_event_no, act_dep_time, act_arr_time, apc_arrive_datetime, apc_depart_datetime)]

# pull the records from stage
channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
stg_lrt_init_match <- data.table(sqlQuery(channel, "select * FROM stg_lrt_init_match WHERE opd_date > '2019-10-25'",
                                          stringsAsFactors = FALSE))
close(channel)
names(stg_lrt_init_match) <- tolower(names(stg_lrt_init_match))
stg_lrt_init_match[, `:=`(opd_date = ymd(opd_date, tz = "America/Chicago"),
                          init_depart_datetime = ymd_hms(init_depart_datetime, tz = "America/Chicago"))]

stg_lrt_init_match[, `:=`(matt_init_depart_datetime = floor_date(opd_date, 'day')  + dseconds(act_dep_time_key))]
stg_lrt_init_match[site_id == 56018, matt_init_depart_datetime := floor_date(opd_date, 'day') + dseconds(act_arr_time_key)]
stg_lrt_init_match[site_id == 51405, matt_init_depart_datetime := floor_date(opd_date, 'day')  + dseconds(act_arr_time_key)]
stg_lrt_init_match[, variance := as.numeric(abs(difftime(init_depart_datetime, matt_init_depart_datetime, units = 'secs'))),
                   by = 1:nrow(stg_lrt_init_match)]
prob <- stg_lrt_init_match[matt_init_depart_datetime != init_depart_datetime & floor_date(opd_date, 'day') >= '2019-11-01' &
                           opd_date != '2019-11-03' & opd_date != "2019-11-02" &  variance == 86400,
                           .(matt_init_depart_datetime, init_depart_datetime)]


dateIssue <- prob[, .(N = .N), keyby = .(opd_date)]
write.csv(prob, file = "time_mismatch.csv", row.names = FALSE)

setkey(stg_lrt_init_match, event_no)
setkey(apcTime, event_no)
test <- merge(stg_lrt_init_match, apcTime)
noMatch <- test[apc_depart_datetime != init_depart_datetime]
