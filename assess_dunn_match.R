library(data.table)
library(RODBC)
library(lubridate)


channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
tabs <- sqlTables(channel)
stgArincMatch <- sqlColumns(channel, 'stg_init_avl_match')


dunnMatch <- data.table(sqlQuery(channel, "select * from stg_init_avl_match WHERE INIT_DEPART_DATETIME >= '2019-11-01'"))
names(dunnMatch) <- paste0("dunn_", tolower(names(dunnMatch)))
dunnMatch[, event_no := dunn_event_no]



# read in my matched data
initApc <- fread("data/nov_5_match_all_cars.csv")

# initApc[, `:=`(arinc_depart_datetime = ymd_hms(arinc_apc_depart_d))]
initApc[, event_no := veh_stop_passenger_event_no]
setkey(initApc, event_no)
setkey(dunnMatch, event_no)
initApcDunn <- merge(initApc, dunnMatch, all.x = TRUE)

# 56334 56339
initApcDunn[record_id == dunn_record_id, .N]
initApcDunn[record_id != dunn_record_id & !is.na(dunn_record_id), .N]
initApcDunn[is.na(dunn_record_id), .N]
initApcDunn[is.na(dunn_record_id) & site_id %in% c(56334, 56339), .N]
prob <- initApcDunn[(event_no != dunn_event_no | is.na(dunn_event_no)) &
                      !(site_id %in% c(56334, 56339)), .(N = .N), keyby = .(site_id, arinc_revised_station_id, arinc_trip_direction)]

badMatch <- initApcDunn[(event_no != dunn_event_no | is.na(dunn_event_no)) &
                          !(site_id %in% c(56334, 56339)) & abs(arinc_apc_depart_diff) < 180]
windowSize <- initApcDunn[(event_no != dunn_event_no | is.na(dunn_event_no)) &
                             abs(arinc_apc_depart_diff) > 180 & abs(arinc_apc_depart_diff) < 1800]
setkey(badMatch, revised_car, apc_depart_datetime)


# classify different types of match failures
initApcDunn[is.na(dunn_event_no), failure_type := "Event_no missing from Dunn Table"]
initApcDunn[record_id != dunn_record_id, failure_type := "Mis-matched Dunn Record ID"]
initApcDunn[!is.na(record_id) & is.na(dunn_record_id), failure_type := "Missing Dunn Record ID"]
prob <- initApcDunn[!is.na(failure_type) & !is.na(arinc_apc_depart_diff) & abs(arinc_apc_depart_diff) < 1800]
initApcDunn[abs(arinc_apc_depart_diff) < 1800 & !(site_id %in% c(56334, 56339)), .N]


# pull the stage data
# pull the arinc staging information into a table
channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
stgArincMatch <- data.table(sqlQuery(channel, "select * from STG_LRT_ARINC_MATCH WHERE arinc_depart_datetime > '2019-11-01'",
                                     stringsAsFactors = FALSE))
names(stgArincMatch) <- tolower(names(stgArincMatch))
close(channel)

badMatchArinc <- stgArincMatch[record_id %in% badMatch$record_id]

# get the init stagin records
channel <- odbcConnect('LrtDatamartStage', uid = getOption('odbc.uid'), pwd = getOption('odbc.pwd'))
initStageCols <- sqlColumns(channel, 'STG_INIT_MATCH')
stgInitMatch <- data.table(sqlQuery(channel, "select * from stg_init_match WHERE opd_date >= '2019-11-01'",
                                    stringsAsFactors = FALSE))
close(channel)
names(stgInitMatch) <- tolower(names(stgInitMatch))



# grab example vehicle data from staging
veh253Init <- stgInitMatch[vehicle_id == 253 & opd_date == "2019-11-05"]
veh253Arinc <- stgArincMatch[vehicle_id == 253 & depart_date %in% c("2019-11-05", "2019-11-06")]  
setkey(veh253Arinc, arinc_depart_datetime)
setkey(veh253Init, init_depart_datetime)

veh204Init <- stgInitMatch[vehicle_id == 204 & opd_date == "2019-11-05"]
veh204Arinc <- stgArincMatch[vehicle_id == 204 & depart_date %in% c("2019-11-05", "2019-11-06")]  
setkey(veh204Arinc, arinc_depart_datetime)
setkey(veh204Init, init_depart_datetime)

match204 <- initApcDunn[revised_car == 204
                        , .(revised_car, arinc_revised_station_id, record_id, arinc_depart_datetime, apc_depart_datetime)]

# 
# 
# noMatch <- initApcDunn[is.na(dunn_event_no) & !is.na(record_id) & !(arinc_revised_station_id %in% c("MAAM", "UNDP")) &
#                          abs(arinc_apc_depart_diff) < 180,
#                        .(event_no, record_id, sched_calendar_id_date, opd_date, depart_date, site_id, revised_car, 
#                          apc_dep_time, apc_depart_datetime, arinc_depart_datetime, arinc_revised_station_id, arinc_trip_direction)]
# 
# # get the arinc stage records
# noMatchArinc <- stgArincMatch[record_id %in% noMatch$record_id]
# noMatchInit <- stgInitMatch[event_no %in% noMatch$event_no]
# names(noMatchInit) <- paste0("stg_init_match_", names(noMatchInit))
# setkey(noMatch, event_no)
# noMatchInit[, event_no := stg_init_match_event_no]
# setkey(noMatchInit, event_no)
# noMatchInit <- merge(noMatchInit, noMatch[, .(event_no, apc_dep_time, apc_depart_datetime)])
# write.csv(noMatchInit, file = "nov_5__match_init.csv")
# 
# setkey(noMatch, record_id, site_id)
# setkey(stgArincMatch, record_id, site_id)
# test <- merge(noMatch[abs(arinc_apc_depart_diff) < 180, .(record_id, apc_depart_datetime, depart_datetime = arinc_depart_datetime, 
#                           arinc_revised_station_id, site_id, arinc_apc_depart_diff) ], stgArincMatch, all.x = TRUE)
