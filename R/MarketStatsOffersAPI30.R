#' Get stats by offers
#'
#' Function to get stats by offers. Maximum period is 30 days!
#' @param date1 Start date
#' @param date2 End date
#' @param client_id Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @param shop_id Your shop ID, see \code{\link{https://partner.market.yandex.ru/?list=yes}}
#' @param token Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @param feedId You can check it in Pricelabs
#' @param DBD Adding a date column, default to FALSE, use it only for 1-day stats
#' @export
#' @importFrom httr POST
#' @importFrom httr GET
#' @importFrom httr stop_for_status
#' @importFrom httr content
#' @importFrom httr add_headers
#' @importFrom rjson toJSON
#' @importFrom rjson fromJSON
#' @examples
#' MarketStatsOffersAPI30()

MarketStatsOffersAPI30 <- function (date1 = "10daysAgo", date2 = "today", client_id = NULL, shop_id = NULL,
                              token = NULL, feedId = NULL, DBD = FALSE) 
{
  proc_start <- Sys.time()
  packageStartupMessage("Processing", appendLF = F)
  if (is.null(client_id) | is.null(token)) {
    stop("Аргументы client_id, metrics и token являются обязательными, заполните их и запустите запрос повторно!")
  }
  if (getOption("stringsAsFactors") == TRUE) {
    string_as_factor <- "change"
    options(stringsAsFactors = F)
  } else {
    string_as_factor <- "no change"
  }
  
  result <- data.frame(stringsAsFactors = F)
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  ch <- 0
 
  date_from = date1
  date_till = date2
  numdays <- as.integer(difftime(date2,date1))
   if (numdays == 0) divnumber = 0 else  divnumber <- (numdays-1) %/% 29
   if (numdays %% 29 == 0) {divremainder <- 0} else {divremainder <- (numdays) %% 29}
  
for (i in 0:divnumber)
{
   if (i == divnumber) {divtill <- 0} else {divtill <- 1}
   if (i == 0) {divstart <- 0} else {divstart <- 1}
   date_from1 <- as.Date(date1) + i*29 + divstart
   if (divremainder == 0) date_till1 <- as.Date(date2) - (divnumber-i)*29
   else date_till1 <- as.Date(date2) - divremainder*divtill - (divnumber-i-1)*29*divtill
  ch <- ch + 1
  limit <- 1000
  offset <- 1
  row_pos <- offset
  last_query <- FALSE
  while (last_query == FALSE) {
    date_from1 <- format(date_from1, format="%d-%m-%Y")
    date_till1 <- format(date_till1, format="%d-%m-%Y")
    query <- paste0("feedId" = feedId, "&page=", row_pos, "&pageSize=", limit, 
                    "&fromDate=", date_from1, "&toDate=", 
                    date_till1, 
                    "&oauth_token=", token, "&oauth_client_id=", client_id)
    query <- gsub(":", "%3a", query)
    query <- paste0("https://api.partner.market.yandex.ru/v2/campaigns/", shop_id, "/stats/offers.json?", 
                    query)
   # query <- paste0("https://api.partner.market.yandex.ru/v2/campaigns/21472065/feeds.json?", "&oauth_token=", token, "&oauth_client_id=", client_id)
    answer <- GET(query)
    rawData <- content(answer, "parsed", "application/json")
    
if (rawData$offersStats$totalOffersCount > 0)
{
    dataset <- rawData$offersStats$offerStats


    if (length(dataset) > 0)
    {
    

  #  rows <- lapply(rawData$offersStats$offerStats, function(x) return(x))
   for (rows_i in 1:length(dataset)) {
   result <- rbind(result,unlist(dataset[[rows_i]]),stringsAsFactors = F)
   }
    }

    
     if (rawData$offersStats$totalOffersCount <= row_pos*limit) {
      last_query <- TRUE
    }
    
    row_pos = row_pos + 1

  
  } else last_query <- TRUE
    packageStartupMessage(".", appendLF = F)
    
}
}
  
   column_names <- c(unlist(lapply(c(names(dataset[[1]])),
                                              function(x) return(x))))
   colnames(result) <- column_names
   
   if (DBD == TRUE) result$date <- date1
   packageStartupMessage(appendLF = T)
   packageStartupMessage("Processed ",length(result$clicks)," rows", appendLF = T)
   
   total_work_time <- round(difftime(Sys.time(), proc_start , units ="secs"),0)
   packageStartupMessage(paste0("Total time: ",total_work_time, " sec."))
  return(result)
  }


