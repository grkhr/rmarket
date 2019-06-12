#' Get stats by offers
#'
#' Function to get stats by offers. Maximum period is 30 days!
#' @param date1 Start date
#' @param date2 End date
#' @param client_id Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @param shop_id Your shop ID, see \code{\link{https://partner.market.yandex.ru/?list=yes}}
#' @param token Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @param feedId See \code{\link{https://pricelabs.yandex.ru/stats/feeds}} 
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

MarketStatsOffersAPI30 <- function (date1 = "10daysAgo", 
                                    date2 = "today", 
                                    client_id = NULL, 
                                    shop_id = NULL,
                                    token = NULL, 
                                    feedId = NULL) {
  proc_start <- Sys.time()
  if (is.null(client_id) | is.null(token)) {
    stop("Check if you set client_id and token. It's necessary.")
  }
  options(stringsAsFactors = F)
  
  CheckStats <- suppressMessages(MarketStatsAPI(date1 = date1, date2 = date2, client_id = client_id, shop_id = shop_id, token = token))
  
  if (nrow(CheckStats) == 0) {
      packageStartupMessage("No data for period.", appendLF = T)
      return (CheckStats)
  }
  
  result <- data.frame()
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  ch <- 0
  if (abs(difftime(Sys.Date() - 1, date1)) >= 30)
    stop("These stats are available only for 30 last days. Check the API docs and try another period.")
  packageStartupMessage("Processing", appendLF = F)
  
  dates <- as.character(seq.Date(date1, date2, by = "day"))
  
  for (i in 1:length(dates)) {
    ch <- ch + 1
    limit <- 1000
    offset <- 1
    row_pos <- offset
    last_query <- FALSE
    while (last_query == FALSE) {
      date_from1 <- format(as.Date(dates[[i]]), format="%d-%m-%Y")
      date_till1 <- format(as.Date(dates[[i]]), format="%d-%m-%Y")
      query <- paste0("feedId" = feedId, "&page=", row_pos, "&pageSize=", limit, 
                      "&fromDate=", date_from1, "&toDate=", 
                      date_till1, 
                      "&oauth_token=", token, "&oauth_client_id=", client_id)
      query <- gsub(":", "%3a", query)
      query <- paste0("https://api.partner.market.yandex.ru/v2/campaigns/", shop_id, "/stats/offers.json?", 
                      query)
      answer <- GET(query)
      rawData <- content(answer, "parsed", "application/json")
    
      if (length(rawData$offersStats$offerStats) == 0 && (date2 == Sys.Date()-1 || date2 == Sys.Date())) {
        stop("Market stats doesn't ready to collect. Full stats exactly available after 12:00 for a previous day. Try again later. If the problem still happens, send a message to a technical support.")
      }
        
      if (rawData$offersStats$totalOffersCount > 0) {
        dataset <- rawData$offersStats$offerStats
        if (length(dataset) > 0) {
        #  rows <- lapply(rawData$offersStats$offerStats, function(x) return(x))
            for (rows_i in 1:length(dataset)) {
              result <- rbind(result,c(unlist(dataset[[rows_i]]),dates[[i]]))
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
   colnames(result) <- c(column_names, "date")
   packageStartupMessage(appendLF = T)
   packageStartupMessage("Processed ", length(result$clicks), " rows", appendLF = T)
   total_work_time <- round(difftime(Sys.time(), proc_start , units ="secs"),0)
   packageStartupMessage(paste0("Total time: ", total_work_time, " sec."))
   packageStartupMessage()
   s1 <- sum(as.numeric(CheckStats$clicks))
   s2 <- sum(as.numeric(result$clicks))
   s3 <- sum(as.numeric(CheckStats$spending))
   s4 <- sum(as.numeric(result$spending))
   if (s1 != s2 || s3 != s4)
     stop("Something went wrong, data isn't correct. Check the data in Yandex.Market and PriceLabs interfaces. If it isn't equal,
          write to Yandex.Market technical support and if they'll solve the problem, then try again.")
   return(result)
}


