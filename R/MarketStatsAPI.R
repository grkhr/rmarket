#' Get day-by-day general stats
#'
#' Function to get day-by-day general stats
#' @param date1 Start date
#' @param date2 End date
#' @param client_id Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @param shop_id Your shop ID, see \code{\link{https://partner.market.yandex.ru/?list=yes}}
#' @param token Your API token, see \code{\link{https://tech.yandex.ru/market/partner/doc/dg/concepts/authorization-docpage/}}
#' @export
#' @importFrom httr POST
#' @importFrom httr GET
#' @importFrom httr stop_for_status
#' @importFrom httr content
#' @importFrom httr add_headers
#' @importFrom rjson toJSON
#' @importFrom rjson fromJSON
#' @examples
#' MarketStatsAPI()

MarketStatsAPI <- function (date1 = "10daysAgo", date2 = "today", client_id = NULL, shop_id = NULL,
                              token = NULL) 
{
  proc_start <- Sys.time()
  packageStartupMessage("Processing", appendLF = F)
  options(stringsAsFactors = F)
  
  if (is.null(client_id) | is.null(token)) {
    stop(
      "client_id or token required!"
    )
  }
  
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  
  dates <- date_ranges(date1, date2, 170)
  dates[] <- lapply(dates, as.Date)
  
  result <- data.frame()
  
  for (i in 1:nrow(dates))
  {
    limit <- 1000
    offset <- 1
    last_query <- FALSE
    while (last_query == FALSE) {
      date1 <- format(dates[i,1], format = "%d-%m-%Y")
      date2 <- format(dates[i,2], format = "%d-%m-%Y")
      query <- paste0(
        "fromDate=",
        date1,
        "&fields=model&toDate=",
        date2,
        "&limit=",
        limit,
        "&offset=",
        offset,
        "&oauth_token=",
        token,
        "&oauth_client_id=",
        client_id
      )
      query <- gsub(":", "%3a", query)
      query <-
        paste0(
          "https://api.partner.market.yandex.ru/v2/campaigns/",
          shop_id,
          "/stats/main-daily.json?",
          query
        )
      answer <- GET(query)
      rawData <- content(answer, "parsed", "application/json")
      
      if (status_code(answer) != 200) {
        packageStartupMessage("Oops... Something went wrong.")
        packageStartupMessage("Status code: ", status_code(answer))
        packageStartupMessage("Error: ", rawData$error$message)
        stop()
      }
      
      if (length(rawData$mainStats)) {
        column_names <- names(rawData$mainStats[[1]])
        rows <- lapply(rawData$mainStats, function(x) return(x))
        res <- do.call(rbind.data.frame, rows)
        result <- rbind(result, res)
        # for (rows_i in 1:length(rows)) {
        #   result <- rbind(result, unlist(rows[[rows_i]]))
        # }
      }
      offset <- offset + limit
      if (length(rawData$mainStats) < limit) {
        last_query <- TRUE
      }
      packageStartupMessage(".", appendLF = F)
    }
  }
  
  #if (nrow(result) > 0) colnames(result) <- column_names
  packageStartupMessage(appendLF = T)
  packageStartupMessage("Processed ",length(result$date), " rows", appendLF = T)
  total_work_time <- round(difftime(Sys.time(), proc_start , units ="secs"), 0)
  packageStartupMessage(paste0("Total time: ", total_work_time, " sec."))
  return(result)
}


