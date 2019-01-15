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
  
  if (is.null(client_id) | is.null(token)) {
    stop(
      "client_id or token required!"
    )
  }
  if (getOption("stringsAsFactors") == TRUE) {
    string_as_factor <- "change"
    options(stringsAsFactors = F)
  }
  else {
    string_as_factor <- "no change"
  }
  result <- data.frame(stringsAsFactors = F)
  
  
  
  date1 <- as.Date(date1)
  date2 <- as.Date(date2)
  
  dseq <- seq.Date(date1, date2, by = 170)
  dseq[length(dseq)+1] <- date2 + 1
  
  result <- data.frame()
  
  for (i in 1:length(dseq - 1))
  {
    limit <- 1000
    offset <- 1
    last_query <- FALSE
    while (last_query == FALSE) {
      date1 <- format(dseq[i], format = "%d-%m-%Y")
      date2 <- format(dseq[i+1] - 1, format = "%d-%m-%Y")
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
      
      if (length(rawData$mainStats) > 0) {
        column_names <- unlist(lapply(c(names(rawData$mainStats[[1]])), function(x) return(x)))
        rows <- lapply(rawData$mainStats, function(x) return(x))
        for (rows_i in 1:length(rows)) {
          result <- rbind(result, unlist(rows[[rows_i]]))
        }
      }
      packageStartupMessage(".", appendLF = F)
      offset <- offset + limit
      if (length(rawData$mainStats) < limit) {
        last_query <- TRUE
      }
      packageStartupMessage(".", appendLF = F)
    }
  }
  
  if (nrow(result) > 0) colnames(result) <- column_names
  packageStartupMessage(appendLF = T)
  packageStartupMessage("Processed ",length(result$date)," rows", appendLF = T)
  
  total_work_time <- round(difftime(Sys.time(), proc_start , units ="secs"),0)
  packageStartupMessage(paste0("Total time: ",total_work_time, " sec."))
  return(result)
}


