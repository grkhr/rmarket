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
      "Аргументы client_id, metrics и token являются обязательными, заполните их и запустите запрос повторно!"
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
  
  
  numdays <- as.integer(difftime(date2, date1))
  if (numdays == 0)
    divnumber = 0
  else
    divnumber <- (numdays - 1) %/% 179
  if (numdays %% 179 == 0) {
    divremainder <- 0
  } else {
    divremainder <- (numdays) %% 179
  }
  for (i in 0:divnumber)
  {
    # metrics <- gsub(" ", "", metrics)
    if (i == divnumber) {
      divtill <- 0
    } else {
      divtill <- 1
    }
    if (i == 0) {
      divstart <- 0
    } else {
      divstart <- 1
    }
    date_from1 <- as.Date(date1) + i * 179 + divstart
    if (divremainder == 0)
      date_till1 <- as.Date(date2) - (divnumber - i) * 179
    else
      date_till1 <-
      as.Date(date2) - divremainder * divtill - (divnumber - i - 1) * 179 * divtill
    
    limit <- 1000
    offset <- 1
    last_query <- FALSE
    packageStartupMessage("Processing", appendLF = F)
    while (last_query == FALSE) {
      date1 <- as.Date(date1)
      date2 <- as.Date(date2)
      date1 <- format(date1, format = "%d-%m-%Y")
      date2 <- format(date2, format = "%d-%m-%Y")
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
      result <- data.frame()
      
      if (length(rawData$mainStats) > 0)
      {
        column_names <- unlist(lapply(c(names(
          rawData$mainStats[[1]]
        )),
        function(x)
          return(x)))
        
        rows <- lapply(rawData$mainStats, function(x)
          return(x))
        for (rows_i in 1:length(rows)) {
          result <- rbind(result, unlist(rows[[rows_i]]))
          
        }
      }
      
      packageStartupMessage(".", appendLF = F)
      offset <- offset + limit
      if (length(rawData$mainStats) * 4 < offset) {
        last_query <- TRUE
      }
      if (length(rawData$mainStats) > 0)
      {
        colnames(result) <- column_names
      }
      
      if (string_as_factor == "change") {
        options(stringsAsFactors = T)
      }
      packageStartupMessage(".", appendLF = F)
      
    }
  }
  packageStartupMessage(appendLF = T)
  packageStartupMessage("Processed ",length(result$date)," rows", appendLF = T)
  
  total_work_time <- round(difftime(Sys.time(), proc_start , units ="secs"),0)
  packageStartupMessage(paste0("Total time: ",total_work_time, " sec."))
  return(result)
}


