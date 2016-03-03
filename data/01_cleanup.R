library(stringi)
library(stringr)
library(purrr)
library(dplyr)

fnameRegex <-  "vornamen-([0-9]+)-([a-zA-Z-]+)\\.csv"
sourceFiles <- list.files(path = "./raw") %>% str_match_all(., pattern = fnameRegex)

# Schema: Date, Region, Firstname, Count

data <- sourceFiles %>% map(function(row) {
  t <- as.POSIXct(stri_c(row[2], "-1-1 0:0:0 UTC"))
  content <- read.csv(stri_c("./raw/", row[1]), sep = ";") %>% transform(
                                                                 firstname = vorname,
                                                                 count = anzahl,
                                                                 sex = geschlecht) %>%
      mutate(district = row[3], timestamp = t) %>% select(timestamp, district, sex, firstname, count)

  content
}) %>% reduce(function(x,y) merge(x, y, all = T))


write.csv(data, "vornamen_merged.csv")

db <- src_sqlite("vornamen_merged.sqlite3", create = T)
copy_to(db, data, temporary = FALSE, indexes = list("timestamp", "district", "firstname", "sex"))



