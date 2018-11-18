#ridership data
library(readxl)
library(dplyr)
library(purrr)

ridership<-read_excel("C:/Z-Work/Transit project/Transit agency profile/June 2018 Adjusted Database.xlsx",sheet = 2)


agencies<-ridership[!duplicated(ridership$Agency),]
dim(agencies)

agencies$`HQ State`


regions<-read.csv("C:/Z-Work/Language Access/Census region and division.csv")
regions$stateName

regions$state.abb<-as.character(regions$abb)

intersect(regions$state.abb,agencies$`HQ State`)


agencies$region<-with(regions,Region[match(agencies$`HQ State`,state.abb)])

View(agencies[is.na(agencies$region),])


table(agencies$region)

agencies %>% filter(`Operating Expenses FY`>1000000 & `Reporter Type` == "Full Reporter" & (region=="Region 1: Northeast") )  %>% select(`HQ State`) %>% table()

agencies %>% filter( `Reporter Type` == "Full Reporter" & (region=="Region 1: Northeast") )  %>% select(`HQ State`) %>% table()

agencies %>% filter( `Reporter Type` == "Full Reporter" & (region=="Region 1: Northeast") )  %>% select(`HQ State`) %>% nrow()

agencies %>% filter( `Reporter Type` == "Full Reporter" & (region=="Region 2: Midwest") )  %>% select(`HQ State`) %>% table()


1regions %>% filter(Region=="Region 1: Northeast") %>% select(State) %>% distinct()

