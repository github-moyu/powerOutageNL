#get lat lon
if(TRUE){
  rm(list=ls())
  
  library(raster)
  library(rgdal)
  library(dplyr)
  library(parallel)  
  
  #input meta 
  #Rico <- readRDS("gadm36_PRI_0_sp.rds")
  #wd <- paste0("d:/Model Validation")
  #wd <- paste0("C:/Users/Y/Desktop/co-/stormPowerOutageValidation_due31Feb2023")
  #wd <- paste0("C:/Users/cenv0794/Desktop/ym/stormPowerOutageValidation_due31Feb2023")
  wd <- paste0("Z:/stormPowerOutageValidation")
  
  target <- sfarrow::st_read_parquet(paste0(wd,'/modelledOutput/targets.geoparquet')) 
  
  info <- read.csv(paste0(wd,'/data/list_20230708.csv'),stringsAsFactors = FALSE)
  colnames(info)[1]<-'ID'
  
  info$cnb <- paste0(info$ID,'_',info$SID)
  info$after <-0
  
  for(r in 1:nrow(info)){
    #r <-8  
    afterF<-list.files(paste0(wd,'/data/BM/15-day/'),pattern=paste0(info[r,'cnb'],'_','afterNL'),recursive = TRUE) 
    if(length(afterF)>0){info[r,'after']<-1}  
  }
  info2 <- info[which(info$after==1),]
  
  fList <-list.files(paste0(wd,'/f-values/'),pattern='.csv')
  
  list <- data.frame(files=fList)
  list$SID <- NA
  for(r in 1:nrow(list)){
    list[r,'SID'] <- substr(strsplit(list[r,'files'],'-')[[1]][2],1,13)
  }
  
  list$after <-0
  
  for(r in 1:nrow(list)){
    #r <-8  
    afterF<-list.files(paste0(wd,'/data/BM/15-day/'),pattern=paste0(list[r,'SID']),recursive = TRUE) 
    if(length(afterF)>0){list[r,'after']<-1}  
  }
  list2 <- list[which(list$after==1),]
  
  no_cores<-detectCores()-1
  no_cores<-5 
  
  cl <- makeCluster(no_cores)  
  clusterExport(cl,c("wd","target","info2","list2"))
  
  #for(ff in 1:length(fList)){
  parLapply(cl,1:nrow(list2),
            function(ff){
              library(raster)
              library(rgdal)
              library(dplyr)
              library(sfarrow)
              library(sf)
              
              #ff <- 20
              f_data <- read.csv(paste0(wd,'/f-values/',list2[ff,'files']),stringsAsFactors = FALSE)
              
              sid <- substr(strsplit(list2[ff,'files'],'-')[[1]][2],1,13)
              
              info3 <- info2[which(info2$SID==sid),]
              
              #target
              fFile<-list.files(paste0(wd,'/f-values/'),pattern=sid)
              f_data <-read.csv(paste0(wd,'/f-values/',fFile),stringsAsFactors = FALSE)
              #plot(target_data)
              targetV <- unique(f_data$target)
              
              urbanV <- rep(NA, length(targetV))
              beforeV <- rep(NA, length(targetV))
              beforeSDV <- rep(NA, length(targetV))
              after10V <- rep(NA, length(targetV))
              after15V <- rep(NA, length(targetV))
              
              for(row in 1:nrow(info3)){
                start_time <- Sys.time()
                #row<-1
                nm <- info3[row,'ID']
                id <- info3[row,'SID'] 
                
                #get input data
                if(TRUE){
                  #get urban cluster
                  urbanFile<-list.files(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/'),pattern='urban')
                  urban_data <- raster(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/',urbanFile))
                  #plot(urban_data)
                  
                  beforeFile<-list.files(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/'),pattern='beforeNL')
                  before_data <- raster(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/',beforeFile))
                  
                  beforeSDFile<-list.files(paste0(wd,'/data/BM/beforeSD/',nm,'_',id,'/'),pattern='beforeNL')
                  beforeSD_data <- raster(paste0(wd,'/data/BM/beforeSD/',nm,'_',id,'/',beforeSDFile))
                  
                  after10File<-list.files(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/'),pattern='afterNL')
                  after10_data <- raster(paste0(wd,'/data/BM/10-day/',nm,'_',id,'/',after10File))
                  
                  after15File<-list.files(paste0(wd,'/data/BM/15-day/',nm,'_',id,'/'),pattern='afterNL')
                  after15_data <- raster(paste0(wd,'/data/BM/15-day/',nm,'_',id,'/',after15File))
                }
                
                for(vv in 1:2){
                  #vv <-1
                  tgt <- targetV[vv]  
                  roi <- target %>% filter(id %in% tgt)  %>% select('id')
                  #plot(roi)
                  #plot(roi,add=TRUE,border='red',lwd=3) 
                  if(length(intersect(extent(urban_data), roi))==0){next}
                  #plot(roi)  
                  
                  #roi2<- sf::st_transform(roi, crdref)
                  #roi2  <- spPolygons( sf::st_coordinates(roi)[,1:2] , crs=crdref)
                  #plot(roi2)
                  urbanV[vv]<-mean(raster::extract(urban_data,roi)[[1]])
                  beforeV[vv]<-mean(raster::extract(before_data,roi)[[1]])
                  beforeSDV[vv]<-mean(raster::extract(beforeSD_data,roi)[[1]])
                  after10V[vv]<-mean(raster::extract(after10_data,roi)[[1]])
                  after15V[vv]<-mean(raster::extract(after15_data,roi)[[1]])
                }
                
                pixels <- data.frame(target=targetV)
                pixels$urban <- urbanV
                pixels$beforeMean <- beforeV
                pixels$beforeSD <- beforeSDV
                pixels$after10 <- after10V
                pixels$after15 <- after15V
                
                write.csv(pixels,paste0(wd,'/result/NL/',nm,'_',id,'.csv'),row.names = FALSE)  
                
                end_time <- Sys.time()
                end_time - start_time
              }
            })
  stopCluster(cl)
}
