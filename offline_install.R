## STEP1: get r packages dependency
getPackages <- function(packs){
  packages <- unlist(
    tools::package_dependencies(packs, available.packages(),
                                which=c("Depends", "Imports"), recursive=TRUE)
  )
  packages <- union(packs, packages)
  packages
}
packages <- getPackages(c("sparklyr"))
packages

#STEP2: download all packages what target packages need!
download.packages(packages, destdir="~/sparklyr", type="source")

#above scripts MUST BE under the online env to excute!!!


#STEP3: make packages dependency index
library(tools)
write_PACKAGES("~/sparklyr")
#below scripts executed on offline env. 
#STEP4: upload packages and dependency index to server

#STEP5: install packages
install.packages("sparklyr", contriburl="file:///home/ebdmusr1/sparklyr")
