#Define the arguments#
table<-read.csv('http://archive.ics.uci.edu/ml/machine-learning-databases/00265/CASP.csv', sep=",")
table<-as.numeric(unlist(table))
table<-matrix(table, ncol=10)
table1<-table
table<-to.dfs(table)
#reduce function sums a list of matrices#
reducer=function(.,A){
  keyval(1,list(Reduce('+',A)))}
#1st map-reduce#
mapper3=function(.,Xr){
  Xr<-Xr
  keyval(1,list(nrow(Xr)))}
#Calculate number of rows#
nrow<-values(
  from.dfs(
    mapreduce(
      input= table,
      map=mapper3,
      reduce=reducer,
      combine=T)))[[1]]
N <- nrow
#2nd map-reduce#
mapper2=function(.,Xr){
  Xr<-Xr
  keyval(1,list(colSums(Xr)))}
#Calculate mu#
mu.N<-values(
  from.dfs(
    mapreduce(
      input= table
      , map=mapper2,
      reduce=reducer,
      combine=T)))[[1]]
mu<-mu.N/nrow
#Define new argument using command sweep(x) #
s.table<- sweep(table1, STATS=mu , MARGIN =2)
s.table<-to.dfs(s.table)
#3rd map-reduce#
mapper1=function(.,Xr){
  Xr<-Xr
  keyval(1,list(crossprod(Xr)))}
#Calculate crossprod(X) / (N-1) #
Cov.X.n1<-values(
  from.dfs(
    mapreduce(
      input= s.table
      , map=mapper1,
      reduce=reducer,
      combine=T )))[[1]]
cov.x<-Cov.X.n1/(nrow-1)
cov.x
#Transform to correlation matrix#
cor.x<-cov2cor(cov.x)
