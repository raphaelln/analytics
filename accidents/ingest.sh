#download the data files
wget -O accident-2002.csv http://api.dataprev.gov.br/previdencia/anuario/2002/acidentes-do-trabalho.csv
wget -O accident-2003.csv http://api.dataprev.gov.br/previdencia/anuario/2003/acidentes-do-trabalho.csv
wget -O accident-2004.csv http://api.dataprev.gov.br/previdencia/anuario/2004/acidentes-do-trabalho.csv
wget -O accident-2005.csv http://api.dataprev.gov.br/previdencia/anuario/2005/acidentes-do-trabalho.csv
wget -O accident-2006.csv http://api.dataprev.gov.br/previdencia/anuario/2006/acidentes-do-trabalho.csv
wget -O accident-2007.csv http://api.dataprev.gov.br/previdencia/anuario/2007/acidentes-do-trabalho.csv
wget -O accident-2008.csv http://api.dataprev.gov.br/previdencia/anuario/2008/acidentes-do-trabalho.csv
wget -O accident-2009.csv http://api.dataprev.gov.br/previdencia/anuario/2009/acidentes-do-trabalho.csv


#copy to hdfs
hdfs dfs -mkdir -p /user/cloudera/accidents/ingest
hdfs dfs -copyFromLocal accident-200*.csv /user/cloudera/accidents/ingest/
