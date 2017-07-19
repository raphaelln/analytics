from pyspark import SparkContext,SparkConf, HiveContext , Row

conf = SparkConf()
sc = SparkContext(conf = conf)
sqlContext = HiveContext(sc)

#load csv and ignore header.
accidents = sc.textFile("/user/cloudera/accidents/ingest/*.csv").filter(lambda rec: not rec.startswith('"')).map(lambda rec: rec.split(","))
#create Dataframe
accidentsDF = accidents.map(lambda rec: Row(year=int(rec[0]), city=rec[2], uf=rec[3], qtd_general=int(rec[4]), \
	                        qtd_route=int(rec[5]), qtd_disease=int(rec[6]), qtd_death= int(rec[7]), qtd_no_cat=int(rec[8]))).toDF()

#filter records, only consider record from 2003 to 2007 and register accidents table for further process
accidentsDF.filter("year >= 2003 and year<=2007").registerTempTable("accidents")

#sum total accidents occurred between 2004 - 2007
newAccidents = sqlContext.sql("select total.city,total.uf, sum(total.qtdTotal) as newAccidents from "\
								"("\
						    	    "select city,uf, (qtd_general+qtd_route+qtd_disease+qtd_death+qtd_no_cat) as qtdTotal "\
								    "from accidents "\
								    "where year >= 2004"\
								 ") as total group by total.city,total.uf")								
newAccidents.registerTempTable("newAccidents")

#sum total accidents occurred in 2003 (not required but will be necessary to show the increase percentage between 2003 to 2007)
totalAccidents2003 = sqlContext.sql("select city,uf, (qtd_general+qtd_route+qtd_disease+qtd_death+qtd_no_cat) as qtdTotal "\
	                                "from accidents "\
	                                "where year=2003")	                                
totalAccidents2003.registerTempTable("accidents2003")


#rank results by uf, get the top 3 cities from each state and calculate % of grew accidents from 2003 to 2007
result = sqlContext.sql("select result.city,result.uf, a2003.qtdTotal as 2003_accidents, result.newAccidents as 2004_2007_accidents,"\
					    "(a2003.qtdTotal + result.newAccidents) as totalAccidents, round((result.newAccidents * 100.0 / a2003.qtdTotal),2) as increase_percent from "\
						"("\
	                       " select city, uf, newAccidents, rank() over (partition by uf order by newAccidents desc) as accidentsRank from newAccidents "\
 						 ") as result inner join accidents2003 a2003 on  a2003.city = result.city and a2003.uf = result.uf"\
	                    " where result.accidentsRank <=3 order by uf,2004_2007_accidents desc")

result.repartition(1).write.mode("overwrite").parquet("/user/cloudera/accidents/result/result_parquet")
sqlContext.setConf("spark.sql.json.compression.codec","uncompressed")
result.repartition(1).write.mode("overwrite").json("/user/cloudera/accidents/result/result_json")
