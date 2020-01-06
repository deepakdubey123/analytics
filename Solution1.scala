val saleDF = sc.TextFile("Sale.csv").map(line=>line.split(",")).map(row=>(row(0),row(1),row(2),row(3))).toDF("sale_id","product_id","created_at","units")

val productDF = sc.TextFile("Product.csv").map(line=>line.split(",")).map(row=>(row(0),row(1),row(2))).toDF("product_id","name","unit_price").registerTempTable("Product")

val joinDF = saleDF.join(productDF, "product_id", "left_outer")

val selectDF = joinDF.select(joinDF("name"),joinDF("unit_price"),joinDF("created_at"),joinDF("units"));

val totalDF = selectDF.select(selectDF("created_at"),selectDF("name"),(selectDF("unit_price") * selectDF("units")).alias("total"))

val maxSaleByDateDF = totalDF.groupBy(totalDF("created_at")).agg(max(totalDF("total")))

val maxSaleByDateRDD = maxSaleByDateDF.rdd

val groupRDD = maxSaleByDateRDD.groupByKey

val transformRDD = groupRDD.map(rec => {
	(rec._1, 
	rec._2.map(name => {
			if (name.size == 1) name
			else {
			val result = ""
			name.foreach{
			result = result + name + ", "}
			}			
			})
	)
})

val finalRDD = transformRDD.map(x=>x.mkString(","))

val header = sc.parallelize(Array("day,top"))

header.union(finalRDD).saveAsTextFile("Solution.csv")
