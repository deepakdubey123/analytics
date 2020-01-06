val saleDF = sc.TextFile("Sale.csv").map(line=>line.split(",")).map(row=>(row(0),row(1),row(2),row(3))).toDF("sale_id","product_id","created_at","units")

val saleDF = sc.TextFile("Sale.csv").map(line=> {
line.split(",")).map(row=> {
(row(0),row(1),
row(2).split("-").map(created_at_tokens => created_at_tokens(0)+created_at_tokens(1))
,row(3)
})
}).toDF("sale_id","product_id","created_at","units")

val productDF = sc.TextFile("Product.csv").map(line=>line.split(",")).map(row=>(row(0),row(1),row(2))).toDF("product_id","name","unit_price")

val joinDF = saleDF.join(productDF, "product_id", "left_outer")

val selectDF = joinDF.select(joinDF("name"),joinDF("unit_price"),joinDF("created_at"),joinDF("units"));

val totalDF = selectDF.select(selectDF("created_at"),selectDF("name"),(selectDF("unit_price") * selectDF("units")).alias("total"))

val totalSaleByDateDF = totalDF.groupBy(totalDF("created_at")).agg(sum(totalDF("total"))).alias("totalSale"))

val maxTotalSaleByMonthDF = totalSaleByDateDF.groupBy(totalDF("created_at")).agg(max(totalSaleByDateDF("totalSale")))

val maxTotalSaleByMonthRDD = maxTotalSaleByMonthDF.rdd

val finalRDD = maxTotalSaleByMonthRDD.map(x=>x.mkString(","))

val header = sc.parallelize(Array("month,total"))

header.union(finalRDD).saveAsTextFile("Solution.csv")

