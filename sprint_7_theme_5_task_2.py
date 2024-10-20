def tag_tops(date, depth, write_path):
    path_list = []
    start_dt = datetime.strptime(date, "%Y-%m-%d")
    for i in range(depth):
        path_dt = start_dt - timedelta(days=i)
        path = f"/user/dimanebori/data/events/date={path_dt.strftime('%Y-%m-%d')}/event_type=message"
        path_list.append(path)
    messages = spark.read.parquet(*input_paths(date, depth))
    window = Window().partitionBy(F.col('user_id')).orderBy(F.desc('msg_count'), F.asc('tag'))
    df_to_save = messages.selectExpr(["event.message_from as user_id", "event.message_id", "explode(event.tags) as tag"]) \
                         .groupBy('user_id', 'tag').agg(F.expr('count(message_id) as msg_count')) \
                         .withColumn('tag_order', F.row_number().over(window)) \
                         .where('tag_order <= 3') \
                         .groupBy('user_id') \
                         .pivot("tag_order", ["1", "2", "3"]) \
                         .agg(F.first("tag")) \
                         .withColumnRenamed("1", "tag_top_1") \
                         .withColumnRenamed("2", "tag_top_2") \
                         .withColumnRenamed("3", "tag_top_3") \
                         .orderBy('user_id')
    df_to_save.repartition(1).write.format('parquet').mode('overwrite').save(f'{write_path}')
    result = 'Done'
    return result