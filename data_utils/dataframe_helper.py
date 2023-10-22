def read_postgrestable_spark(spark, table, config):
    return spark.read \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{config['db_host']}:{config['db_port']}/{config['db_name']}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table) \
        .option("user", config['db_login']) \
        .option("password", config['db_password']) \
        .load()


def write_dataframe_to_postgres(df, table, save_mode, config):
    df.write \
        .format("jdbc") \
        .option("url", f"jdbc:postgresql://{config['db_host']}:{config['db_port']}/{config['db_name']}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table) \
        .option("user", config['db_login']) \
        .option("password", config['db_password']) \
        .mode(save_mode) \
        .save()


def check_format_is_respected(df, columns):
    return all(col in df.columns for col in columns)
