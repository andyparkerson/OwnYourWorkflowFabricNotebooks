# # test_startup.py
# import os
# from pyspark.sql import SparkSession

# # Print launch command for visibility
# os.environ["SPARK_PRINT_LAUNCH_COMMAND"] = "1"

# spark = (
#     SparkSession.builder
#     .master("local[1]")
#     .appName("startup-smoke-test")
#     # Pin driver to loopback to avoid VPN/proxy issues
#     .config("spark.driver.host", "localhost")
#     .config("spark.driver.bindAddress", "127.0.0.1")
#     # Use fixed ports to avoid conflicts (pick unused ports if these are busy)
#     .config("spark.driver.port", "50050")
#     .config("spark.blockManager.port", "50051")
#     .config("spark.ui.port", "4041")
#     # Prefer IPv4 (some stacks hang on IPv6)
#     .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
#     .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
#     # Use local, writable dirs
#     .config("spark.sql.warehouse.dir", r"C:\temp\spark-warehouse")
#     .config("spark.local.dir", r"C:\temp\spark-tmp")
#     # Show progress
#     .config("spark.ui.showConsoleProgress", "true")
#     .getOrCreate()
# )

# print("Spark version:", spark.version)
# spark.range(5).show()
# spark.stop()
# print("Smoke test OK")


from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

builder = (
    SparkSession.builder
    .master("local[1]")
    .appName("delta-pip-test")
    .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.warehouse.dir", r"C:\temp\spark-warehouse")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.port", "50160")
    .config("spark.blockManager.port", "50161")
    .config("spark.ui.port", "4042")
    .config("spark.driver.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    .config("spark.executor.extraJavaOptions", "-Djava.net.preferIPv4Stack=true")
    .config("spark.local.dir", r"C:\temp\spark-tmp")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
print("Spark:", spark.version)