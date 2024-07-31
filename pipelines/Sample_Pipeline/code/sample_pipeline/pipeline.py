from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from sample_pipeline.config.ConfigStore import *
from sample_pipeline.functions import *
from prophecy.utils import *

def pipeline(spark: SparkSession) -> None:
    pass

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Sample_Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/Sample_Pipeline")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/Sample_Pipeline", config = Config)(pipeline)

if __name__ == "__main__":
    main()
