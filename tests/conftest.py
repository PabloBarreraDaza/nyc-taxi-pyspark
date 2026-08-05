import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src", "pipeline"))

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """
    A single SparkSession shared across all tests in this session.
    Creating a new one per test would be very slow (JVM startup cost).
    """
    spark = SparkSession.builder \
        .appName("PytestSparkSession") \
        .master("local[2]") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    yield spark

    spark.stop()