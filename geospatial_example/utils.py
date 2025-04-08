#****************************************************************************
# (C) Cloudera, Inc. 2020-2025
#  All rights reserved.
#
#  Applicable Open Source License: GNU Affero General Public License v3.0
#
#  NOTE: Cloudera open source products are modular software products
#  made up of hundreds of individual components, each of which was
#  individually copyrighted.  Each Cloudera open source product is a
#  collective work under U.S. Copyright Law. Your license to use the
#  collective work is as provided in your written agreement with
#  Cloudera.  Used apart from the collective work, this file is
#  licensed for your use pursuant to the open source license
#  identified above.
#
#  This code is provided to you pursuant a written agreement with
#  (i) Cloudera, Inc. or (ii) a third-party authorized to distribute
#  this code. If you do not have a written agreement with Cloudera nor
#  with an authorized and properly licensed third party, you do not
#  have any rights to access nor to use this code.
#
#  Absent a written agreement with Cloudera, Inc. (“Cloudera”) to the
#  contrary, A) CLOUDERA PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY
#  KIND; (B) CLOUDERA DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED
#  WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT LIMITED TO
#  IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND
#  FITNESS FOR A PARTICULAR PURPOSE; (C) CLOUDERA IS NOT LIABLE TO YOU,
#  AND WILL NOT DEFEND, INDEMNIFY, NOR HOLD YOU HARMLESS FOR ANY CLAIMS
#  ARISING FROM OR RELATED TO THE CODE; AND (D)WITH RESPECT TO YOUR EXERCISE
#  OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, CLOUDERA IS NOT LIABLE FOR ANY
#  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR
#  CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO, DAMAGES
#  RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF
#  BUSINESS ADVANTAGE OR UNAVAILABILITY, OR LOSS OR CORRUPTION OF
#  DATA.
#
# #  Author(s): Paul de Fusco
#***************************************************************************/

import os
import numpy as np
from datetime import datetime
import dbldatagen as dg
from dbldatagen import DataGenerator
from pyspark.sql import SparkSession
import sys, random, os, json, random, configparser
from pyspark.sql.functions import col
from pyspark.sql.types import LongType, FloatType, IntegerType, StringType, \
                              DoubleType, BooleanType, ShortType, \
                              TimestampType, DateType, DecimalType, \
                              ByteType, BinaryType, ArrayType, MapType, \
                              StructType, StructField

class GeospatialDataGen():
    """
    Class to generate a sample dataset with geospatial types such as points, linestrings and polygons.
    """

    def __init__(self, spark, num_rows, partitions_requested, min_lon, max_lon, min_lat, max_lat):
        self.spark = spark
        self.num_rows = num_rows
        self.partitions_requested = partitions_requested
        self.min_lon = min_lon
        self.max_lon = max_lon
        self.min_lat = min_lat
        self.max_lat = max_lat

    def datagen(self):
        """
        Method to create the sample dataset with geospatial types such as points, linestrings and polygons.
        """

        point_wkt_format = 'POINT({} {})'
        line_wkt_format = 'LINESTRING({})'
        polygon_wkt_format = 'POLYGON(({}))'

        data_gen = dg.DataGenerator(
            spark,
            rows=num_rows,
            partitions=partitions_requested,
            columns=[
                dg.Column("unique_id", "string", minValue=1, maxValue=num_rows, step=1, prefix='ID', random=True),
                dg.Column("cat_col", values=["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O"]),
                dg.Column("point", dg.DataType.string, formatter=lambda: point_wkt_format.format(dg.randint(self.min_lon, self.max_lon), dg.randint(self.min_lat, self.max_lat))),
                dg.Column("line", dg.DataType.string, formatter=lambda: line_wkt_format.format(",".join([f"{dg.randint(self.min_lon, self.max_lon)} {dg.randint(self.min_lat, self.max_lat)}" for _ in range(2)]))),
                dg.Column("polygon", dg.DataType.string, formatter=lambda: polygon_wkt_format.format(",".join([f"{dg.randint(self.min_lon, self.max_lon)} {dg.randint(self.min_lat, self.max_lat)}" for _ in range(4)])))
            ]
        )

        for i in range(1, 10):
            col_n = f"num_col_{i}"
            data_gen = data_gen.withColumn(col_n, "float", minValue=1, maxValue=100000, random=True)

        df = data_gen.build()

        return df
