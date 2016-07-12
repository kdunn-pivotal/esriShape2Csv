# esriShape2Csv
#### A Spring Cloud Stream splitter processor module for converting ESRI shapefiles into a delimited format

Shapefile -> Geotools DataSource(java.File.io) -> Delimited Flatfile
This has been tested using the Kafka Binder for Spring Cloud Stream.

This tool uses the Geotools library in a Spring Cloud Dataflow stream module to convert an 
ESRI shapefile into a delimited format for loading to a GIS system, like PostGIS. This is
intended to be a Spring Cloud Stream equivalent of the PostGIS utility `shp2pgsql`.

An example registration and stream definition might look something like this:

```
dataflow:>app register --type processor --name shp --uri file://Users/kdunn/.m2/repository/io/pivotal/pde-scdf-shp2csv/0.0.1-SNAPSHOT/pde-scdf-shp2csv-0.0.1-SNAPSHOT.jar

dataflow:>stream create --name convertShapeToCsv --definition 'file  --file.consumer.mode="ref" --file.directory="/Users/kdunn/Desktop/gisData/" --file.filename-regex=".shp" | shp | file --name sink'
```

This module can also be used with shapefile data in Hadoop/HDFS by exposing the shapefiles via 
the NFS Gateway on the nodes where this stream runs.

In the future, the row and column delimiters could be exposed as module parameters. Also, 
support for adjusting the endianness to match the target system. 
