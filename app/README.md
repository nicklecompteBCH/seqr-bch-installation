# README

```{bash}
spark-submit   --jars $HAIL_HOME/hail-all-spark.jar   --conf spark.driver.extraClassPath=$HAIL_HOME/hail-all-spark.jar:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*   --conf spark.executor.extraClassPath=./hail-all-spark.jar:/etc/hadoop/conf:/etc/hive/conf:/usr/lib/hadoop-lzo/lib/*:/usr/share/aws/aws-java-sdk/*:/usr/share/aws/emr/emrfs/conf:/usr/share/aws/emr/emrfs/lib/*:/usr/share/aws/emr/emrfs/auxlib/*   --conf spark.serializer=org.apache.spark.serializer.KryoSerializer   --conf spark.kryo.registrator=is.hail.kryo.HailKryoRegistrator --conf spark.speculation=true load_vcfs_to_emr.py -p alan.csv
```
