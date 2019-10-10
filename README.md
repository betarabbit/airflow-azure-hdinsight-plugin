# airflow-azure-hdinsight-plugin
Airflow plugin to create/delete Azure HDInsight cluster

## Usage
Put this plugin into your Airflow plugin folder, in your DAG file, use the following code to import operators

```py
from airflow.operators.hdinsight_plugin import HDInsightCreateClusterOperator, HDInsightDeleteClusterOperator
```

Please find more examples in `sample_dag.py`
