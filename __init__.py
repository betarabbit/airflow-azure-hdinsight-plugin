from airflow.plugins_manager import AirflowPlugin

from airflow.utils.decorators import apply_defaults
from .operators.hdinsight_operator import HDInsightCreateClusterOperator, HDInsightDeleteClusterOperator


class HdinsightPlugin(AirflowPlugin):
    name = "hdinsight_plugin"
    operators = [HDInsightCreateClusterOperator, HDInsightDeleteClusterOperator]
