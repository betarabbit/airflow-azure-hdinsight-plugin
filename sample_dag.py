import uuid

import airflow
from airflow.models import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

from airflow.operators.hdinsight_plugin import HDInsightCreateClusterOperator, HDInsightDeleteClusterOperator


default_args = {
    'owner': 'airflow',
    'start_date': '2019-10-09',
    'provide_context': True,
}

client_id = Variable.get('AZURE_CLIENT_ID')
secret = Variable.get('AZURE_CLIENT_SECRET')
tenant = Variable.get('AZURE_TENANT_ID')
subscription_id = Variable.get('AZURE_SUBSCRIPTION_ID')

cluster_name = 'cluster_name'
resource_group_name = 'resource_group_name'
resource_group_location = 'resource_group_location'

dag = DAG(
    dag_id='cluster_test_dag',
    default_args=default_args,
    # at 1 o'clock every day
    schedule_interval='0 1 * * *',
)

create_cluster = HDInsightCreateClusterOperator(
    task_id='create_cluster',
    client_id=client_id,
    secret=secret,
    tenant=tenant,
    subscription_id=subscription_id,
    resource_group_name=resource_group_name,
    resource_group_location=resource_group_location,
    deploy_name=str(uuid.uuid4()),
    template_link='URL_TO_YOUR_ARM_TEMPLATE_JSON_FILE',
    parameters_link='URL_TO_YOUR_ARM_TEMPLATE_PARAMETERS_JSON_FILE',
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag
)

delete_cluster = HDInsightDeleteClusterOperator(
    task_id='delete_cluster',
    client_id=client_id,
    secret=secret,
    tenant=tenant,
    subscription_id=subscription_id,
    resource_group_name=resource_group_name,
    resource_group_location=resource_group_location,
    cluster_name=cluster_name,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag
)

create_cluster >> delete_cluster
