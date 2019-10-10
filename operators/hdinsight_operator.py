import logging
from urllib.parse import urlencode

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from azure.common.credentials import ServicePrincipalCredentials
from azure.mgmt.hdinsight import HDInsightManagementClient
from azure.mgmt.resource import ResourceManagementClient
from azure.mgmt.resource.resources.models import DeploymentMode, DeploymentProperties
from azure.mgmt.resource.resources.v2019_08_01.models import TemplateLink


class HDInsightCreateClusterOperator(BaseOperator):
    """Refences
https://github.com/Azure-Samples/resource-manager-python-resources-and-groups
https://docs.microsoft.com/en-us/python/api/overview/azure/key-vault?toc=%2Fpython%2Fazure%2FTOC.json&view=azure-python
"""

    def __init__(self, client_id, secret, tenant, subscription_id, resource_group_name, resource_group_location,
                 deploy_name, template_link, parameters_link, *args, **kwargs):
        super(HDInsightCreateClusterOperator, self).__init__(*args, **kwargs)
        self.secret = secret
        self.client_id = client_id
        self.tenant = tenant
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location
        self.deploy_name = deploy_name
        self.template_link = template_link
        self.parameters_link = parameters_link

    def create_cluster(self, ):
        client = ResourceManagementClient(
            self.get_credential(), self.subscription_id)
        """Deploy the template to a resource group."""
        client.resource_groups.get(
            self.resource_group_name
        )

        deployment_properties = DeploymentProperties(
            mode=DeploymentMode.incremental,
            template_link=TemplateLink(uri=self.template_link),
            parameters_link=TemplateLink(uri=self.parameters_link))

        deployment_async_operation = client.deployments.create_or_update(
            self.resource_group_name,
            self.deploy_name,
            deployment_properties
        )
        deployment_async_operation.wait()
        return deployment_async_operation.result()

    def execute(self, context):
        logging.info("Executing HDInsightCreateClusterOperator ")
        self.create_cluster()
        logging.info("Finished executing HDInsightCreateClusterOperator")

    def get_credential(self):
        return ServicePrincipalCredentials(
            client_id=self.client_id,
            secret=self.secret,
            tenant=self.tenant,
        )


class HDInsightDeleteClusterOperator(BaseOperator):

    def __init__(self, client_id, secret, tenant, subscription_id, resource_group_name, resource_group_location,
                 cluster_name, *args, **kwargs):
        super(HDInsightDeleteClusterOperator, self).__init__(*args, **kwargs)
        self.secret = secret
        self.client_id = client_id
        self.tenant = tenant
        self.subscription_id = subscription_id
        self.resource_group_name = resource_group_name
        self.resource_group_location = resource_group_location
        self.cluster_name = cluster_name

    def delete_cluster(self):
        client = HDInsightManagementClient(
            self.get_credential(), self.subscription_id)
        delete_poller = client.clusters.delete(self.resource_group_name,
                                               cluster_name=self.cluster_name)
        delete_poller.wait()
        return delete_poller.result()

    def execute(self, context):
        logging.info("Executing HDInsightDeleteClusterOperator ")
        self.delete_cluster()
        logging.info("Finished executing HDInsightDeleteClusterOperator")

    def get_credential(self):
        return ServicePrincipalCredentials(
            client_id=self.client_id,
            secret=self.secret,
            tenant=self.tenant,
        )
