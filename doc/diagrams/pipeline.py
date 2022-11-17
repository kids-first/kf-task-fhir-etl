from diagrams import Diagram, Cluster
from diagrams.aws.compute import Batch as BATCH
from diagrams.aws.database import RDS
from diagrams.aws.compute import ECS
from diagrams.aws.integration import SF
from diagrams.aws.integration import Eventbridge as EB
from diagrams.aws.network import DirectConnect as DC
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.compute import Server as Svr
from diagrams.k8s.compute import Deployment
from diagrams.aws.network import VPCPeering
from diagrams.aws.network import ELB as ELB
from diagrams.aws.compute import ElasticContainerServiceContainer as Container

with Diagram("Pipeline", show=False, direction="TB"):
    with Cluster("kf-strides"):
        with Cluster("kf-strides-us-east-1-vpc-prd"):
            with Cluster("kf-strides-us-east-1-vpc-public-subnet"):
                lb = ELB("kf-api-fhir-service-alb")
            with Cluster("kf-strides-us-east-1-vpc-private-subnet"):
                dataservice_rds = RDS("kf-strides-dataservice-rds")
                with Cluster("Batch ECS Cluster"):
                    batch_ecs_cluster = ECS("BatchECSCluster")
                    batch_container = Container("BatchECSContainer")
                ETL = (
                    EB("cron")
                    - SF("StepFunction")
                    - BATCH("BatchDefinition")
                    - batch_ecs_cluster
                    - batch_container
                )
                with Cluster("FHIR Smile CDR"):
                    ecs_cluster_app = ECS("kf-strides-us-east-1-ecs")
                    fhir_container = Container("kf-api-fhir-service")
                    ecs_cluster_app - fhir_container
        lb >> fhir_container
        batch_container >> fhir_container
        batch_container >> dataservice_rds
