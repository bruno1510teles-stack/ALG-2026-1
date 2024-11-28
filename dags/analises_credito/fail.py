from datetime import datetime
import pendulum
from airflow.decorators import dag, task
from analises_credito.kafka_producer import Kafka


def erro():
    errorBody = {
        "@timestamp": datetime.now(),
        "@metadata": {
            "beat": "filebeat",
            "type": "_doc",
            "version": "8.10.4"
        },
        "log": {
            "offset": 7261461,
            "file": {
            "path": "/var/log/containers/ccred-7d6c66ff47-m2jxm_prd-default_ccred-backend-91a4c5de3b0220e250ea4de26e638fb92bd9337cb0aff8f7d57a8410e4307696.log"
            }
        },
        "kubernetes": {
            "namespace_labels": {
            "kubernetes_io/metadata_name": "prd-default",
            "name": "prd-default"
            },
            "replicaset": {
            "name": "ccred-7d6c66ff47"
            },
            "deployment": {
            "name": "ccred"
            },
            "pod": {
            "uid": "34bdfa09-0f06-4ff1-9bad-6588c4ca2c98",
            "ip": "10.100.50.54",
            "name": "ccred-7d6c66ff47-m2jxm"
            },
            "namespace": "prd-default",
            "labels": {
            "pod-template-hash": "7d6c66ff47",
            "app_kubernetes_io/instance": "prd-default",
            "security_istio_io/tlsMode": "istio",
            "sidecar_istio_io/inject": "true",
            "service_istio_io/canonical-name": "ccred",
            "log": "json",
            "app_kubernetes_io/name": "ccred",
            "version": "0.1.183",
            "service_istio_io/canonical-revision": "0.1.183",
            "app_kubernetes_io/managed-by": "thiago.assis",
            "app": "ccred"
            },
            "container": {
            "name": "ccred-backend"
            },
            "node": {
            "name": "ip-10-100-32-216.ec2.internal",
            "uid": "16985b97-4995-443d-82cf-29f0fc3312fa",
            "labels": {
                "eks_amazonaws_com/sourceLaunchTemplateId": "lt-0fddb6b7f2c434204",
                "eks_amazonaws_com/sourceLaunchTemplateVersion": "1",
                "topology_ebs_csi_aws_com/zone": "us-east-1b",
                "eks_amazonaws_com/capacityType": "ON_DEMAND",
                "node_kubernetes_io/instance-type": "m5a.xlarge",
                "failure-domain_beta_kubernetes_io/zone": "us-east-1b",
                "eks_amazonaws_com/nodegroup-image": "ami-09dea1d77f0b6e0ed",
                "kubernetes_io/os": "linux",
                "beta_kubernetes_io/os": "linux",
                "beta_kubernetes_io/arch": "amd64",
                "beta_kubernetes_io/instance-type": "m5a.xlarge",
                "kubernetes_io/arch": "amd64",
                "kubernetes_io/hostname": "ip-10-100-32-216.ec2.internal",
                "failure-domain_beta_kubernetes_io/region": "us-east-1",
                "topology_kubernetes_io/region": "us-east-1",
                "k8s_io/cloud-provider-aws": "25d05bbf9eaedd08a0ea15a2171a3dc0",
                "topology_kubernetes_io/zone": "us-east-1b",
                "eks_amazonaws_com/nodegroup": "OnDemand_Apps"
            },
            "hostname": "ip-10-100-32-216.ec2.internal"
            },
            "namespace_uid": "419854d8-1452-4b4f-a700-03a5824eff07"
        },
        "host": {
            "architecture": "x86_64",
            "os": {
            "platform": "ubuntu",
            "version": "20.04.6 LTS (Focal Fossa)",
            "family": "debian",
            "name": "Ubuntu",
            "kernel": "5.10.219-208.866.amzn2.x86_64",
            "codename": "focal",
            "type": "linux"
            },
            "containerized": True,
            "ip": [
            "10.100.32.216",
            "fe80::8ff:ddff:fe63:8035",
            "fe80::c4f9:86ff:fe9a:2303",
            "10.100.59.68",
            "fe80::8ff:c1ff:fec1:e9fb",
            "fe80::287a:95ff:fe11:7365",
            "fe80::741d:d7ff:fedc:6db8",
            "fe80::38fe:55ff:fecf:25c2",
            "fe80::fc4e:5cff:fe42:3e",
            "fe80::3cd0:a9ff:fed5:436",
            "fe80::bc62:feff:feff:d173"
            ],
            "name": "ip-10-100-32-216.ec2.internal",
            "mac": [
            "0A-FF-C1-C1-E9-FB",
            "0A-FF-DD-63-80-35",
            "2A-7A-95-11-73-65",
            "3A-FE-55-CF-25-C2",
            "3E-D0-A9-D5-04-36",
            "76-1D-D7-DC-6D-B8",
            "BE-62-FE-FF-D1-73",
            "C6-F9-86-9A-23-03",
            "FE-4E-5C-42-00-3E"
            ],
            "hostname": "ip-10-100-32-216.ec2.internal"
        },
        "log.logger": "org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer",
        "error.type": "java.lang.IllegalStateException",
        "error.stack_trace": "Erro na execução da DAG teste",
        "container": {
            "id": "91a4c5de3b0220e250ea4de26e638fb92bd9337cb0aff8f7d57a8410e4307696",
            "runtime": "containerd",
            "image": {
            "name": "artifactory.alpenet.com.br/docker-release/alpe/ccred:0.1.183"
            }
        },
        "service.name": "ccred",
        "ecs.version": "1.2.0",
        "process.thread.name": "org.springframework.kafka.KafkaListenerEndpointContainer#3-0-C-1",
        "error.message": "dag_run_id: teste dag id",
        "log.level": "ERROR",
        "message": "Erro na execução da DAG teste",
        "input": {
            "type": "container"
        },
        "cloud": {
            "instance": {
            "id": "i-07c76cf7edd8452f8",
            "name": "ip-10-100-32-216.ec2.internal"
            },
            "machine": {
            "type": "m5a.xlarge"
            },
            "availability_zone": "us-east-1b",
            "service": {
            "name": "Nova"
            },
            "provider": "openstack"
        },
        "stream": "stdout",
        "ecs": {
            "version": "8.0.0"
        },
        "agent": {
            "id": "cb4228d9-d2df-463d-a2cf-b7d71547ae77",
            "name": "ip-10-100-32-216.ec2.internal",
            "type": "filebeat",
            "version": "8.10.4",
            "ephemeral_id": "891f18a2-c1d9-40a7-9ebc-2411ac1265d3"
        },
        "event.dataset": "ccred.log"
    }
    Kafka.send(topic="prd.default.jira-connector.v1.logs.create.out", key="teste", value= errorBody)

default_args = {
    'owner': 'Rafael Leite'
}

@dag(
    default_args=default_args,
    start_date=pendulum.today('UTC').add(days=-1),
    schedule= None,
    description='DAG consulta trino e faz upload do arquivo para minio',
    catchup=False,
    on_failure_callback=erro()
)
def fail():

    @task
    def fail():
        raise ValueError("Erro")
    relato = fail()
    relato

dag_instance = fail()