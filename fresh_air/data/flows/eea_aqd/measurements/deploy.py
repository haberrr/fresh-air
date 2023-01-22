from collections import namedtuple

import typer
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from prefect.filesystems import LocalFileSystem
from prefect.filesystems import GitHub
from prefect_gcp.cloud_run import CloudRunJob

from fresh_air.data.flows.eea_aqd.measurements.load import load_eeq_aqd_measurements

Config = namedtuple('Config', ['pollutant', 'code'])


def deploy():
    configs = [
        Config(pollutant='PM2.5', code=6001),
        Config(pollutant='PM10', code=5),
    ]

    for config in configs:
        deployment = Deployment.build_from_flow(
            flow=load_eeq_aqd_measurements,  # noqa
            name=f'EEA AQD: update {config.pollutant} measurements',
            version=1,
            parameters={
                'pollutant_code': config.code,
                'batch_size': 20,
            },
            storage=GitHub.load('repo'),
            # infrastructure=CloudRunJob.load("cloudrun"),
            infrastructure=DockerContainer.load('docker-fresh-air'),
        )

        deployment.apply()  # noqa


if __name__ == '__main__':
    typer.run(deploy)
