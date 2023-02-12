from collections import namedtuple

import typer
from prefect.context import get_settings_context
from prefect.deployments import Deployment

from fresh_air.data.flows.eea_aqd.measurements.load import eea_aqd_measurements_core

Config = namedtuple('Config', ['pollutant', 'code'])

configs = [
    Config(pollutant='PM2.5', code=6001),
    Config(pollutant='PM10', code=5),
]


def deploy():
    context = get_settings_context()

    if context.profile.name == 'dev':
        from prefect.filesystems import LocalFileSystem as Storage
        from prefect.infrastructure.docker import DockerContainer as Infrastructure
    else:
        from prefect.filesystems import GitHub as Storage
        from prefect_gcp.cloud_run import CloudRunJob as Infrastructure

    for config in configs:
        # noinspection PyTypeChecker
        deployment: Deployment = Deployment.build_from_flow(
            flow=eea_aqd_measurements_core,
            name=f'EEA AQD: update {config.pollutant} measurements',
            version=1,
            parameters={
                'pollutant_code': config.code,
                'batch_size': 20,
            },
            storage=Storage.load('repo'),
            infrastructure=Infrastructure.load("cloudrun"),
            skip_upload=True,
        )

        deployment.apply()


if __name__ == '__main__':
    typer.run(deploy)
