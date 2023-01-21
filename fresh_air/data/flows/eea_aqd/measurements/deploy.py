import typer
from prefect.deployments import Deployment
from prefect.infrastructure.docker import DockerContainer
from prefect.filesystems import GitHub

from fresh_air.data.flows.eea_aqd.measurements.load import load_eeq_aqd_measurements


def deploy():
    deployment = Deployment(
        flow=load_eeq_aqd_measurements,
        name='EEA AQD: update P2.5 measurements',
        version=1,
        parameters={
            'pollutant_code': 6001,
            'batch_size': 20,
        },
        storage=GitHub.load('repo'),
        infrastructure=DockerContainer.load("prefect"),
        entrypoint='fresh_air/data/flows/eea_aqd/measurements/load.py',
    )

    deployment.apply()


if __name__ == '__main__':
    typer.run(deploy)
