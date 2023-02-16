import typer
from prefect.context import get_settings_context
from prefect.deployments import Deployment

from fresh_air.data.flows.gfs.forecasts.load import save_gfs_forecasts


def deploy():
    context = get_settings_context()

    if context.profile.name == 'dev':
        from prefect.filesystems import LocalFileSystem as Storage
        from prefect.infrastructure.docker import DockerContainer as Infrastructure
    else:
        from prefect.filesystems import GitHub as Storage
        from prefect_gcp.cloud_run import CloudRunJob as Infrastructure

    # noinspection PyTypeChecker
    deployment: Deployment = Deployment.build_from_flow(
        flow=save_gfs_forecasts,
        name=f'Update GFS forecasts',
        version=1,
        storage=Storage.load('repo'),
        infrastructure=Infrastructure.load("cloudrun"),
        skip_upload=True,
    )

    deployment.apply()


if __name__ == '__main__':
    typer.run(deploy)
