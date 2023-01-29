from prefect.infrastructure.docker import DockerContainer, ImagePullPolicy
from prefect.filesystems import LocalFileSystem
from prefect.context import get_settings_context

infrastructure = DockerContainer(
    image='us-central1-docker.pkg.dev/turbiner/prefect/fresh-air:latest',
    image_pull_policy=ImagePullPolicy.NEVER,
    env={
        'FRESH_AIR_STORAGE__USE_STORAGE': 'local',
        'FRESH_AIR_STORAGE__LOCAL__BASE_DIR': '/shared/data',
        'FRESH_AIR_STORAGE__LOCAL__FORMAT': 'avro',
    },
    volumes=[
        '/home/haberr/.fresh_air:/shared',
        '/home/haberr/Documents/Personal/turbiner/fresh_air:/opt/prefect/fresh_air',
    ],
    mem_limit='2g',
    auto_remove=True,
)

storage = LocalFileSystem(
    basepath='/opt/prefect/fresh_air',
)

if __name__ == '__main__':
    context = get_settings_context()

    assert context.profile == 'dev', 'Blocks can be deployed only for dev Prefect profile.'

    infrastructure.save(
        name='cloudrun',
        overwrite=True,
    )

    storage.save(
        name='repo',
        overwrite=True,
    )
