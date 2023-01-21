from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="FRESH_AIR",
    settings_files=['settings.yaml', '.secrets.yaml'],
)
