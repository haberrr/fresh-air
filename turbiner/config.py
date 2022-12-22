from dynaconf import Dynaconf

settings = Dynaconf(
    envvar_prefix="TURBINER",
    settings_files=['settings.yaml', '.secrets.yaml'],
)
