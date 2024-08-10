from setuptools import find_packages, setup

setup(
    name="streaming_demo_project",
    version="0.0.1",
    packages=find_packages(),
    package_data={
        "streaming_demo_project": [
            "dbt-project/**/*",
        ],
    },
    install_requires=[
        "dagster",
        "dagster-cloud",
        "dagster-dbt",
        "dbt-redshift<1.9",
    ],
    extras_require={
        "dev": [
            "dagster-webserver",
        ]
    },
)