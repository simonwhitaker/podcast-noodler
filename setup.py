from setuptools import find_packages, setup

setup(
    name="podcast_noodler",
    packages=find_packages(exclude=["podcast_noodler_tests"]),
    install_requires=[
        "dagster",
        "dagster-cloud"
    ],
    extras_require={"dev": ["dagster-webserver", "pytest"]},
)
