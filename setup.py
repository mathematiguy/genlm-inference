from setuptools import setup, find_packages

setup(
    name="pkg",
    version="0.1",
    packages=find_packages(exclude=["tests*"]),
    description="A minimal python project template",
    url="https://github.com/mathematiguy/minimal-project",
    author="Caleb Moses",
    author_email="calebjdmoses@gmail.com",
    include_package_data=True
)
