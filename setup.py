from setuptools import setup, find_packages

setup(
    name="datatest-pipeline-simulator",
    version="0.1.0",
    description="A comprehensive framework for simulating, testing, and validating data pipelines",
    author="sudo_yaaash",
    author_email="yashr.official2022@gmail.com",
    url="https://github.com/yourusername/datatest-pipeline-simulator",
    packages=find_packages(),
    include_package_data=True,
    python_requires=">=3.8",
    install_requires=[
        "click>=8.0.0",
        "pyspark>=3.0.0",
        "pyyaml>=6.0",
    ],
    entry_points="""
        [console_scripts]
        datatest=src.cli.main:cli
    """,
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Topic :: Software Development :: Testing",
        "Topic :: Scientific/Engineering :: Information Analysis",
    ],
)