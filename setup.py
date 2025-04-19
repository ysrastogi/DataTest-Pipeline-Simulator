from setuptools import setup, find_packages

setup(
    name="datatest-pipeline-simulator",
    version="0.1.0",
    description="A comprehensive framework for simulating, testing, and validating data pipelines",
    author="sudo_yaaash",
    author_email="yashr.official2022@gmail.com",
    url="https://github.com/yourusername/datatest-pipeline-simulator",
    packages=find_packages(exclude=["tests", "tests.*"]),
    python_requires=">=3.8",
    install_requires=[
        "pyspark>=3.2.0",
        "pytest>=6.0.0",
        "pandas>=1.3.0",
        "numpy>=1.20.0",
        "PyYAML>=6.0",
        "click>=8.0.0",
        "fastapi>=0.68.0",
        "uvicorn>=0.15.0",
        "streamlit>=1.9.0",
        "matplotlib>=3.4.0",
        "seaborn>=0.11.0",
        "pyarrow>=5.0.0",
    ],
    extras_require={
        "dev": [
            "black",
            "flake8",
            "mypy",
            "isort",
            "pytest-cov",
            "sphinx",
            "pre-commit",
        ],
        "test": [
            "pytest",
            "pytest-mock",
            "pytest-cov",
        ],
    },
    entry_points={
        "console_scripts": [
            "datatest=interfaces.cli.commands:main",
        ],
    },
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