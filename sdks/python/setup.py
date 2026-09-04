from setuptools import setup, find_packages

with open("../../../sdks/python/README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="tollmeshcache",
    version="1.1.0",
    author="TollMesh Team",
    author_email="team@tollmesh.io",
    description="Python SDK for TollMeshCache - Distributed CRDT-based caching",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/TollMesh/toll-mesh-store",
    packages=find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "License :: OSI Approved :: Apache Software License",
        "Operating System :: OS Independent",
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    python_requires=">=3.8",
    install_requires=[
        "requests>=2.28.0",
    ],
    extras_require={
        "dev": [
            "pytest>=7.0.0",
            "pytest-asyncio>=0.20.0",
            "pytest-cov>=4.0.0",
            "black>=22.0.0",
            "flake8>=4.0.0",
            "mypy>=0.990",
        ],
        "async": ["httpx>=0.24.0"],
        "grpc": ["grpcio>=1.50.0", "grpcio-tools>=1.50.0", "protobuf>=3.20.0"],
    },
    entry_points={
        "console_scripts": [
            "tollmesh-init=tollmeshcache.cli:init",
            "tollmesh-config=tollmeshcache.cli:config",
        ],
    },
)
