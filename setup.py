import os

from setuptools import setup

with open(os.path.join(os.path.dirname(__file__), "README.rst")) as wfile:
    README = wfile.read()

setup(
    name="redis-unique-queue",
    version="0.0.2",
    description=(
        "A module that combines the use of redis in-built data types to build a"
        " unique queue for processing and expiry."
    ),
    long_description=README,
    url="https://github.com/geonyoro/redis-exec-retry",
    author="George Nyoro",
    author_email="geonyoro@gmail.com",
    license="MIT License",
    py_modules=["ruqueue"],
    install_requires=["redis>=4.6.0"],
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3.8",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
)
