from setuptools import find_packages, setup

from djanble import __version__

setup(
    name="djanble",
    version=__version__,
    author="Longern",
    author_email="i@longern.com",
    url="https://github.com/longern/djanble",
    description="Django tablestore database backend",
    keywords="django tablestore",
    license="MIT",
    packages=find_packages(include=("djanble", "djanble.*")),
    include_package_data=True,
    classifiers=[
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Framework :: Django",
        "Environment :: Web Environment",
        "Operating System :: OS Independent",
        "License :: OSI Approved :: MIT License",
    ],
    python_requires=">=3.6",
    install_requires=[
        "django>=2.1",
        "pandasql",
    ],
)
