import setuptools

with open("requirements.txt") as f:
    required = f.read().splitlines()

setuptools.setup(
    name="WeatherPlot",
    version="0.1",
    packages=setuptools.find_packages(),
    install_requires=required,
)
