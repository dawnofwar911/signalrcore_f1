import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="signalrcore-F1",
    version="0.1.4",
    author="dawnofwar911",
    author_email="alexander-daniels@hotmail.co.uk",
    description="A Python SignalR Core client(json and messagepack), that is designed to work with the F1 live timing signalr feed",
    keywords="signalr core client 3.1",
    long_description=long_description,
    long_description_content_type="text/markdown",
    license_file="LICENSE",
    url="https://github.com/dawnofwar911/signalrcore_f1",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6"
    ],
    install_requires=[
        "requests>=2.22.0",
        "websocket-client==1.0.0",
        "msgpack==1.0.2"
    ]
)
