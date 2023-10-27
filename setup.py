import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="yagooglesearch",
    version="2.0.0",
    author="Brennon Thomas, Chris Stenkamp",
    author_email="info@opsdisk.com",
    description="A Python library for executing intelligent, realistic-looking, and tunable Google searches.",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/cstenkamp/yagooglesearch",
    packages=setuptools.find_packages(),
    package_data={
        "yagooglesearch": [
            "user_agents.txt",
            "result_languages.txt",
        ],
    },
    install_requires=[
        "beautifulsoup4>=4.9.3",
        "requests>=2.31.0",
        "requests[socks]",
        "librecaptcha"
    ],
    python_requires=">=3.6",
    license='BSD 3-Clause "New" or "Revised" License',
    keywords="python google search googlesearch",
)
