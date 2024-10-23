from setuptools import setup, find_namespace_packages

print(find_namespace_packages("src"))

setup(
    name="airports",
    version="0.0.1",
    author="Simon Dale",
    author_email="simon.dale@bjss.com",
    description="Airports pipeline",
    packages=["airports"],
    classifiers=["Programming Language :: Python :: 3"],
    python_requires=">=3.8",
    zip_safe=False
)
