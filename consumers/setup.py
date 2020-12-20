from setuptools import find_packages, setup

requires = [
    "colorlog>=3.1.4",
    "dataclasses-avroschema",
    "faust",
    "python-schema-registry-client",
    "yarl<1.6.0,>=1.0",
    "multidict<5.0,>=4.5",
    "simple-settings",
    "typing-extensions",
]

setup(
    name='faust_project',
    version='1.0.0',
    description='Use Faust to process medicare data',
    author='Cesar Flores',
    author_email='cfloressuazo@gmail.com',
    url='http://localhost',
    platforms=['any'],
    license='Proprietary',
    packages=find_packages(exclude=['tests', 'tests.*']),
    include_package_data=True,
    zip_safe=False,
    install_requires=requires,
    python_requires='~=3.6',
    entry_points={
        'console_scripts': [
            'consumers = faust_project.app:main',
        ],
        'faust.codecs': [
            'avro_medicare_value = faust_project.codecs.avro:avro_medicare_value_codec',
            'avro_medicare_key = faust_project.codecs.avro:avro_medicare_key_codec'
        ],
    },
)
