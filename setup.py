from setuptools import setup, find_packages

setup(
    name='DN_Media',
    version='1.0.0',
    packages=find_packages(), 
    install_requires=[
        'pandas',  
        'sqlite3',
        'argparse',
        'logging',
        'datetime',
        'statsmodels',
        'matplotlib',
        'pickle'
    ],
    entry_points={
        'console_scripts': [
            'run_pipeline=dn_media_pipeline:main', 
        ],
    },
    python_requires='>=3.9',  
)