from setuptools import setup, find_packages

setup(
    name='cbr_currencies',
    version='0.1.0',
    author='Nikita Protasov',
    author_email='gomer@12370@gmail.com',
    description='Пакет для работы с валютами ЦБР',
    packages=find_packages(),  
    python_requires='>=3.10',
    install_requires=[
        'pandas>=2.0',
        'numpy>=1.24',
        'python-decouple',
        'zeep==4.3.1',
        'psycopg2-binary==2.9.10'
    ],
)
