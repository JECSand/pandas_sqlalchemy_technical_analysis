# pandas_sqlalchemy_technical_analysis
Scripted examples of Beta and RSI algorithms running against historical stock data from my yahoofinancials module. The resulting data frames are then pushed to a cloud base PostgreSQL Database. More algorithms are coming!

## Features
* An algorithm to solve for the beta values of a set of stocks against an index fund
* An algorithm to determine the RSI of a set of stocks
* sqlalchemy functionality to create PostgreSQL tables and to push results to the cloud
* Currently set up to only run on Python 3.x

## How to use
1. Make sure you have Python 3+ set up
2. Clone this repository from git-hub
```R
git clone https://github.com/JECSand/pandas_sqlalchemy_technical_analysis.git
```
3. Go to Amazon Web Services and spin up a PostgreSQL RDS Instance
  * [AWS PostgreSQL RDS](https://aws.amazon.com/rds/postgresql/) - Some good documentation on how to get started with AWS RDS.
4. Once the RDS Instance is up, grab the credentials from AWS and add them to lines 50-54 of the code.
5. To start the script, enter:
```R
python3 pandas_with_sqlalchemy.py
```
6. When the process is complete, your terminal window should show the following:
[![Screenshot_from_2017-10-26_09-50-07.png](https://s1.postimg.org/46s0bwttin/Screenshot_from_2017-10-26_09-50-07.png)](https://postimg.org/image/1bzc64emqz/)

## Coming Soon!
### More Algorithms to be added!
