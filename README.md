# OLAP

Input data form: sector, ticker, date, open, high, low, close, volume, OpenInt.
For example: Technology,nvda,7/17/07,29,29.588,28.736,28.762,17326327,0

Aggregate Functions: top k, min, max, mean, sum, count.
Aggregate Functions do not support date
Example: --min open, --max close, --count

Group-by

command line: python OLAP.py --input input.csv Aggregate Functions --group-by filed name 
Example: python OLAP.py --input input.csv --group-by sector --min open --max open 
