OLAP(Online Analytical Processing)
The program implements OLAP queries.

Input data form(csv file): sector, ticker, date, open, high, low, close, volume, OpenInt
For example: Technology,nvda,7/17/07,29,29.588,28.736,28.762,17326327,0

Aggregate Functions: --min <numeric-field-name>, --max <numeric-field-name>, --mean <numeric-field-name>, --sum <numeric-field-name>, and --count <numeric-field-name> 
Aggregate Functions do not support date
Example: --min open, --max close, --count

For categorical columns: group-by and top-k
top-k: 
--top <k> <categorical field name>
computes top k most common values of categorical-field-name

group-by:
--group-by <categorical-field-name>
computes the requested aggregates independently for each distinct value in <categorical-field-name>   

command line: python OLAP.py --input input.csv --group-by fieldname --Aggregate Functions 
Example: python OLAP.py --input input.csv --group-by sector --min open --max open 

