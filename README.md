CS562Project
============

The CS562 project

This is a repository where we will work on our CS562 project!!!

read input file for relevant fields
information schema querying
get code repository


fetch datatypes for a column in a table
http://dev.mysql.com/doc/refman/5.0/en/columns-table.html

using Connector/Net library for database calls
http://zetcode.com/db/mysqlcsharptutorial/

another tutorial explaining individual methods to use for database queries
http://www.codeproject.com/Articles/43438/Connect-C-to-MySQL


Left for us to do:
handle case of #_count_* (essentially just replace * with word STAR)

fix where clause comparisions concerning unspecified type. search where clause string for all instances of result[ and then find matching ].  replace inner text with name transformed equivalent. then change it into the form of ((type) result[""] ) to ensure proper handling of different object types


for select clause, it would probably be easier to write the input just like we did the where clause with result[""]

there are 2 types of statements to watch out for in the select clause

1.state = ""
1.sales < 2_avg_sales.

One condition relies on data from the database query. the other relies on precalculated values.

We could write both like result["1_state"] and result["2_avg_sales"]. Then we doing the previous transformation like we did in the where clause we could just check to see if the desired value is in the mf_struct. if it is, then change result["x"] to current_object.x

Alternatively, we could have a distinction between . and the _
the input would instead be result["1.state"] and result["2_avg_sales"]. if the result value starts with #. then we know it relies on the current database transaction. if its an underscore instead, then we would replace result["2_avg_sales"] with current_object.avg_sales_2

Both solutions should work without too much effort