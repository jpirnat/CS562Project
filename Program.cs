﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

/* database cs562
 * user cs562user
 * password cs562password */

/*

TABLE:
sales
SELECT ATTRIBUTE(S):
cust, 1_sum_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
WHERE:
(result["quant"]>2000) && (result["quant"]<3000) && (result["month"]>6)
NUMBER OF GROUPING VARIABLES(n):
3
GROUPING ATTRIBUTES(V):
cust
F-VECT([F]):
1_sum_quant, 2_sum_quant, 3_sum_quant, 3_avg_quant
SELECT CONDITION-VECT([s]):
1.state='NY'
2.state='NJ'
3.state='CT'

*/
namespace CS_562_project
{
    class Program
    {

        private static MySqlConnection connection;

        const string is_num_regex = @"[1-9]+_.*";
        const string aggregation_match = @"(sum|min|max|avg|count)_.*";

        private static void initialize_database()
        {
            string server = "localhost";
            string database = "cs562";
            string uid = "cs562user";
            string password = "cs562password";
            string connectionString;
            connectionString = "SERVER=" + server + ";" + "DATABASE=" +
            database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";

            connection = new MySqlConnection(connectionString);
        }

        static void Main(string[] args)
        {

            initialize_database();

            StreamReader reader;
            if (args.Length == 1)
            {
                reader = new StreamReader(args[0]); 
            }
            else
            {
                reader = new StreamReader("test_input.txt");
            }

            const string table_line = "TABLE:";
            const string select_line = "SELECT ATTRIBUTE(S):";
            const string where_line = "WHERE:";
            const string num_grouping_vars_line = "NUMBER OF GROUPING VARIABLES(n):";
            const string grouping_attrs_line = "GROUPING ATTRIBUTES(V):";
            const string f_vect_line = "F-VECT([F]):";
            const string select_cond_line = "SELECT CONDITION-VECT([σ]):";

            reader.ReadLine(); // table_line
            string table_name = reader.ReadLine();

            reader.ReadLine(); // select_line
            string[] select_vars = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // where_line
            string where_clause = reader.ReadLine();
            
            reader.ReadLine(); // num_grouping_vars
            int num_grouping_vars = Convert.ToInt32(reader.ReadLine());
            
            reader.ReadLine(); // grouping_attrs_line
            string[] grouping_attrs = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // f_vect_line
            string[] f_vect = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // select_cond_line
            string[] select_cond = reader.ReadToEnd().Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);



            /* 
             * now having read in all relevant information, construct class for reading
             * 
             * for the class, need to store all info from select_vars, and f_vect
             * need to prevent duplicates and also do name transformation (ex: 1_sum_tax => sum_tax_1) (done)
             * also need to add additional information for avg (if avg, also add sum and count vars) (done)
             * then, after each name determined determine type. (still need to handle database interaction)
             *      count = int
             *      avg = double
             *      sum depends on type (either int or double)
             *      others will be same type as defined (need to do search through mysql: see readme for details)
             */

            var class_vars = new Dictionary<string, string>();
            foreach (var x in select_vars)
            {
                //Console.WriteLine("given "+x);

                var name = name_transform(x);  // transform name from 1_sum_tax => sum_tax_1
                if (class_vars.Keys.Contains(name))
                {
                    continue;
                }
                else
                {
                    string type = "string"; // default placeholder type of string
                    type = type_lookup(x);  // turn varchar(50) => string and Date => DateTime
                    //Console.WriteLine("adding to dictionary with values key=" + name + " value=" + type);
                    class_vars.Add(name, type);
                }
            }

            // handle avg_ case
            foreach (var x in select_vars)
            {
                var regex = @"[0-9]+_avg_.*";
                if (Regex.IsMatch(x, regex))
                {
                    var index1 = x.IndexOf('_');
                    var withoutFirst = x.Substring(index1 + 1);
                    var first = x.Substring(0, index1);
                    var index2 = withoutFirst.IndexOf('_');
                    var withoutSecond = withoutFirst.Substring(index2 + 1);
                    var second = withoutFirst.Substring(0, index2);

                    var sum_name = first + "_sum_" + withoutSecond;
                    var count_name = first + "_count_" + withoutSecond;
                    //Console.WriteLine("handling avg case: testing for "+sum_name+" and "+count_name);

                    if (!class_vars.ContainsKey(name_transform(sum_name)))
                    {
                        class_vars.Add(name_transform(sum_name), type_lookup(sum_name));
                    }

                    if (!class_vars.ContainsKey(name_transform(count_name)))
                    {
                        class_vars.Add(name_transform(count_name), type_lookup(count_name));
                    }
                }
            }

            var include_string =
            @"
			using System;
			using System.Collections.Generic;
			using System.Linq;
			using System.Text;
			using System.IO;
			using System.Text.RegularExpressions;
			using MySql.Data.MySqlClient;

			".Replace("\t", "");
            Console.WriteLine(include_string);

            var class_string_builder = new StringBuilder();
            class_string_builder.AppendLine("class mf_struct {");
            foreach (var pair in class_vars)
            {
                class_string_builder.Append("\tpublic ");
                class_string_builder.Append(pair.Value);
                class_string_builder.Append(" ");
                class_string_builder.Append(pair.Key);
                class_string_builder.AppendLine(";");
            }

            class_string_builder.AppendLine("}");

            Console.WriteLine(class_string_builder.ToString());

            var main_class_builder = new StringBuilder();
            main_class_builder.AppendLine("public class output {");
            main_class_builder.AppendLine("static List<mf_struct> collection = new List<mf_struct>();");
            main_class_builder.AppendLine(create_retrieve_method(grouping_attrs));
            main_class_builder.AppendLine(create_main_method());
            main_class_builder.AppendLine("}");

            Console.WriteLine(main_class_builder.ToString());

            string inner_querystuffs = @"
string query = ""SELECT * FROM "" + table_name + "" WHERE "" + where_clause;

try
{
	connection.Open();
	MySqlCommand cmd = new MySqlCommand(""use cs562"", connection);
	cmd.ExecuteNonQuery();
	cmd = new MySqlCommand(query, connection);
	var result = cmd.ExecuteReader();
	while (result.Read())
	{
		if (!(" + where_clause + @"))
        {
            continue;
        }
        
        // do all the stuff here
	}
	connection.Close();
}
catch (Exception e)
{
	Console.WriteLine(""could not open database connection: "");
	Console.WriteLine(e.Message);
	return ""string"";
}
";

            return;
        }

        /**
         * this creates a string containing the code for a static method that will search through 
         * the collection based on group attrs. if there is a pre-existing obj, it will be returned
         * otherwise, one will be created and initialized
         */
        private static string create_retrieve_method(string[] grouping_attrs)
        {
            var builder = new StringBuilder();
            builder.Append("private static mf_struct fetch_object_from_grouping_vars(");
            for (int i = 0; i < grouping_attrs.Length; i++)
            {
                if (i > 0)
                    builder.Append(", ");
                builder.Append(database_lookup_type(grouping_attrs[i]));
                builder.Append(" ");
                builder.Append(grouping_attrs[i]);
            }
            builder.AppendLine(") {");

            builder.AppendLine("	mf_struct to_return = null;");
            builder.AppendLine("	foreach (var obj in collection)");
            builder.AppendLine("	{");
            foreach (var attr in grouping_attrs)
            {
                builder.AppendLine("		if(!(obj." + attr + " == " + attr + ")) continue;");
            }
            builder.AppendLine("		to_return = obj;");
            builder.AppendLine("	}");
            builder.AppendLine("	if (to_return == null)");
            builder.AppendLine("	{");
            builder.AppendLine("		to_return = new mf_struct();");
            builder.AppendLine("		collection.Add(to_return);");

            /*
             * initialization code for mf_struct vars here
             */
            foreach (var attr in grouping_attrs)
            {
                builder.AppendLine("		to_return." + attr + " = " + attr + ";");
            }

            builder.AppendLine("	}");
            builder.AppendLine("	return to_return;");
            builder.AppendLine("}");
            return builder.ToString();
        }

        private static string create_main_method()
        {
            var main_method_builder = new StringBuilder();
            main_method_builder.AppendLine("static void Main(string[] args) {");
            main_method_builder.AppendLine("}");


            return main_method_builder.ToString();
        }

        /**
         * this class is supposed to transform 1_sum_tax => sum_tax_1
         */
        private static string name_transform(string name)
        {
            if (name.StartsWith("0_"))
            {
                return name.Substring(2);
            }

            Match match = Regex.Match(name, is_num_regex, RegexOptions.IgnoreCase);
            if (match.Success)
            {
                // do transformation to name
                var index = name.IndexOf('_');
                var num = name.Substring(0, index);
                name = name.Substring(index + 1) + "_" + num;
            }

            return name;
        }

        /**
         * this class is supposed to transform 1_sum_tax => int
         */
        private static string type_lookup(string name)
        {
            string aggregation = "";
            if (name.StartsWith("0_"))
            {
                name = name.Substring(2);
            }

            Match num_regex_name = Regex.Match(name, is_num_regex, RegexOptions.IgnoreCase);
            if (num_regex_name.Success) // is in form of #_something
            {
                // do transformation to name
                var num_index = name.IndexOf('_');
                //var num1 = name.Substring(0, num_index);
                name = name.Substring(num_index + 1); // remove #_ from name
                //Console.WriteLine("num match");

                Match aggregation_match_name = Regex.Match(name, aggregation_match, RegexOptions.IgnoreCase);
                if (aggregation_match_name.Success)
                {
                    //Console.WriteLine("aggregation match");
                    // remaining part is in form of sum_something or min_something or ...
                    var aggr_index = name.IndexOf('_');
                    aggregation = name.Substring(0, aggr_index);
                    name = name.Substring(aggr_index + 1); // remove aggregation_ from name
                }
            }

            return database_lookup_type(name, aggregation);
        }

        /**
         * this method is supposed to access the database and use the mysql information schema table.
         * it will use the table to determine the corresponding sql type of column whose name is stored 
         * in the variable name
         */
        private static string database_lookup_type(string name, string aggregation_type = "")
        {
            string type = "string"; // default to string type

            //Console.WriteLine("aggregation_type = " + aggregation_type);

            if (aggregation_type.Equals("count", StringComparison.CurrentCultureIgnoreCase))
            {
                return "int"; // count should be type int
            }
            else if (aggregation_type.Equals("avg", StringComparison.CurrentCultureIgnoreCase))
            {
                return "double"; // avg should be of type double
            }

            // search for name in mysql information schema table here
            // after retrieving result, convert it to the corresponding C# type
            string query = "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_DEFAULT FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE table_name = 'sales' and column_name = '" + name + "'";

            try
            {
                connection.Open();
                MySqlCommand cmd = new MySqlCommand("use cs562", connection);
                cmd.ExecuteNonQuery();
                cmd = new MySqlCommand(query, connection);
                var result = cmd.ExecuteReader();
                while (result.Read())
                {
                    type = (string)result["DATA_TYPE"] + "";
                }
                connection.Close();
            }
            catch (Exception e)
            {
                Console.WriteLine("could not open database connection: ");
                Console.WriteLine(e.Message);
                return "string";
            }
            //Console.WriteLine("database returned "+type+" for "+name);

            if (type.StartsWith("varchar", StringComparison.CurrentCultureIgnoreCase))
                type = "string";
            else if (type.StartsWith("int", StringComparison.CurrentCultureIgnoreCase))
                type = "int";
            else if (type.StartsWith("float", StringComparison.CurrentCultureIgnoreCase))
                type = "double";
            else if (type.StartsWith("double", StringComparison.CurrentCultureIgnoreCase))
                type = "double";
            else
                type = "string";

            return type;
        }
    }
}
