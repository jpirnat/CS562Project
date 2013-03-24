using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

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
	        string database = "test_database";
	        string uid = "root";
	        string password = "password";
	        string connectionString;
	        connectionString = "SERVER=" + server + ";" + "DATABASE=" + 
			database + ";" + "UID=" + uid + ";" + "PASSWORD=" + password + ";";
	
	        connection = new MySqlConnection(connectionString);
		}

        static void Main(string[] args)
        {
            if (args.Length != 1)
            {
                Console.WriteLine("usage: takes 1 argument which is the input file");
                return;
            }
			
			initialize_database();
            
            var reader = new StreamReader(args[0]);
            
            const string select_line = "SELECT ATTRIBUTE(S):";
            const string num_grouping_vars_line = "NUMBER OF GROUPING VARIABLES(n):";
            const string grouping_attrs_line = "GROUPING ATTRIBUTES(V):";
            const string f_vect_line = "F-VECT([F]):";
            const string select_cond_line = "SELECT CONDITION-VECT([σ]):";

            reader.ReadLine(); // select_line
            var select_vars = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            reader.ReadLine(); // num_grouping_vars
            var num_grouping_vars = Convert.ToInt32(reader.ReadLine());
            reader.ReadLine(); // grouping_attrs_line
            var grouping_attrs = reader.ReadLine();
            reader.ReadLine(); // f_vect_line
            var f_vect = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            reader.ReadLine(); // select_cond_line
            var select_cond = reader.ReadToEnd().Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);

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

            var class_string_builder = new StringBuilder();
            class_string_builder.AppendLine("class mf_struct {");
            foreach (var pair in class_vars)
            {
                class_string_builder.Append("\t");
                class_string_builder.Append(pair.Value);
                class_string_builder.Append(" ");
                class_string_builder.Append(pair.Key);
                class_string_builder.AppendLine(";");
            }

            class_string_builder.AppendLine("}");

            Console.WriteLine(class_string_builder.ToString());
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
        private static string database_lookup_type(string name, string aggregation_type="")
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
				"WHERE table_name = 'sales' and column_name = '"+name+"'";
			
			try {
				connection.Open();
				MySqlCommand cmd = new MySqlCommand("use test_database", connection);
				cmd.ExecuteNonQuery();
				cmd = new MySqlCommand(query, connection);
				var result = cmd.ExecuteReader();
				while(result.Read())
				{
					type = (string) result["DATA_TYPE"]+"";
				}
				connection.Close();
			}catch(Exception e) {
				Console.WriteLine("could not open database connection: ");
				Console.WriteLine(e.Message);
				return "string";
			}
			//Console.WriteLine("database returned "+type+" for "+name);
			
			if(type.StartsWith("varchar", StringComparison.CurrentCultureIgnoreCase))
				type = "string";
			else if(type.StartsWith("int", StringComparison.CurrentCultureIgnoreCase))
				type = "int";
			else if(type.StartsWith("float", StringComparison.CurrentCultureIgnoreCase))
				type = "double";
			else if(type.StartsWith("double", StringComparison.CurrentCultureIgnoreCase))
				type = "double";
			else
				type = "string";

            return type;
        }
    }
}

