using System;

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
	public class Program2
	{
		private static MySqlConnection connection;

        const string is_num_regex = @"[1-9]+_.*";
        const string aggregation_match = @"(sum|min|max|avg|count)_.*";
		
		/**
         * this class is supposed to transform 1_sum_tax => sum_tax_1
         */
        private static string name_transform(string name)
        {
			name = name.ToLower().Trim();
			if(Regex.IsMatch(name, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
			{
				var pos = name.IndexOf('_');
				string start = name.Substring(0, pos);
				string end = name.Substring(pos+1);
				if(start == "*")
					start = "STAR";
				return end+"_"+start;
			}
			else 
				return name;
        }
		
		/**
         * this class is supposed to transform 1_sum_tax => int
         */
        private static string type_lookup(string name)
        {
			name = name.ToLower().Trim();
			if(Regex.IsMatch(name, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
			{
				string[] array = extract_aggregate_name_components(name);
				return database_lookup_type(array[2], array[1]);
			}
			else
			{
				return database_lookup_type(name, "");
			}
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
		
		private static string mysql_connectionString = "";
        private static void initialize_database()
        {
            string mysql_server = "localhost";
            string mysql_database = "cs562";
            string mysql_uid = "cs562user";
            string mysql_password = "cs562password";
            
            mysql_connectionString = "SERVER=" + mysql_server + ";" + "DATABASE=" +
            mysql_database + ";" + "UID=" + mysql_uid + ";" + "PASSWORD=" + mysql_password + ";";

            connection = new MySqlConnection(mysql_connectionString);
        }
		
		static List<string> structure_items = new List<string>();
		static Dictionary<string, string> type_dictionary = new Dictionary<string, string>(); // maps 1_sum_quant -> int
		static Dictionary<int, string> count_dictionary = new Dictionary<int, string>(); // this dictionary holds all count aggregates needed to be calculated
		static Dictionary<int, string> min_dictionary = new Dictionary<int, string>(); // this dictionary holds all min aggregates needed to be calculated
		static Dictionary<int, string> max_dictionary = new Dictionary<int, string>(); // this dictionary holds all max aggregates needed to be calculated
		static Dictionary<int, string> avg_dictionary = new Dictionary<int, string>(); // this dictionary holds all avg aggregates needed to be calculated
		static Dictionary<int, string> sum_dictionary = new Dictionary<int, string>(); // this dictionary holds all sum aggregates needed to be calculated
		static string table_name = "sales"; // will be overridden in main
		static string[] select_vars;
		static string where_clause;
		static int num_grouping_vars;
		static string[] grouping_attrs;
		static string[] f_vect;
		static string[] select_cond;

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
			
			// Parse Input file
            const string table_line = "TABLE:";
            const string select_line = "SELECT ATTRIBUTE(S):";
            const string where_line = "WHERE:";
            const string num_grouping_vars_line = "NUMBER OF GROUPING VARIABLES(n):";
            const string grouping_attrs_line = "GROUPING ATTRIBUTES(V):";
            const string f_vect_line = "F-VECT([F]):";
            const string select_cond_line = "SELECT CONDITION-VECT([σ]):";

            reader.ReadLine(); // table_line
            table_name = reader.ReadLine();

            reader.ReadLine(); // select_line
            select_vars = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // where_line
            where_clause = reader.ReadLine();
            
            reader.ReadLine(); // num_grouping_vars
            num_grouping_vars = Convert.ToInt32(reader.ReadLine());
            
            reader.ReadLine(); // grouping_attrs_line
            grouping_attrs = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // f_vect_line
            f_vect = reader.ReadLine().Split(new string[] { ", " }, StringSplitOptions.RemoveEmptyEntries);
            
            reader.ReadLine(); // select_cond_line
            select_cond = reader.ReadToEnd().Split(new string[] { "\n" }, StringSplitOptions.RemoveEmptyEntries);
			
			// TODO: transform where_clause and select_cond according to name transformations.
			// TODO: in select_cond, transform results depending on whether or not item is from query or inside mf_structure
			
			// Finished paring input file
			
			// gather structure information
			foreach(string x in select_vars)
			{
				string name = x.ToLower().Trim();
				if(structure_items.Contains(name))
					continue;
				
				if(Regex.IsMatch(name, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
				{
					string[] comps = extract_aggregate_name_components(name);
					if(comps[1] == "avg")
					{
						string count_str = comps[0]+"_count_"+comps[2];
						string sum_str = comps[0]+"_sum_"+comps[2];
						
						if(!structure_items.Contains(count_str))
						   structure_items.Add(count_str);
						if(!structure_items.Contains(sum_str))
						   structure_items.Add(sum_str);
					}
				}
				
				structure_items.Add(name);
			}
			
            foreach (var x in structure_items)
            {
                string type = "string"; // default placeholder type of string
                type = type_lookup(x);  // turn varchar(50) => string and Date => DateTime
                type_dictionary.Add(x, type);
				
				// add to other dictionaries here
				if(Regex.IsMatch(x, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
				{
					string[] comps = extract_aggregate_name_components(x);
					if(comps[1] == "avg")
						avg_dictionary.Add(int.Parse(comps[0]), comps[2]);
					else if(comps[1] == "count")
						count_dictionary.Add(int.Parse(comps[0]), comps[2]);
					else if(comps[1] == "min")
						min_dictionary.Add(int.Parse(comps[0]), comps[2]);
					else if(comps[1] == "max")
						max_dictionary.Add(int.Parse(comps[0]), comps[2]);
					else if(comps[1] == "sum")
						sum_dictionary.Add(int.Parse(comps[0]), comps[2]);
				}
            }
			// finish gathering structure information for each item
			
			// start code generation
			
			// generate includes
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
			// finish generating includes
			
			// generate class structure
			StringBuilder class_builder = new StringBuilder();
			class_builder.AppendLine("class mf_struct {");
			foreach(string x in structure_items)
			{
				string type = type_dictionary[x];
				class_builder.Append("\tpublic "+type+" ");
				class_builder.Append(name_transform(x));
				if(type == "int" || type == "float" || type == "double")
					class_builder.Append(" = 0");
				else
					class_builder.Append(" = \"\"");
				class_builder.AppendLine(";");
			}
			class_builder.AppendLine("}");
			Console.WriteLine(class_builder.ToString());
			// finish generating class structure
			
			var main_class_builder = new StringBuilder();
            main_class_builder.AppendLine("public class output {");
            main_class_builder.AppendLine("static List<mf_struct> collection = new List<mf_struct>();");
			main_class_builder.AppendLine("private static MySqlConnection connection = new MySqlConnection(\""+mysql_connectionString+"\");");
            main_class_builder.AppendLine(create_retrieve_method(grouping_attrs));
            main_class_builder.AppendLine(create_main_method());
            main_class_builder.AppendLine("}");

            Console.WriteLine(main_class_builder.ToString());
		}
		
		/**
         * this creates a string containing the code for a static method that will search through 
         * the collection based on group attrs. if there is a pre-existing obj, it will be returned
         * otherwise, the method will return null
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
            builder.AppendLine("	return to_return;");
            builder.AppendLine("}");
            return builder.ToString();
        }
		
		/*
		 * this function turns 1_count_quant => {1, count, quant}
		 */
		static string[] extract_aggregate_name_components(string x)
		{
			return x.Split(new char[] {'_'});
		}
		
		private static string create_main_method()
        {
            var main_method_builder = new StringBuilder();
            main_method_builder.AppendLine("static void Main(string[] args) {");
			main_method_builder.AppendLine(@"string query = ""Select * from "+table_name+" \";");
			string connection_start_code = @"
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
        }";        
			string connection_end_code = @"
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
			
			// do all future code inside of a copy of connection_start_code and connection_end_code
			main_method_builder.AppendLine("Console.WriteLine(\"1 2 3\");");
            main_method_builder.AppendLine("}");

            return main_method_builder.ToString();
        }

	}		
}

