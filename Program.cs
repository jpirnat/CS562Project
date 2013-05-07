using System;

using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Text.RegularExpressions;
using MySql.Data.MySqlClient;

/* database cs562
 * user cs562user
 * password cs562password 
 * 
 * create table sales (cust varchar(50), prod varchar(50), day int, month int, year int, state varchar(50), quant int);
 */

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
result["1.state"]=="NY"
result["2.state"]=="NJ"
result["3.state"]=="CT"
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
		
		/*
		 * this method transforms the where clause by adding type information in front of each item. ex: result["quant"] => (int)result["quant"]
		 */
		private static string transform_where_clause(string where_clause)
		{
			if(where_clause.Trim() == "")
				return "true"; // ensures all cases pass
			
			//(result["quant"]>2000) && (result["quant"]<3000) && (result["month"]>6)
			int pos = 0;
			while(pos != -1)
			{
				pos = where_clause.IndexOf("result[\"", pos);
				if(pos == -1)
					continue;
				int second_pos = where_clause.IndexOf("\"]", pos);
				string item = where_clause.Substring(pos+8, second_pos-(pos+8));
				string type = database_lookup_type(item, "");
				where_clause = where_clause.Insert(pos, "("+type+")");
				pos = second_pos+1+ ("("+type+")").Length; // pos should now be second_pos+1 + length of inserted text
				
				//Console.WriteLine("***where clause transform: "+item +" "+type);
			}
			return where_clause;
		}
		
		/*
		 * this method transforms the select clause by adding type information in front of each item. 
		 * ex: result["quant"] => (int)result["quant"]
		 * ex: structure["1_sum_quant"] => ((int)structure.sum_quant_1)
		 */
		private static string transform_select_clause(string select_clause_string)
		{
			if(select_clause_string.Trim() == "")
			{
				return "true";
			}
			
			int pos = 0;
			while(pos != -1)
			{
				pos = select_clause_string.IndexOf("result[\"", pos);
				if(pos == -1)
					continue;
				int second_pos = select_clause_string.IndexOf("\"]", pos);
				string item = select_clause_string.Substring(pos+8, second_pos-(pos+8));
				string type = database_lookup_type(item, "");
				select_clause_string = select_clause_string.Insert(pos, "("+type+")");
				pos = second_pos+1+ ("("+type+")").Length; // pos should now be second_pos+1 + length of inserted text
			}
			
			pos = 0;
			while(pos != -1)
			{
				pos = select_clause_string.IndexOf("structure[\"", pos);
				if(pos == -1)
					continue;
				int second_pos = select_clause_string.IndexOf("\"]", pos);
				string item = select_clause_string.Substring(pos+11, second_pos-(pos+11));
				
				// name transform item and grab correct type
				string type = type_lookup(item);
				string replacement = "(("+type+")structure."+name_transform(item)+")";
				
				select_clause_string = select_clause_string.Remove(pos, second_pos-pos+2);
				select_clause_string = select_clause_string.Insert(pos, replacement);
				pos = pos+1+replacement.Length;
			}
			
			return select_clause_string;
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
            select_cond = reader.ReadToEnd().Split(new string[] { "\n" }, StringSplitOptions.None);
			if(num_grouping_vars > select_cond.Length)
			{
				//Console.WriteLine("/*not enough select statement lines. need one for each grouping variable with blank lines allowed*/");
				//Console.WriteLine("/*numSelectCondExpr = "+select_cond.Length+"*/");
				//Console.WriteLine("/*numGroupingVars = "+num_grouping_vars+"*/");
				// TODO: exit here
			}
			else
			{
				for(int i = 0; i < select_cond.Length; i++)
				{
					select_cond[i] = transform_select_clause(select_cond[i]);
					//Console.WriteLine("/*transformed select clause #"+i+" = "+select_cond[i]+"*/");
				}
			}
			
			where_clause = transform_where_clause(where_clause);
			
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
					else
						structure_items.Add(name);
				}
				else
					structure_items.Add(name);
			}
			
			for(int i = 0; i < num_grouping_vars; i++)
			{
				// add existence booleans. 
				// if set to false then no items matched the grouping specification
				structure_items.Add("EXISTANCE_"+(i+1));
			}
			
            foreach (var x in structure_items)
            {
				if(x.StartsWith("EXISTANCE_"))
				{
					type_dictionary.Add(x, "bool");
					continue;
				}
				
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
				else if(type == "bool")
					class_builder.Append(" = false");
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
			main_class_builder.AppendLine(create_pretty_printing());
            main_class_builder.AppendLine("}");

            Console.WriteLine(main_class_builder.ToString());
		}
		
		/*
		 * TODO: create pretty printing method code here and then invoke this method in the main class
		 */
		private static string create_pretty_printing()
		{
			/*
			 * create int array to hold size needed by each column
			 * set default size of each to size of column_name (ex: 0_sum_count => 11)
			 * for each object in collections
			 * 	if not aggregation, update array with max size needed
			 * 	else if aggregation of 0_
			 * 		if avg then calculate avg if available (null otherwise)
			 * 		else update array with avg
			 * else if #_
			 * 	check existance boolean. if not true then all values are 0
			 * 	update array with value (avg => null if count == 0)
			 * 
			 * finally after gathering all size info, print everything out
			 */
			
			StringBuilder sb = new StringBuilder();
			// create array to hold all variables
			sb.AppendLine("// pretty printing code below");
			sb.AppendLine("private static int max(int a, int b) { if(a < b) return b; else return a; }");
			sb.AppendLine("private static string pretty_print(string s) { return s; }");
			sb.AppendLine("private static string pretty_print(int i) { return \"\"+i; }");
			sb.AppendLine("private static string pretty_print(double d) { return string.Format(\"{0:0.00}\", d); }");
			
			sb.AppendLine("private static void pretty_print() {");
			sb.AppendLine("\tint[] size_array = new int["+select_vars.Length+"];");
			for(int i = 0; i < select_vars.Length; i++)
			{
				// initialize array with length of names
				sb.AppendLine("\tsize_array["+i+"] = \""+select_vars[i]+"\".Length;");
			}
			sb.AppendLine("\tfor(int i = 0; i < collection.Count; i++) {");
			for(int i = 0; i < select_vars.Length; i++)
			{
				string variable = select_vars[i];
				if(!Regex.IsMatch(variable, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
				{
					string format_string = "\t\tsize_array[{0}] = max(size_array[{1}], pretty_print(collection[{2}].{3}).Length);";
					sb.AppendLine(string.Format(format_string, i, i, i, name_transform(variable)));
				}
				else
				{
					// handle aggregation cases here
					string[] comps = extract_aggregate_name_components(variable);
					if(comps[0] == "0" && comps[1] != "avg")
					{
						string format_string = "size_array[{0}] = max(size_array[{1}], pretty_print(collection[{2}].{3}).Length);";
						sb.AppendLine("\t\t"+string.Format(format_string, i, i, i, name_transform(variable)));
					}
					else if(comps[0] == "0" && comps[1] == "avg")
					{
						// 4 is length of null
						string count_str = comps[0]+"_count_"+comps[2];
						string sum_str = comps[0]+"_sum_"+comps[2];
						string format_str1 = "if (collection[{0}].{1} == 0) size_array[{2}] = max(size_array[{3}], 4);";
						sb.AppendLine("\t\t"+string.Format(format_str1, i, name_transform(count_str), i, i));
						string format_str2 = "else size_array[{0}] = max(size_array[{1}], pretty_print(collection[{2}].{3}*1.0/collection[{4}].{5}).Length);";
						sb.AppendLine("\t\t"+string.Format(format_str2, i, i, i, name_transform(sum_str), i, name_transform(count_str)));
					}
					else
					{
						sb.AppendLine("\t\tif (collection[i].existance_"+comps[0]+") {");
						if(comps[1] != "avg")
						{
							string format_string = "size_array[{0}] = max(size_array[{1}], pretty_print(collection[{2}].{3}).Length);";
							
							sb.AppendLine("\t\t\t"+string.Format(format_string, i, i, i, name_transform(variable)));
							
						}
						else
						{
							// 4 is length of null
							string count_str = comps[0]+"_count_"+comps[2];
							string sum_str = comps[0]+"_sum_"+comps[2];
							string format_str1 = "if (collection[{0}].{1} == 0) size_array[{2}] = max(size_array[{3}], 4);";
							sb.AppendLine("\t\t\t"+string.Format(format_str1, i, name_transform(count_str), i, i));
							string format_str2 = "else size_array[{0}] = max(size_array[{1}], pretty_print(collection[{2}].{3}*1.0/collection[{4}].{5}).Length);";
							sb.AppendLine("\t\t\t"+string.Format(format_str2, i, i, i, name_transform(sum_str), i, name_transform(count_str)));
						}
						sb.AppendLine("\t\t}");
						// 4 is the length of "NULL"
						sb.AppendLine("\t\t"+string.Format("else size_array[{0}] = max(size_array[{1}], 4);", i, i));
					}
				}
			}
			sb.AppendLine("\t}"); // for loop end
			
			// TODO: do actual printing and padding here
			for(int i = 0; i < select_vars.Length; i++)
			{
				string variable = select_vars[i];
				string format_string = "";
				if(Regex.IsMatch(variable, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
				{
					string[] comps = extract_aggregate_name_components(variable);
					if(database_lookup_type(comps[2]) != "string")
					{
						format_string = "Console.Write(\"{0}\".PadLeft(size_array[{1}]));";
					}
					else
					{
						format_string = "Console.Write(\"{0}\".PadRight(size_array[{1}]));";
					}
				}
				else
				{
					if(database_lookup_type(variable) != "string")
						format_string = "Console.Write(\"{0}\".PadLeft(size_array[{1}]));";
					else
						format_string = "Console.Write(\"{0}\".PadRight(size_array[{1}]));";
				}
				sb.AppendLine("\t"+string.Format(format_string, variable, i));
				sb.AppendLine("\tConsole.Write(\" \");");
			}
			sb.AppendLine("\tConsole.WriteLine(\"\");");
			
			sb.AppendLine("\tforeach(var structure in collection) {"); // todo: check existance_ bool here
			for(int i = 0; i < select_vars.Length; i++)
			{
				string variable = select_vars[i];
				string format_string = "";
				if(Regex.IsMatch(variable, @"^[0-9]+_(sum|min|max|avg|count)_.*"))
				{
					string[] comps = extract_aggregate_name_components(variable);
					if(database_lookup_type(comps[2]) != "string")
					{
						format_string = "Console.Write(pretty_print(structure.{0}).PadLeft(size_array[{1}]));";
					}
					else
					{
						format_string = "Console.Write(pretty_print(structure.{0}).PadRight(size_array[{1}]));";
					}
				}
				else
				{
					if(database_lookup_type(variable) != "string")
						format_string = "Console.Write(pretty_print(structure.{0}).PadLeft(size_array[{1}]));";
					else
						format_string = "Console.Write(pretty_print(structure.{0}).PadRight(size_array[{1}]));";
				}
				if(!Regex.IsMatch(variable, @"^[0-9]+_avg_.*"))
					sb.AppendLine("\t\t"+string.Format(format_string, name_transform(variable), i));
				else
				{
					string[] comps = extract_aggregate_name_components(variable);
					// TODO handle avg case here
					sb.AppendLine("\t\t"+string.Format(format_string, name_transform(variable), i));
				}
				sb.AppendLine("\t\tConsole.Write(\" \");");
			}
			sb.AppendLine("\t\tConsole.WriteLine(\"\");");
			sb.AppendLine("\t}");
			
			
			sb.AppendLine("}"); // method end
			return sb.ToString();
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
			continue;";
			
			string connection_end_code = @"
	}
	connection.Close();
}
catch (Exception e)
{
	Console.WriteLine(""could not open database connection: "");
	Console.WriteLine(e.Message);
}
";
			
			// do all future code inside of a copy of connection_start_code and connection_end_code
			
			// create initial collections set.
			main_method_builder.AppendLine(connection_start_code);
			StringBuilder create_initial_collection = new StringBuilder();
			create_initial_collection.Append("\t\tmf_struct structure = fetch_object_from_grouping_vars(");
			for(int i = 0; i < grouping_attrs.Length; i++)
			{
				string ga = grouping_attrs[i];
				create_initial_collection.Append("("+database_lookup_type(ga, "")+")");
				create_initial_collection.Append("result[\""+ga+"\"]");
				if(i != grouping_attrs.Length-1)
					create_initial_collection.Append(", ");
			}
			create_initial_collection.AppendLine(");");
			// handle structure not already added case
			create_initial_collection.AppendLine("\t\tif(structure == null) {");
			create_initial_collection.AppendLine("\t\t\tstructure = new mf_struct();");
			for(int i = 0; i < grouping_attrs.Length; i++)
			{
				string ga = grouping_attrs[i];
				create_initial_collection.AppendLine("\t\t\tstructure."+ga+" = ("+database_lookup_type(ga, "")+") result[\""+ga+"\"];");
			}
			
			foreach(KeyValuePair<int, string> p in min_dictionary)
			{
				if(p.Key == 0)
				{
					string attr = p.Value;
					string var_name = name_transform(""+p.Key+"_min_"+p.Value);
					create_initial_collection.AppendLine("\t\t\tstructure."+var_name+" = ("+database_lookup_type(attr, "")+") result[\""+attr+"\"];");
				}
			}

			foreach(KeyValuePair<int, string> p in max_dictionary)
			{
				if(p.Key == 0)
				{
					string attr = p.Value;
					string var_name = name_transform(""+p.Key+"_max_"+p.Value);
					create_initial_collection.AppendLine("\t\t\tstructure."+var_name+" = ("+database_lookup_type(attr, "")+") result[\""+attr+"\"];");
				}
			}
			
			create_initial_collection.AppendLine("\t\t\tcollection.Add(structure);");
			create_initial_collection.AppendLine("\t\t}");
			// search through count, min, max, and sum dictionaries and if for 0 and fits conditons, then update structure with the rows information
			// ex: if count contains <0, quant>, output structure.{name_transform(0_count_quant)}++
			// ex: if min contains <0, month>, 
			// output if structure.{name_transform(0_min_sum)} > result["month"] -> structure.{name_transform(0_min_sum)} = result["month"]
			foreach(String s in create_updater_code(0))
				create_initial_collection.AppendLine("\t\t"+s);
			
			// update fields
			main_method_builder.AppendLine(create_initial_collection.ToString());
			main_method_builder.AppendLine(connection_end_code);
			
			// finished with initial creation code
			
			// build up for loops for grouping variables
			for(int i = 0; i < num_grouping_vars; i++)
			{
				StringBuilder sb = new StringBuilder();
				
				sb.AppendLine(connection_start_code);
				
				sb.Append("\t\tmf_struct structure = fetch_object_from_grouping_vars(");
				for(int j = 0; j < grouping_attrs.Length; j++)
				{
					string ga = grouping_attrs[j];
					sb.Append("("+database_lookup_type(ga, "")+")");
					sb.Append("result[\""+ga+"\"]");
					if(j != grouping_attrs.Length-1)
						sb.Append(", ");
				}
				sb.AppendLine(");");
				
				sb.AppendLine(string.Format("\t\tif(!( {0} )) continue;", select_cond[i]));
				
				sb.AppendLine("\t\tif(!structure.existance_"+(i+1)+") {");
				sb.AppendLine("\t\t\tstructure.existance_"+(i+1)+" = true;");
				foreach(KeyValuePair<int, string> pair in min_dictionary)
				{
					if(pair.Key == i+1)
					{
						//sb.AppendLine("\t\t\t"+create_min_updater(pair.Key, pair.Value));
						string attr = pair.Value;
						string var_name = name_transform(""+pair.Key+"_min_"+pair.Value);
						create_initial_collection.AppendLine("\t\t\tstructure."+var_name+" = ("+database_lookup_type(attr, "")+") result[\""+attr+"\"];");
					}
				}
				
				foreach(KeyValuePair<int, string> pair in max_dictionary)
				{
					if(pair.Key == i+1)
					{
						//sb.AppendLine("\t\t\t"+create_max_updater(pair.Key, pair.Value));
						string attr = pair.Value;
						string var_name = name_transform(""+pair.Key+"_max_"+pair.Value);
						create_initial_collection.AppendLine("\t\t\tstructure."+var_name+" = ("+database_lookup_type(attr, "")+") result[\""+attr+"\"];");
					}
				}
				sb.AppendLine("\t\t}");
				
				foreach(String s in create_updater_code(i+1))
				{
					sb.AppendLine("\t\t"+s);
				}
				sb.AppendLine(connection_end_code);
				
				main_method_builder.AppendLine(sb.ToString());
			}
			
			//main_method_builder.AppendLine("Console.WriteLine(\"1 2 3\");");
            main_method_builder.AppendLine("}");

            return main_method_builder.ToString();
        }
		
		private static List<string> create_updater_code(int key)
		{
			List<string> code = new List<string>();
			foreach(KeyValuePair<int, string> pair in min_dictionary)
			{
				if(pair.Key == key)
					code.Add(create_min_updater(key, pair.Value));
			}
			
			foreach(KeyValuePair<int, string> pair in max_dictionary)
			{
				if(pair.Key == key)
					code.Add(create_max_updater(key, pair.Value));
			}
			
			foreach(KeyValuePair<int, string> pair in count_dictionary)
			{
				if(pair.Key == key)
					code.Add(create_count_updater(key, pair.Value));
			}
			
			foreach(KeyValuePair<int, string> pair in sum_dictionary)
			{
				if(pair.Key == key)
					code.Add(create_sum_updater(key, pair.Value));
			}
			
			return code;
		}
		
		private static string create_min_updater(int key, string attribute)
		{
			string attr_string = ""+key+"_min_"+attribute;
			attr_string = name_transform(attr_string);
			string type = database_lookup_type(attribute, "");
			string format_string = "if( (({0})structure.{1}) > (({2})result[\"{3}\"]) ) structure.{4} = (({5})result[\"{6}\"]);";
			string output = string.Format(format_string, type, attr_string, type, attribute, attr_string, type, attribute);
			return output;
		}
		
		
		private static string create_sum_updater(int key, string attribute)
		{
			string attr_string = ""+key+"_sum_"+attribute;
			attr_string = name_transform(attr_string);
			string type = database_lookup_type(attribute, "");
			string format_string = "structure.{0} += (({1})result[\"{2}\"]);";
			string output = string.Format(format_string, attr_string, type, attribute);
			return output;
		}
		
		private static string create_count_updater(int key, string attribute)
		{
			string attr_string = ""+key+"_count_"+attribute;
			attr_string = name_transform(attr_string);
			string format_string = "structure.{0}++;";
			string output = string.Format(format_string, attr_string);
			return output;
		}
		
		private static string create_max_updater(int key, string attribute)
		{
			string attr_string = ""+key+"_max_"+attribute;
			attr_string = name_transform(attr_string);
			string type = database_lookup_type(attribute, "");
			string format_string = "if( (({0})structure.{1}) < (({2})result[\"{3}\"]) ) structure.{4} = (({5})result[\"{6}\"]);";
			string output = string.Format(format_string, type, attr_string, type, attribute, attr_string, type, attribute);
			return output;
		}
	}		
}

