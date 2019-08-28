import csv 
import CSVCat as CSVCatalog
import json

max_sample_rows=10

class CSVTable:
    # Table engine needs to load table definition information.
    __catalog__ = CSVCatalog.CSVCatalog()

    def __init__(self, t_name, load=True):
        """
        Constructor.
        :param t_name: Name for table.
        :param load: Load data from a CSV file. If load=False, this is a derived table and engine will
            add rows instead of loading from file.
        """

        self.__t_name__ = t_name
        self.__description__ = None
        if load:
            self.__load_info__()  # Load metadata
            self.__rows__ = None
            self.__load__()  # Load rows from the CSV file.
            self.__build_indexes__()
        else:
            self.__file_name__ = "DERIVED"

    def __get_file_name__(self):
        return self.__description__["definition"]["path"]

    def __add_row__(self,new_r):
        if self.__rows__ is None:
            self.__rows__ = []
        self.__rows__.append(new_r)

    def __load_info__(self):
        result=self.__catalog__.get_table(self.__t_name__).describe_table()
        self.__description__=result

    def __load__(self):

        try:
            fn = self.__get_file_name__()
            with open(fn, "r") as csvfile:
                # CSV files can be pretty complex. You can tell from all of the options on the various readers.
                # The two params here indicate that "," separates columns and anything in between " ... " should parse
                # as a single string, even if it has things like "," in it.
                reader = csv.DictReader(csvfile, delimiter=",", quotechar='"')
                column_names = self.__get_column_names__()
                for r in reader:
                    projected_r = self.project([r], column_names)[0]
                    self.__add_row__(projected_r)

        except IOError as e:
            raise Exception ("Could not read file")

    def __get_row_count__(self):
        if self.__rows__ is not None:
            return len(self.__rows__)
        else:
            return 0

    def __format_odict(self, l):
        result=""
        temp=list(l)
        for i in range(0, len(temp)):
        	result+="{:<15}".format(temp[i])
        return result

    def __str__(self):
        """
        You can do something simple here. The details of the string returned depend on what properties you
        define on the class. So, I cannot provide a simple implementation.
        :return:
        """
        if self.__description__ is not None:
            result = "Table Name: " + self.__t_name__ + " File Path: " + self.__get_file_name__() + "\n"
            result += "Count of rows: " + str(self.__get_row_count__()) + "\n"
            json_desc = json.dumps(self.__description__, indent = 2)
            result += json.dumps(json_desc,indent=2)

            result += "\n Index info: "
            idxes = self.__description__["indexes"]
            if idxes is not None:
                for (k,v) in idxes.items():
                    result += "\n"
                    elements = len(v.keys())
                    result += "Index Name: " +k + ", Columns: " + ",".join(v["columns"])
                    idx_entry = self.__indexes__[k]
                    result += ", No. of entries: " + str(len(idx_entry.keys()))
            else:
                result += "No indexes"
        else:
            result = "Table Name: " + self.__t_name__ + " File Path: " + self.__file_name__ + "\n"
            result += "Count of rows: " + str(self.__get_row_count__()) + "\n"

        l = self.__get_row_count__()

        result += "\n\nSample rows:"

        if l > 0:
            n = min(l,max_sample_rows)
            if n < max_sample_rows:
                first_n = n
                second_n = 0
            else:
                first_n = int(max_sample_rows / 2)
                second_n = l - first_n

            if first_n==0:
                first_n=1
            result+"\n"
            cn=self.__get_column_names__()
            result+=self.__format_odict(cn)
            result+="\n"
            the_rows=self.get_row_list()
            for i in range(0, first_n):
                temp_r=the_rows[i]
                result+=self.__format_odict(temp_r.values())+"\n"
            if second_n >0:
                middle_row=["..."]* len(self.__get_column_names__())
                result+=self.__format_odict(middle_row)+"\n"
                for i in range(l-1, second_n -1, -1):
                    temp_r=the_rows[i]
                    result+=self.__format_odict(temp_r.values())+"\n"
            else:
                result+="No rows"

        return result

    def __get_sub_where_template__(self,t):
        if t is None:
        	return None
        table_columns=self.__get_column_names__()
        result={}
        for n in table_columns:
        	v= t.get(n, None)
        	if v is not None:
        		result[n]=v
        	if len(result)==0:
        		result=None
        return result

    def __get_on_template__(self,t,fields):
        result={}
        for i in fields:
            if t[i] is not None:
                result[i]=t[i]
        return result

    def __build_index__(self, index_name,index_columns):
        new_index = {}
        for r in self.__rows__:
            idx_value = self.__get_index_value__(r, index_name)
            idx_entry = new_index.get(idx_value, [])
            idx_entry.append(r)
            new_index[idx_value] = idx_entry

        return new_index

    def __build_indexes__(self):
        self.__indexes__ = {}
        defined_indexes = self.__description__["indexes"]
        for (k,v) in defined_indexes.items():
            new_idx = self.__build_index__(k, v["columns"])
            self.__indexes__[k] = new_idx


    def __get_access_path__(self, tmp):
    	if self.__indexes__ is None:
    		return None, None
    	else:
    		result=None
    		count=None
    	if tmp is None:
    		return None

    	tmp_set=set(tmp)
    	idx_list=self.__description__.get('indexes', None)
    	for (k, the_idx)in idx_list.items():
    		columns=set(the_idx['columns'])
    		if columns.issubset(tmp_set):
    			if result is None:
    				result=k
    				count=len(self.__indexes__[result])
    			else:
    				if count < len(self.__indexes__['index_name']):
    					result=k
    					count=len(self.__indexes__[result])
    	return result, count

    def matches_template(self, row, t):
        """
        :param row: A single dictionary representing a row in the table.
        :param t: A template
        :return: True if the row matches the template.
        """
        if t is None:
            return True

        try:
            c_names = list(t.keys())
            for n in c_names:
                if row[n] != t[n]:
                    return False
            else:
                return True
        except Exception as e:
            raise (e)

    def project(self, rows, fields):
        """
        Perform the project. Returns a new table with only the requested columns.
        :param fields: A list of column names.
        :return: A new table derived from this table by PROJECT on the specified column names.
        """
        try:
            if fields is None: 
                return rows  
            else:
                result = []
                for r in rows:  
                    tmp = {}  
                    for j in range(0, len(fields)):  
                        v = r[fields[j]]
                        tmp[fields[j]] = v
                    else:
                        result.append(tmp)  
                return result

        except KeyError as ke:
            raise Exception ("Invalid field in project ")

    def __find_by_template_index__(self, t, idx, fields=None, limit=None, offset=None):
        idx_name=idx
        idx_info=self.__get_index_info__(idx_name)
        idx_columns=idx_info['columns']
        key_values=self.__get_index_value__(t, idx_name)
        the_index=self.__indexes__[idx_name]
        tmp_result=the_index.get(key_values, None)

        if tmp_result:
            result=[]
            for r in tmp_result:
                if self.matches_template(r, t):
                    result.append(r)
            result = self.project(result, fields)
        else:
            result=None
        return result

    def __primary_keys_valid__(self):
        keys = set(self.key_columns)
        cols = set(self.headers)

        if not keys.issubset(cols):
            return False
        else:
            return True

    def __find_by_template_scan__(self, t, fields=None, limit=None, offset=None):
        """
        Returns a new, derived table containing rows that match the template and the requested fields if any.
        Returns all row if template is None and all columns if fields is None.
        :param t: The template representing a select predicate.
        :param fields: The list of fields (project fields)
        :param limit: Max to return. Not implemented
        :param offset: Offset into the result. Not implemented.
        :return: New table containing the result of the select and project.
        """

        if limit is not None or offset is not None:
            raise Exception ("Invalid field in project ")

        if self.__rows__ is not None:
            result = []
            for r in self.__rows__:
                if self.matches_template(r, t):
                    result.append(r)

            result = self.project(result, fields)
        else:
            result = None

        return result

    def __get_key__(self,r):
        if self.key_columns is None:
            return None

        result = {}

        try:
            for k in self.key_columns:
                result[k] = r[k]
            return result
        except Exception as e:
            raise (e)
    
    def find_by_template(self, t, fields=None, limit=None, offset=None):
        # 1. Validate the template values relative to the defined columns.
        # 2. Determine if there is an applicable index, and call __find_by_template_index__ if one exists.
        # 3. Call __find_by_template_scan__ if not applicable index.
        if t is not None:
            access_index, count = self.__get_access_path__(list(t.keys()))
        else:
            access_index = None

        if access_index is None:
            return self.__find_by_template_scan__(t,fields=fields,limit=None,offset=None)
        else:
            result = self.__find_by_template_index__(t,access_index,fields,limit,offset)
        return result

    def find_by_primary_key(self, key_values, values=None):
        template = dict(zip(self.key_columns, key_values))
        result = self.find_by_template(template,values)
        if result.rows is not None:
            return result.rows[0]
        else:
            return None

    def insert(self, r):
        raise Exception ("Invalid field in project ")

    def delete(self, t):
        raise Exception ("Invalid field in project ")

    def update(self, t, change_values):
        raise Exception ("Invalid field in project ")

    def __execute_nested_loop_join__(self, right_r, on_fields, where_template, project_fields):
        left_r=self
        left_rows=left_r.get_row_list()
        right_rows=right_r.get_row_list()
        result_rows=[]

        left_rows_processed=0
        for lr in left_rows:
            on_template=self.get_on_template(lr,on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r={**lr, **rr}
                    result_rows.append(new_r)
            left_rows_processed+=1
            if left_rows_processed%10==0:
                print("Processed ", left_rows_processed, " left rows ...")

    def __get_index_info__(self, index_name):
        result=self.__description__["indexes"].get(index_name)
        return result

    def __get_column_names__(self):
        if self.__description__ is not None:
            cn=self.__description__["columns"]
            result=[k["col_name"] for k in cn]
        else:
            result=(self.__rows__[0].keys())
        return result

    def __get_index_definition__(self, index_name):
        if self.__description__["indexes"] is not None:
            idx_def=self.__description__["indexes"].values()
            for d in idx_def:
                if d["index_name"]==index_name:
                    return d

    def __get_index_value__(self, row, index_name):
        idx_elements=[]
        idx_info=self.__get_index_info__(index_name)
        columns=idx_info["columns"]
        for c in columns:
            idx_elements.append(row[c])
            result="_".join(idx_elements)
        return result

    def __create_index__(self, columns):
        l=self._col_list_valid(columns)
        if l!=True:
            raise Exception ("Invalid field in project ")

        if self.indexes is None:
            self.indexes={}
        index_name="_".join(columns)
        idx=self.indexes.get(index_name,None)
        if idx is not None:
            raise Exception ("Invalid field in project ")

        self.indexes[index_name]={}
        index=self.indexes[index_name]
        for (k,r) in self.rows.items():
            key=self.__get_index_values(r, index_name)
            bucket= index.get(key, None)
            if bucket is None:
                bucket={}
                index[key]=bucket
        bucket[k]=r

    def __join_rows__(self, l, r, on_fields):
        result_rows=[]
        for lr in l :
            on_template=self.__get_on_template__(lr, on_fields)
            for rr in r:
                if self.matches_template(rr, on_template):
                    new_r={**lr, **rr}
                    result_rows.append(new_r)
        return result_rows

    def get_row_list(self):
        if self.__rows__ is not None:
            result=self.__rows__
        else:
            result=None
        return result

    def __choose_scan_probe_table__(self, right_r, on_fields):
        left_path, left_count = self.__get_access_path__(on_fields)
        right_path, right_count = right_r.__get_access_path__(on_fields)

        if left_path is None and right_path is None:
            return self, right_r
        elif left_path is None and right_path is not None:
            return self, right_r
        elif left_path is not None and right_path is None:
            return right_r, self
        elif right_count<left_count:
            return self, right_r
        else:
            return right_r, self

    def join(self,right_r,on_fields,where_template=None, project_fields =None, optimize = True):
        if not optimize:
            return join_no_optimize(right_r,on_fields,where_template,project_fields)
        scan_t, probe_t = self.__choose_scan_probe_table__(right_r, on_fields)
        scan_sub_template = scan_t.__get_sub_where_template__(where_template)
        probe_sub_template = probe_t.__get_sub_where_template__(where_template)

        if optimize:
            scan_rows = scan_t.find_by_template(scan_sub_template)
        else:
            scan_rows = scan_t.get_row_list()

        join_result = []

        for l_r in scan_rows:
            on_template = scan_t.__get_on_template__(l_r, on_fields)
            if probe_sub_template is not None:
                probe_where = {**on_template, **probe_sub_template}
            else:
                probe_where = on_template

            current_right_rows = probe_t.find_by_template(probe_where)

            if current_right_rows is not None and len(current_right_rows) > 0:
                new_rows = self.__join_rows__([l_r], current_right_rows, on_fields)
                join_result.extend(new_rows)

        final_rows = []
        for r in join_result:
            if self.matches_template(r,where_template):
                r = self.project([r],fields = project_fields)
                final_rows.append(r[0])

        join_result = self.__table_from_rows__("JOIN(" + self.__t_name__ + "," + right_r.__t_name__ + ")",None,final_rows)
        return join_result

    def __table_from_rows__(self,name,keys,rows):
        result = CSVTable(name,load=False)
        if rows is None:
            return result
        new_rows = []
        for i in range(0, len(rows)):
            new_rows.append(rows[i])
        result.__rows__ = new_rows
        return result


    def join_no_optimize(self, right_r, on_fields, where_template=None, project_fields=None):
        """
        Implements a JOIN on two CSV Tables. Support equi-join only on a list of common
        columns names.
        :param left_r: The left table, or first input table
        :param right_r: The right table, or second input table.
        :param on_fields: A list of common fields used for the equi-join.
        :param where_template: Select template to apply to the result to determine what to return.
        :param project_fields: List of fields to return from the result.
        :return: List of dictionary elements, each representing a row.
        """
        # If not optimizations are possible, do a simple nested loop join and then apply where_clause and
        # project clause to result.
        #
        # At least two vastly different optimizations are be possible. You should figure out two different optimizations
        # and implement them.
        #
        left_r = self
        selected_l = self.find_by_template(where_template)
        selected_l = self.table_from_rows("LEFTSELECTED", None, selected_l)
        left_rows = selected_l.get_row_list()
        right_rows = right_r.get_row_list()
        result_rows = []

        left_rows_processed = 0
        for lr in left_rows:
            on_template = self.get_on_template(lr, on_fields)
            for rr in right_rows:
                if self.matches_template(rr, on_template):
                    new_r = {**lr,**rr}
                    result_rows.append(new_r)
            left_rows_processed += 1
            if left_rows_processed % 10 == 0:
                print ("Processed", left_rows_processed, " left rows ...")

        join_result = self.table_from_rows("JOIN:" + left_r.__t_name__ + ":" + right_r.__t_name__, None, result_rows)
        result = join.result.find_by_template(t = where_template, fields = project_fields)
        return result



