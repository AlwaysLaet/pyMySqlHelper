from getpass import getpass
import json
import pymysql
import pandas as pd

def _get_creds(user = None, 
               password = None, 
               host = None, 
               database = None, 
               check_creds = True,
               **kwargs):
    """**function _get_creds**: 
    Via prompts or passed parameters, get user credentials for MySQL database connection.
    
    """
    creds = kwargs.copy()
    user = creds.get('user', user)
    password = creds.get('password', password)
    host = creds.get('host', host)
    database = creds.get('database', database)
    if not user: user = input("Please input the mysql user: ")
    if not password: password = getpass(f"Please input the password for user '{user}': " )
    if not host: host = getpass("Please input the host address for the mysql server: ")
    if not database: database = input("Please input the name of the desired database: ")
    
    if check_creds:
        ret = input("\n".join(["Would you like to check your inputs?",
                               "Danger! This will expose your password and host.",
                               "(y/[n]): "]))
        if str(ret).lower() in ['y','yes']:
            param_str = "\n".join(["",
                                   "You have input the following parameters:",
                                   f"user: '{user}'",
                                   f"password: '{password}'",
                                   f"host: '{host}'",
                                   f"database: '{database}'",
                                   ""])
            print(param_str)
            correct = input("Is this correct? ([y]/n): ")
            if str(correct).lower() in ['n','no']:
                return _get_creds(**creds)
    creds['user'] = user
    creds['password'] = password
    creds['host'] = host
    creds['database'] = database

    return creds

class pyMySqlHelpers(object):
    """**class pyMySqlHelpers**: common helper fuctions given a pyMySql cursor
    
    If initialized with an active cursor, the `tables` attribute
    will be populated with the available tables with some descriptive
    information, such as number of rows and column names. 
    
    **Attributes**
    
    - cursor: Can be None, but if filled with pyMySql cursor, 
              can be used by default within methods.
    - tables: dict. A dictionary of available tables given active cursor.
              If populated, will hold some table descriptive info.
    - get_table_properties: bool. Get table descriptive info by default. 
              
    **Visible Methods**
    
    - create_randomized_table: Given a table t, will create a row-randomized version.
    - get_table_nrows: Given a table t, will return the number of rows of t.
    - get_table_colnames: Given a table t, will return a list of column names of t. 
    - get_chunk: Given a table t, a starting index and a chunk size, will return
                 the corresponding chunk.
    - generate_chunks: Given a table t and number of chunks n, will create a generator
                       to return iterating linearly through table thru n chunks.
    
    #########################
    
    """
    def __init__(self, 
                 cursor = None, 
                 get_table_properties = True):
        self.get_table_properties = get_table_properties
        self.cursor = cursor
    
    
    @property
    def cursor(self):
        return self.__cursor
    
    @cursor.setter
    def cursor(self, cursor):
        if cursor:
            try:
                self.cursor.close()
            except:
                pass
            finally:
                self.__cursor = cursor
                self._reset_tables(cursor)
                
                
    def _reset_tables(self, cursor = None):
        """**method _reset_tables**: Reset and setup tables again from cursor. 
        """
        self.tables = {}
        self._setup_tables(cursor)
    
    @staticmethod
    def _get_extant_tables(cursor):
        """**method _get_extant_tables**: _setup_tables helper function.
        """
        cursor.execute("SHOW TABLES;")
        return [tbl[0] for tbl in cursor.fetchall()]
    
    def _setup_tables(self, cursor = None):
        """**method _setup_tables**: Get table info from cursor.
        """
        if not cursor:
            cursor = self.cursor
        if cursor:
            tbl_names = self._get_extant_tables(cursor)
            for tbl_name in tbl_names:
                tbl = self.tables.setdefault(tbl_name, {})
                if self.get_table_properties:
                    tbl['nrows'] = self.get_table_nrows(tbl_name, cursor)
                    tbl['colnames'] = self.get_table_colnames(tbl_name, cursor)
        else:
            tbl_names = []
        return tbl_names
            
    
    @staticmethod
    def _create_randomized_table(table_name,
                                 random_table_name, 
                                 cursor, 
                                 keep_cols = None):
        """**method _create_randomized_table**: create_randomized_table helper function.
        """
        
        if not keep_cols:
            keep_cols = ['*']
        
        return cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {random_table_name} AS
            SELECT {",".join(keep_cols)} 
            FROM {table_name} 
            ORDER BY RAND();
        """)
    
    def create_randomized_table(self, 
                                table_name,
                                random_table_name, 
                                cursor = None,
                                keep_cols = None,
                                verbose = True):
        """**method create_randomized_table**:
        Create a new row-randomized table from existing table.
        """
        
        if not cursor:
            cursor = self.cursor
        if not cursor:
            raise ValueError("No cursor found to create randomized table.")
        res = self._create_randomized_table(table_name, random_table_name, cursor, keep_cols)
        tbl = self.tables.setdefault(random_table_name,{})
        if res:
            tbl['nrows'] = res
        if self.get_table_properties:
            tbl['nrows'] = self.get_table_nrows(random_table_name, cursor)
            tbl['colnames'] = self.get_table_colnames(random_table_name, cursor)
            if verbose:
                print(f"{random_table_name} exists with {tbl['nrows']} rows.")
        elif verbose:
            print(f"{random_table_name} exists.")
        pass
    
    @staticmethod
    def _get_table_nrows(table_name, cursor):
        """**method _get_table_nrows**: get_table_nrows helper function.
        """
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        return cursor.fetchall()[0][0]
    
    def get_table_nrows(self, 
                        table_name, 
                        cursor = None, 
                        recalc = False):
        if not cursor:
            cursor = self.cursor
        tbl = self.tables.setdefault(table_name,{})
        if tbl:
            nrows = tbl.get('nrows')
            if nrows and (not recalc):
                return nrows
            
        tbl['nrows'] = self._get_table_nrows(table_name, cursor)
        return tbl['nrows']
    
    @staticmethod
    def _get_table_colnames(table_name, cursor):
        """**method _get_table_colnames**: get_table_colnames helper function.
        """
        cursor.execute(f"SHOW COLUMNS FROM {table_name};")
        return [c[0] for c in cursor.fetchall()]
    
    def get_table_colnames(self, 
                           table_name, 
                           cursor = None, 
                           recalc = False):
        if not cursor:
            cursor = self.cursor
            if not cursor:
                raise ValueError("No cursor found to count rows.")
        tbl = self.tables.setdefault(table_name,{})
        if tbl:
            colnames = tbl.get('colnames')
            if colnames and (not recalc):
                return colnames

        tbl['colnames'] = self._get_table_colnames(table_name, cursor)
        return tbl['colnames']
    
    def get_chunk(self, 
                  table_name,
                  start_idx, 
                  chunk_size, 
                  cursor = None,
                  keep_cols = []):
        if not cursor:
            cursor = self.cursor
            if not cursor:
                raise ValueError("No cursor found to create table chunk.")
        if not keep_cols:
            keep_cols = ['*']
        cursor.execute(f"""
        SELECT {','.join(keep_cols)}
            FROM {table_name}
            LIMIT {start_idx},{chunk_size};
        """)
        return cursor.fetchall()
    
    def generate_chunks(self, 
                        table_name,  
                        n_chunks = 10,
                        cursor = None,
                        keep_cols = [],
                        as_pandas = True, 
                        respect_index = True):
        
        if not cursor:
            cursor = self.cursor
            if not cursor:
                raise ValueError("No cursor found to create table chunks.")
        if table_name not in self.tables:
            return False
        tbl = self.tables.get(table_name)
        nrows = tbl.setdefault('nrows', self.get_table_nrows(table_name, cursor))
        if (not keep_cols) or ('*' in keep_cols):
            colnames = tbl.setdefault('colnames', self.get_table_colnames(table_name, cursor))
        else:
            colnames = keep_cols
        chunk_size = nrows // n_chunks
        chunk_sizes = [chunk_size]*(n_chunks-1) + [nrows - chunk_size*(n_chunks-1)]
        
        start_idx = 1
        for cs in chunk_sizes:
            cnk = self.get_chunk(table_name = table_name, 
                                 cursor = cursor, 
                                 start_idx = start_idx, 
                                 chunk_size = cs, 
                                 keep_cols = keep_cols)
            if as_pandas:
                index = range(start_idx-1, start_idx-1+cs) if respect_index else None
                try:
                    yield pd.DataFrame(cnk, columns = colnames, index = index)
                except Exception as e:
                    print("Issue coericing to data frame. Yielding raw chunk.")
                    yield cnk
            else:
                yield cnk           
            start_idx += cs
        
    

class pyMySqlConnectionHelper(pyMySqlHelpers):
    """**class pyMySqlConnectionHelper**: Child class of `pyMySqlHelpers`
    Intended to aid setting up the connection.
    
    **Additional Visible Attributes**:
    
    - connection: The pyMySql connection object created from user credentials.
    
    **Additional Visible Methods**:
    
    - from_json_creds: Create an instance of this class through saved json credentials.
    - save_json_creds: Save MySQL credentials to json file.
    - open_connection: Open a new connection to MySQL database based on user credentials.
    
    #########################
    
    """
    
    __doc__ += pyMySqlHelpers.__doc__
    
    def __init__(self, 
                 creds = {}, 
                 auto_connect = True, 
                 get_table_properties = True,
                 **kwargs):
        self.__creds = _get_creds(**creds, **kwargs)
        if auto_connect:
            self.open_connection(_on_init = True)
            cursor = self.connection.cursor()
        else:
            cursor = None
        
        super().__init__(cursor = cursor, get_table_properties = get_table_properties)
        
    @classmethod
    def from_json_creds(cls, creds_fp, check_creds = False, **kwargs):
        """**class method from_json_creds**:
        Open a new connection from json credentials.
        
        **Parameters**
        
        - creds_fp: str. File path to the json credentials file. 
        - check_creds: bool. If True, will prompt user if they wish 
                       to check loaded credentials.
        - **kwargs: Additional arguments to pass to the connect method.
        
        """
        with open(creds_fp, 'r') as fin:
            creds = json.load(fin)
        return cls(creds, check_creds = check_creds, **kwargs)

    def save_json_creds(self, json_fp):
        """**method save_json_creds**: Save credentials to json.
        """
        with open(json_fp, 'w') as fout:
            json.dump(self.__creds, fout)
        return json_fp
    
    def open_connection(self, verbose = True, _on_init = False):
        """**method open_connection**: 
        Open a connection based on stored credentials.
        
        **Parameters**
        
        - verbose: bool. Whether to print out connection message on success.
        - _on_init: bool. Intended to be False except for class initialization.
        
        Returns None.
        
        """
        c = pymysql.connect(**self.__creds)
        self.connection = c
        if c.open:
            if verbose:
                print("\n".join([f"New connection established to '{self.__creds['database']}'"]))
            if not _on_init:
                cursor = c.cursor()
                self.cursor = cursor
        else:
            print("No connection established.")
            self.cursor = None
    
    @property
    def connection(self):
        return self.__cnx
    
    @connection.setter
    def connection(self, c):
        try:
            self.connection.close()
        except:
            pass
        finally:
            self.__cnx = c