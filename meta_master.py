import psycopg2
from psycopg2.extras import RealDictCursor
import json
from collections import OrderedDict
import argparse
import os

class redshift_handler:
  
  def __init__(self, db, user, password, host, port):
    self.db = db
    self.user = user
    self.password = password
    self.host = host
    self.port = port

  def connect_DB(self, r_DB=None):
    r_DB = r_DB if r_DB else self.db
    try:
      con = psycopg2.connect(database = r_DB,
                             user = self.user,
                             password = self.password,
                             host = self.host,
                             port = self.port)
      con.autocommit = True
    except Exception, e:
      raise e
    return con

  def get_tables_and_views(self):
    con = self.connect_DB()
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute("SELECT table_name, table_type\
                FROM information_schema.tables\
                WHERE table_schema='public'\
                AND table_type IN ('BASE TABLE','VIEW');")
    cur_data = cur.fetchall()
    tabel_list = [table_dict['table_name'] for table_dict in cur_data
                  if table_dict['table_type'] == 'BASE TABLE']
    view_list = [table_dict['table_name'] for table_dict in cur_data
                 if table_dict['table_type'] == 'VIEW']
    con.close()
    return {'tables' : tabel_list,
            'views' : view_list}

  def column_details(self,table,cur):
    column_details = {}
    cur.execute("SELECT info.table_schema, info.table_name,\
                info.column_name, info.ordinal_position,\
                info.column_default, info.is_nullable, pg.type,\
                pg.sortkey, pg.distkey\
                FROM information_schema.columns as info\
                INNER JOIN pg_table_def as pg ON pg.column = info.column_name\
                AND pg.tablename = info.table_name\
                WHERE info.table_name = '" + table +\
                "' ORDER BY info.ordinal_position")
    for column in cur.fetchall():
      column_details[column['column_name']] =\
      dict((k, v) for k, v in column.iteritems() if v is not None)
      
    return column_details

  def meta_data_extractor(self):
    table_details = {}
    view_details = {}
    con = self.connect_DB()
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    for d_table in self.get_tables_and_views()['tables']:
      table_details[d_table] = self.column_details(d_table,cur)
    for d_view in self.get_tables_and_views()['views']:
      view_details[d_view] = self.column_details(d_view,cur)
    meta_data = {'meta':{'table':table_details,
                         'view':view_details}}
    con.close()
    return meta_data

  def schema_export(self, output_Path=None):
    json_path = (output_Path+'/' if output_Path else '') +'meta_schema.json'
    with open(json_path, 'w') as meta:
      json.dump(self.meta_data_extractor(), meta, indent=4, sort_keys=True)
    return json_path

  def create_query(self, table_name, column_properties):
    columns = []
    sort_col = {}    
    col_sort = OrderedDict(sorted(column_properties.iteritems(),
                                  key=lambda x: x[1]['ordinal_position']))
    for col, prop in col_sort.iteritems():
      column_type = col + ' ' + prop['type']
      if prop['is_nullable'] == 'NO' :
        column_type += ' NOT NULL'
      if prop['sortkey'] != 0 :
        sort_col[col] = abs(prop['sortkey'])
      columns.append(column_type)
    sort_query = ''
    sort_col = OrderedDict(sorted(sort_col.iteritems(),
                                  key=lambda x: x[1]))
    if sort_col != {}:
      sort_query = ' interleaved sortkey ('+(', ').join(sort_col.keys())+')'
      
    return "CREATE TABLE " + table_name +'(' + (', ').join(columns) + ')'\
           + sort_query + ';'

  def table_updater(self, req_table_meta, dest_DB = None):
    con = self.connect_DB(dest_DB)
    cur = con.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    print "Creating Tables ........."
    for table in self.get_tables_and_views()['tables']:
      if cmp(self.column_details(table,cur), req_table_meta['meta']['table'][table]) != 0 :
        try: cur.execute("DROP TABLE " + table + ";")
        except: pass
        cur.execute(self.create_query(table, req_table_meta['meta']['table'][table]))
        print '- ' + table
      con.commit()
    con.close()
    print "Done !!"

  def view_handler(self, view_Json_Path):
    con = self.connect_DB()
    cur = con.cursor()
    print "Creating Views ........."
    with open(View_Json_Path) as view_query:
      req_view = json.load(view_query)
    for view in req_view['view']:
      cur.execute(view)
      con.commit()
    con.close()
    print 'Done !!'

  def meta_merger(self, meta_path_list):
    req_meta = {}
    for meta in meta_path_list:
      data = json.load(open(meta))
      if req_meta == {} :
        req_meta = data
        continue
      for d_tab in data['meta']['table'].keys():
        if d_tab in req_meta['meta']['table'].keys():
          for d_col in data['meta']['table'][d_tab].keys():
            if d_col in req_meta['meta']['table'][d_tab].keys():
              if cmp(data['meta']['table'][d_tab][d_col], req_meta['meta']['table'][d_tab][d_col]) != 0:
                req_meta['meta']['table'][d_tab][d_col] = data['meta']['table'][d_tab][d_col]
            else: req_meta['meta']['table'][d_tab][d_col] = data['meta']['table'][d_tab][d_col]
        else: req_meta['meta']['table'][d_tab] = data['meta']['table'][d_tab]

    return req_meta

  def schema_deploy(self, meta_list):
    self.table_updater(self.meta_merger(meta_list.split(',')))

  def copy_DB(self, dest_DB):
    meta_path = self.schema_export()
    self.table_updater(meta_path, dest_DB)
    print "copied to " + dest_DB


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Pass your redshift parameter through Env Variables :\
                                   MART_DB_NAME, MART_DB_USERNAME,\
                                   MART_DB_PASSWORD, MART_DB_HOST")
  parser.add_argument("-e","--schema_export", help="Schema Json Export (with path)")
  parser.add_argument("-v","--view_handler", help="Updating views. File path required.")
  parser.add_argument("-c","--copy_DB", help="Required destination DB")
  parser.add_argument("-s","--schema_deploy", help="Multiple Schema file path must be sepearated by ','")

  args = parser.parse_args()
  
  c = redshift_handler(db=os.environ.get('MART_DB_NAME'),
                       user=os.environ.get('MART_DB_USERNAME'),
                       password=os.environ.get('MART_DB_PASSWORD'),
                       host=os.environ.get('MART_DB_HOST'),
                       port='5439')    

  if args.schema_export:
    c.schema_export(args.schema_export)
  if args.view_handler:
    c.view_handler(args.view_handler)
  if args.copy_DB:
    c.copy_DB(args.copy_DB)
  if args.schema_deploy:
    c.schema_deploy(args.schema_deploy)
  
