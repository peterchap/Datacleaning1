from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# an Engine, which the Session will use for connection
# resources

def table_factory (name, tablename, schemaname):

   table_class = type(
        name,
        (Base,),
        dict (
            __tablename__ = tablename,
            __table_args__ = {'schema': schemaname}
            )
        )
    return table_class

server = '78.129.204.215'
database = 'ListRepository'

engine = create_engine("mssql+pyodbc://perf_webuser:n3tw0rk!5t@t5@" + server + "/" + database + "?driver=ODBC+Driver+17+for+SQL+Server")

Base = declarative_base()


tia = table_factory (name = "temp_tia",
                         tablename = "temp_tia",
                         schemaname = 'dbo')

dbcols =['list_id', 'source_url', 'title', 'first_name', 'last_name', 'id', 'email',\
'optin_date','is_duplicate','is_ok','is_blacklisted','is_banned_word','is_banned_domain','is_complaint',\
'is_hardbounce','domain','user_status', 'last_open','last_click','system_created','master_filter',\
'import_filter','email_id','primary_membership_id','primary_membership']
     

# create a Session

Session = Session = sessionmaker(engine)
session = Session()

# work with sess
session.bulk_insert_mappings(tia, df.to_dict(orient="records"))
session.commit()
session.close()