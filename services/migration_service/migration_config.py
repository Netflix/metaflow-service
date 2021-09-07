from services.utils import DBConfiguration
# Shell Related Password and username setting happens here for Goose. 
# Ensure we escape the right characters in username/password to avoid goose failures. 
SHELL_ESCAPE_TRANSLATION = str.maketrans({"-":  r"\-",
                "]":  r"\]",
                "\\": r"\\",
                "^":  r"\^",
                "$":  r"\$",
                "*":  r"\*",
                ".":  r"\."})
db_conf = DBConfiguration()

host = db_conf.host
port = db_conf.port
user = db_conf.user.translate(SHELL_ESCAPE_TRANSLATION)
password = db_conf.password.translate(SHELL_ESCAPE_TRANSLATION)
database_name = db_conf.database_name
