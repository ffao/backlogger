from sqlalchemy import Table, Column, Integer, String, BigInteger, Boolean, MetaData, DateTime, create_engine, func, column, UnicodeText
from sqlalchemy.sql import select, delete
import datetime, pathlib
from zoneinfo import ZoneInfo

bot_timezone = ZoneInfo("America/Sao_Paulo")

metadata_obj = MetaData()
users = Table('users', metadata_obj,
     Column('id', BigInteger, primary_key=True),
     Column('name', UnicodeText(100)),
     Column('bl_user', String),
     Column('bl_pass', String),
     Column('is_admin', Boolean)
)

# It would make sense to have a table to be able to directly query current status, but making sure both tables are always updated together would add complexity.
# Since this project is really small I'm going for the simple solution and just querying the history table if I need to.
game_history = Table('game_history', metadata_obj,
    Column('user_id', BigInteger),
    Column('game_name', UnicodeText),
    Column('console', String(10)),
    Column('time', DateTime),
    Column('status', Integer),
    Column('comment', UnicodeText)
)

# Mistakes when adding games are very common so it's useful to have a quick and dirty way to correct them.
# Using !undo is preferred if the erroneous entry is the latest one in the history table.
adjustments = Table('adjustments', metadata_obj,
    Column('user_id', BigInteger),
    Column('status', Integer),
    Column('time', DateTime),
    Column('delta', Integer)
)

class Database:
    def __init__(self):
        score_path = pathlib.Path().resolve() / "scores.db"
        self.engine = create_engine(r'sqlite:///' + str(score_path))
    
    def get_current_year_scores(self):
        current_year = datetime.datetime.now(tz=bot_timezone).year
        year_start_utc = datetime.datetime(year=current_year, month=1, day=1, tzinfo=bot_timezone).astimezone(datetime.timezone.utc)
        
        # The following mess is for counting how many games have a certain status, but only if the status is the highest status the game had
        # For example, if a game gets updated to 2, then 3, it counts only as a 3
        # greatest-n-per-group is already a bit messy in SQL, it gets even worse in SQLAlchemy...
        b = (
            select( game_history.c.user_id, game_history.c.game_name, func.max(game_history.c.status).label('status') )
            .group_by(game_history.c.user_id, game_history.c.game_name)
            .where(game_history.c.time >= year_start_utc)
        )
        a = game_history.join(b, (game_history.c.user_id == b.c.user_id) & (game_history.c.game_name == b.c.game_name) & (game_history.c.status == b.c.status))
        s = (
            select(a.c.game_history_user_id, a.c.game_history_status, func.count('*').label('count'))
            .select_from(a)
            .where(a.c.game_history_time >= year_start_utc)
            .group_by(a.c.game_history_user_id, a.c.game_history_status)
        )
        
        d = {}
        with self.engine.connect() as conn:
            result = conn.execute(s)
            for row in result:
                d.setdefault(row["user_id"], {})[ row["status"] ] = row["count"]
                
        with self.engine.connect() as conn:
            result = conn.execute(select(adjustments.c.user_id, adjustments.c.status, func.sum(adjustments.c.delta).label('count')).group_by(
                adjustments.c.user_id, adjustments.c.status).where(adjustments.c.time >= year_start_utc))
            for row in result:
                if row["status"] not in d.setdefault(row["user_id"], {}):
                    d[row["user_id"]][ row["status"] ] = row["count"]
                else:
                    d[row["user_id"]][ row["status"] ] += row["count"]
        
        return d
        
    def add_adjustment(self, user_id, status, delta, time=None):
        if time is None:
            time = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.engine.connect() as conn:
            conn.execute(adjustments.insert(), {"user_id": user_id, "status": status, "delta": delta, "time": time})
            
    def add_game(self, user_id, game_name, status, console=None, comment=None, time=None):
        if time is None:
            time = datetime.datetime.now(tz=datetime.timezone.utc)
        with self.engine.connect() as conn:
            conn.execute(game_history.insert(), {"user_id": user_id, "game_name": game_name, "status": status, "comment": comment, "time": time, "console": console})
            
    def get_users(self):
        d = {}
        with self.engine.connect() as conn:
            result = conn.execute(select(users))
            for row in result:
                d[ row["id"] ] = {}
                d[ row["id"] ]["name"] = row["name"]
                d[ row["id"] ]["bl_user"] = row["bl_user"]
                d[ row["id"] ]["bl_pass"] = row["bl_pass"]
                d[ row["id"] ]["is_admin"] = row["is_admin"]
        return d
        
    def get_game_status(self, user_id, game):
        # Fairly inefficient, but as long as we do not have a current status table, scanning the history is the only option.
        with self.engine.connect() as conn:
            return conn.execute(
                select( func.max(game_history.c.status) ).where((game_history.c.game_name == game) & (game_history.c.user_id == user_id))
            ).scalar()
            
    def remove_last_history_entry(self):
        # The bot processes messages one at a time so we can be sure that there won't be two entries with the same timestamp
        with self.engine.connect() as conn:
            conn.execute( delete(game_history).where(game_history.c.time == select(func.max(game_history.c.time)).scalar_subquery()) )