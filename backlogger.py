import discord, re, time, requests, pickle, logging, sys, io, random, datetime, sqlalchemy, asyncio, os, pathlib
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from BackloggerySession import BackloggerySession
import database
from database import bot_timezone

db = database.Database()
users = db.get_users()
sched = None

name_to_id = {}
for user_id in users:
    name_to_id[ users[user_id]["name"] ] = user_id

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# create a file handler
handler = logging.FileHandler( str(pathlib.Path().resolve() / "sana.log") )
handler.setLevel(logging.INFO)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

TOKEN = os.environ['BACKLOGGER_BOT_TOKEN']

# discord.py doesn't play nice with Python 3.10 so we need this workaround for the time being.
if sys.platform == "win32" and (3, 8, 0) <= sys.version_info:
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
client = discord.Client()

boardchan_id = 528365754056441856
board_id = 542496437796339723
mainchan_id = 481237800449081356
gifchan_id = 595801066730422282

async def update_board(client):
    scores = {}
    data = db.get_current_year_scores()
    for user in users:
        for status in [2,3,4]:
            count = (data[user][status] if user in data and status in data[user] else 0)
            scores.setdefault(users[user]["name"], []).append(count)
            
    cur_year = datetime.datetime.now(tz=bot_timezone).year
    first_line = 'Stats for {0}:'.format(cur_year)
    text = first_line + '''

Doda:   b - {0}  | c - {1} | m - {2}
ffao:     b - {3} | c - {4} | m - {5}
Monk:  b - {6} | c - {7} | m - {8}'''
    text = text.format(*(scores["Doda"] + scores["ffao"] + scores["Monk"]))

    msg = await client.get_channel(boardchan_id).fetch_message(board_id)
    await msg.edit(content=text)

#------ BOT COMMANDS ------

async def parrot(content, message, channel):
    """
    Sends content to channel, also including attachments in message. Mostly used to copy messages from one channel to another.
    
    Parameters
    ----------
    content : 
        The content that will be sent.
    message : 
        The original message whose attachments will be copied.
    channel : 
        Target channel to send the message.
    """
    atts = []
    for att in message.attachments:
        att_data = await att.read()
        file_obj = io.BytesIO(att_data)
        atts.append(discord.File(file_obj, filename=att.filename))

    emojis = re.findall(r':(\w*):', content)
    for message_emoji in emojis:
        for emoji in client.emojis:
            if emoji.name == message_emoji:
                content = content.replace(':%s:' % message_emoji, str(emoji))

    await channel.send(content, files=atts)

def is_video_message(message):
    """
    Does a simplistic attempt at recognizing if a message contains animated content.
    
    Parameters
    ----------
    message : 
        The message to check
    
    Returns
    -------
    A boolean indicating if the message contains animated content.
    """
    if 'gfy' in message.content: 
        return True
    for att in message.attachments:
        if 'gif' in att.filename or 'mp4' in att.filename:
            return True
    return False

sana_messages = []
rigged = False

def rig():
    """
    Sets a flag so the next call to send_sana_msg will send a predefined message.    
    """
    global rigged
    rigged = True

async def refresh():
    """
    Loads and saves in memory messages in the Sana channel that can be sent by the !sana command.
    """
    global sana_messages
    sana_messages = await client.get_channel(gifchan_id).history(limit=None).flatten() 

async def send_sana_msg(channel, require_video=False):
    """
    Picks a random message from a set of predefined messages and sends it to the specified channel.
    
    Parameters
    ----------
    channel : 
        Target channel to send the message to
    require_video=False : 
        Restrict the choice to messages that contain animated content (gifs, mp4s, etc)
    """
    global rigged
    if rigged:
        rigged = False
        await channel.send("https://gfycat.com/acidicunequaledfirecrest")
        return

    if not sana_messages: 
        await refresh()
    msg = random.choice(sana_messages)
    while require_video and not is_video_message(msg):
        msg = random.choice(sana_messages)
    await parrot(msg.content, msg, channel)

async def newyear():
    channel = client.get_channel(mainchan_id)
    await channel.send("Feliz ano novo! Que todos zerem mais jogos em 2022! :heart:")
    await channel.send("https://gfycat.com/imaginaryhideousdrafthorse")

    first_line = 'Stats for {0}:'.format(cur_year)
    text = first_line + '''

Doda:   b - {0}  | c - {1} | m - {2}
ffao:     b - {3} | c - {4} | m - {5}
Monk:  b - {6} | c - {7} | m - {8}'''
    text = text.format(*(scores["Doda"] + scores["ffao"] + scores["Monk"]))

    boardchannel = client.get_channel(boardchan_id)
    await boardchannel.send(text)

    await update_board(client)

emoji_to_status = {"beaten": 2, "completed": 3, "mastered": 4}
status_to_emoji = dict( (emoji_to_status[emoji], emoji) for emoji in emoji_to_status )
status_to_verb = {2: "beating", 3:"completing", 4: "mastering"}

async def make_adjustment(message, status, delta):
    """
    Adds an adjustment to the counts to the user indicated in message.
    Used in case of data entry mistakes, also used to migrate the old pickle database (that only had counts) to the new schema.
    
    Parameters
    ----------
    message : 
        The message that triggered the adjustment
    status : 
        The status whose count should be adjusted
    delta : 
        Value of the adjustment
    """
    parts = message.content.split()
    if len(parts) >= 2: name = parts[1]
    else: name = users[message.author.id]["name"]
    db.add_adjustment( name_to_id[name], status, delta)
    await message.channel.send("Removing one {1} game from {0}...".format(name, status_to_emoji[status]))
    await update_board(client)


#------ BOT EVENTS AND MESSAGE PARSING ------

@client.event
async def on_message(message):
    # we do not want the bot to reply to itself
    if message.author == client.user or message.author.id not in users:
        return

    update_pattern = r'(.*) <:(beaten|completed|mastered):\d*> ?!?(|.*\[\[(.*?)/(.*)\]\].*)$'
    m = re.match(update_pattern, message.content)
    if m is not None:
        groups = m.groups()
        status = emoji_to_status[ groups[1] ]
        name, console, comment = groups[0], groups[3], groups[4]
            
        if "[[" in message.content and users[message.author.id]["bl_user"]:
            bl_user = users[message.author.id]["bl_user"]
            bl_pass = users[message.author.id]["bl_pass"]
            logger.info("Adding game '{0}' on console '{1}' to {2}'s backloggery...".format(name,console,users[message.author.id]["name"]))
            
            comment_len = len(comment.encode('utf-16-le'))//2
            if comment_len > 150:
                await message.channel.send('The comment is too long! It has {0} characters while the maximum is 150.'.format(comment_len))
                return
            
            already_exists = False
            with BackloggerySession() as s:
                s.login(bl_user, bl_pass)
                bl_game_info = s.find_game(name, bl_user, console)
                if bl_game_info is not None:
                    gameid, bl_status = bl_game_info
                    already_exists = True
            
                if already_exists and bl_status > status:
                    await message.channel.send("Game '{0}' already exists on {1}'s backloggery with status {2}!".format(name, users[message.author.id]["name"], status_to_emoji[bl_status].capitalize()))
                else:
                    await message.channel.send("Adding game '{0}' on console '{1}' to {2}'s backloggery...".format(name,console,users[message.author.id]["name"]))
                    logger.info("Logged in! Waiting 10 seconds...")
                    time.sleep(10)
                    if not already_exists:
                        s.create_game(name, console, comment, bl_user)
                        logger.info("Created game!")
                        gameid, bl_status = s.find_game(name, bl_user, console)
                        logger.info("Found game! Waiting 10 seconds...")
                        time.sleep(10)
                    s.update_game(gameid, name, console, comment, str(status), bl_user)
                    logger.info("Updated status!")
        
        db_status = db.get_game_status(message.author.id, name)
        if db_status is not None and db_status >= status:
            await message.channel.send("Game '{0}' was already added previously!".format(name))
        else:
            db.add_game(message.author.id, name, status, console, comment)
            scores = db.get_current_year_scores()
            await message.channel.send("Congratulations on {2} game #{0} this year, {1}!".format(scores[message.author.id][status], users[message.author.id]["name"], status_to_verb[status]))
            await update_board(client)
            
            if db_status is not None and db_status < status:
                await message.channel.send("Removing one {1} game from {0}...".format(users[message.author.id]["name"], status_to_emoji[db_status]))

    if message.content.startswith("!unbeat"):
        await make_adjustment(message, 2, -1)

    if message.content.startswith("!uncomplete"):
        await make_adjustment(message, 3, -1)

    if message.content.startswith("!unmaster"):
        await make_adjustment(message, 4, -1)

    if message.content.strip() == "!sana":
        await send_sana_msg(message.channel)

    if message.content.strip() == "!sana gif":
        await send_sana_msg(message.channel, require_video=True)
        
    if message.content.strip() == "!undo":
        await message.channel.send("Undoing last change...")
        db.remove_last_history_entry()
        await update_board(client)

    if users[message.author.id]["is_admin"]:
        if message.content == "!quitsana":
            await client.logout()

        if message.content == "!refresh":
            await refresh()

        if message.content == "!rig":
            rig()
        
        if message.content == "!scores":
            await message.channel.send(str(db.get_current_year_scores()))

        if message.content.startswith("!parrotboard"):
            await parrot(message.content[12:], message, client.get_channel(boardchan_id))

        elif message.content.startswith("!parrotmain"):
            await parrot(message.content[11:], message, client.get_channel(mainchan_id))

        elif message.content.startswith("!parrot"):
            await parrot(message.content[7:], message, message.channel)

import traceback
@client.event
async def on_error(event, *args, **kwargs):
    logger.warning(traceback.format_exc()) #logs the error
    message = args[0] #Gets the message object
    await message.channel.send("I had some problems processing the last command. You can doublecheck your command or ask your nearest ffao for debugging help.") 
    await message.channel.send("```%s```" % traceback.format_exc())

@client.event
async def on_ready():
    global sched
    logger.info('Logged in as')
    logger.info(client.user.name)
    logger.info(client.user.id)
    logger.info('------')

    await client.get_guild(481237800449081354).me.edit(nick="Sana")
    
    if sched is None:
        sched = AsyncIOScheduler()
        cur_year = datetime.datetime.now(tz=bot_timezone).year
        next_year_start = datetime.datetime(year=cur_year+1, day=1, month=1, tzinfo=bot_timezone).astimezone(datetime.timezone.utc)
        sched.add_job(newyear, 'date', run_date=next_year_start)
        sched.start()

#------ DEFINE A WINDOWS SERVICE ------

import win32service  
import win32serviceutil  
import win32event  
import asyncio
import servicemanager
import sys

class PySvc(win32serviceutil.ServiceFramework):  
    _svc_name_ = "Sana"    
    _svc_display_name_ = "Sana"  
    _svc_description_ = "Discord bot"    

    def SvcDoRun(self):  
        self.loop = asyncio.get_event_loop()
        self.ReportServiceStatus(win32service.SERVICE_RUNNING)
        self.loop.run_until_complete(client.start(TOKEN))
      
    # called when we're being shut down      
    def SvcStop(self):
        # tell the SCM we're shutting down  
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)  
        asyncio.ensure_future(client.logout(), loop=self.loop)
          
if __name__ == '__main__':
    if sys.argv[-1] == "standalonerun":
        loop = asyncio.get_event_loop()
        loop.run_until_complete(client.start(TOKEN))
    else:
        win32serviceutil.HandleCommandLine(PySvc)