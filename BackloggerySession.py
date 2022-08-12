import requests, re

class BackloggerySession:
    def __init__(self):
        self.session = requests.Session()
        
    def __enter__(self):
        self.session.__enter__()
        return self
        
    def __exit__(self, exc_type, exc_value, traceback):
        return self.session.__exit__(exc_type, exc_value, traceback)
        
    def login(self, bl_user, bl_pass):
        payload = {"username": bl_user, "password": bl_pass}
        self.session.post("https://backloggery.com/login.php", data=payload)
    
    def create_game(self, name, console, note, bl_user):
        defaults = {"comp": "", "orig_console": "", "region": "0", "own": "1", "achieve1": "", "achieve2": "",
                    "online": "", "note": "", "rating": "8", "submit2": "Stealth Add", "comments": "", "complete": "1"}

        defaults["name"]=name
        defaults["console"]=console
        defaults["note"]=note
        params = {"user": bl_user}

        self.session.post("https://backloggery.com/newgame.php", data=defaults, params=params)

    def find_game(self, name, bl_user, console=""):
        params = {"user":bl_user,"console":"","rating":"","status":"","unplayed":"","own":"","region":"","region_u":"0","wish":"","alpha":"1","total":"0","aid":"1","ajid":"0","temp_sys":"ZZZ"}
        params["search"]=name
        params["console"]=console
        p = self.session.get('https://backloggery.com/ajax_moregames.php', params=params)
        
        m = re.search('gameid=(.*?)"', p.text)
        if m is None:
            return None
        game_id = m.groups()[0]
        
        m = re.search('img alt="\((.*?)\)"',p.text)
        status = m.groups()[0]
        status = {"U":1, "B":2, "C":3, "M":4}[status]

        return (game_id, status)

    def update_game(self, gameid, name, console, note, status, bl_user):
        defaults = {"comp": "", "orig_console": "", "region": "0", "own": "1", "achieve1": "", "achieve2": "",
                    "online": "", "note": "", "rating": "8", "submit1": "Save", "comments": ""}

        defaults["name"]=name
        defaults["console"]=console
        defaults["note"]=note
        defaults["status"]=status
        params = {"user": bl_user, "gameid": gameid}

        p = self.session.post("https://backloggery.com/update.php", data=defaults, params=params)