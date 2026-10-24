import asyncio, json, os, random, socketserver, threading, time
from datetime import datetime
from http.server import SimpleHTTPRequestHandler

import discord
import requests
from discord import app_commands

TOKEN = os.environ.get("DISCORD_BOT_TOKEN")
WEBHOOK = os.environ.get("DISCORD_WEBHOOK_URL")
PORT = int(os.environ.get("PORT", 10000))
PROXY_URL = os.environ.get("PROXY_URL")  # ex: http://user:pass@host:port
CONFIG_FILE, SEEN_FILE = "config.json", "seen_items.json"
API = "https://www.vinted.fr/api/v2/catalog/items"
MAX_SEEN = 20000

START = time.time()
state = {"paused": False, "http_ok": False, "last_scan": None, "last_status": "démarrage"}

# ---------- Persistance ----------
def load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default

def save_json(path, data):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(tmp, path)

config = load_json(CONFIG_FILE, {"_scan_interval_seconds": 30, "_max_price": 50, "_keywords": []})
seen = load_json(SEEN_FILE, [])
seen_set = set(map(str, seen))

def save_config(): save_json(CONFIG_FILE, config)

def mark_seen(item_id):
    item_id = str(item_id)
    if item_id in seen_set:
        return
    seen_set.add(item_id); seen.append(item_id)
    if len(seen) > MAX_SEEN:
        for old in seen[:len(seen) - MAX_SEEN]:
            seen_set.discard(old)
        del seen[:len(seen) - MAX_SEEN]
    save_json(SEEN_FILE, seen)

# ---------- Serveur keep-alive ----------
class Handler(SimpleHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(b"Vortex en ligne")
    def log_message(self, *a): pass

def run_keepalive():
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("", PORT), Handler) as srv:
        state["http_ok"] = True
        srv.serve_forever()

# ---------- Vinted ----------
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "fr-FR,fr;q=0.9",
    "Referer": "https://www.vinted.fr/",
})
if PROXY_URL:
    session.proxies.update({"http": PROXY_URL, "https": PROXY_URL})
_last_session = 0.0

def ensure_session(force=False):
    global _last_session
    if force or time.time() - _last_session > 20 * 60:
        session.cookies.clear()
        session.get("https://www.vinted.fr/", timeout=15)
        _last_session = time.time()

def fetch_items(keyword):
    """Exécuté dans un thread (bloquant). Retourne la liste d'items ou []."""
    params = {"search_text": keyword, "order": "newest_first",
              "per_page": 40, "price_to": config["_max_price"]}
    for attempt in range(4):
        try:
            ensure_session()
            r = session.get(API, params=params, timeout=15)
            if r.status_code in (401, 403):
                ensure_session(force=True); continue
            if r.status_code == 429 or r.status_code >= 500:
                time.sleep(min(5 * 2 ** attempt, 120)); continue
            r.raise_for_status()
            return r.json().get("items", [])
        except Exception as e:
            state["last_status"] = f"erreur: {type(e).__name__}"
            time.sleep(min(5 * 2 ** attempt, 120))
    return []

def parse_price(p):
    if isinstance(p, dict):
        return p.get("amount", "?")
    return p if p is not None else "?"

def send_embed(keyword, item):
    price = parse_price(item.get("price"))
    url = item.get("url") or f"https://www.vinted.fr/items/{item['id']}"
    photo = (item.get("photo") or {}).get("url")
    embed = {
        "title": f"🎯 MOT-CLÉ TROUVÉ [{keyword.upper()}] : {item.get('title', '')}"[:256],
        "url": url,
        "color": 0x09B1BA,
        "fields": [
            {"name": "💶 Prix", "value": f"**{price} €**", "inline": True},
            {"name": "🔗 Lien", "value": f"[Voir l'annonce]({url})", "inline": True},
        ],
        "footer": {"text": f"Scan à {datetime.now().strftime('%H:%M:%S')}"},
    }
    if photo:
        embed["image"] = {"url": photo}
    r = requests.post(WEBHOOK, json={"embeds": [embed]}, timeout=15)
    if r.status_code == 429:
        time.sleep(float(r.json().get("retry_after", 2)))
        requests.post(WEBHOOK, json={"embeds": [embed]}, timeout=15)

warmed = set()  # mots-clés déjà "amorcés" depuis le démarrage

async def scan_loop():
    while True:
        if state["paused"]:
            state["last_status"] = "en pause"
            await asyncio.sleep(2); continue
        state["last_status"] = "scan en cours"
        for kw in list(config["_keywords"]):
            if state["paused"]:
                break
            items = await asyncio.to_thread(fetch_items, kw)
            first_pass = kw not in warmed
            for it in items:
                iid = str(it.get("id"))
                if iid in seen_set:
                    continue
                mark_seen(iid)
                if not first_pass and WEBHOOK:
                    await asyncio.to_thread(send_embed, kw, it)
            warmed.add(kw)
            await asyncio.sleep(random.uniform(3, 6))
        state["last_scan"] = datetime.now().strftime("%H:%M:%S")
        state["last_status"] = "actif"
        await asyncio.sleep(config["_scan_interval_seconds"])

# ---------- Bot Discord ----------
class Vortex(discord.Client):
    def __init__(self):
        super().__init__(intents=discord.Intents.default())
        self.tree = app_commands.CommandTree(self)
    async def setup_hook(self):
        await self.tree.sync()
        self.loop.create_task(scan_loop())

bot = Vortex()

@bot.tree.command(name="recherche", description="Ajouter un mot-clé à surveiller")
async def recherche(i: discord.Interaction, mot_cle: str):
    kw = mot_cle.strip().lower()
    if kw in config["_keywords"]:
        return await i.response.send_message(f"ℹ️ '{kw}' est déjà surveillé.")
    config["_keywords"].append(kw); save_config()
    await i.response.send_message(f"✅ Le mot-clé '{kw}' a été ajouté à la surveillance.")

@bot.tree.command(name="stoprecherche", description="Retirer un mot-clé")
async def stoprecherche(i: discord.Interaction, mot_cle: str):
    kw = mot_cle.strip().lower()
    if kw not in config["_keywords"]:
        return await i.response.send_message(f"❌ '{kw}' n'est pas dans la liste.")
    config["_keywords"].remove(kw); warmed.discard(kw); save_config()
    await i.response.send_message(f"🗑️ Le mot-clé '{kw}' a été retiré.")

@bot.tree.command(name="motscles", description="Lister les mots-clés actifs")
async def motscles(i: discord.Interaction):
    k = config["_keywords"]
    txt = "\n".join(f"• {x}" for x in k) or "Aucun mot-clé."
    await i.response.send_message(f"🔍 **{len(k)} mot(s)-clé(s) surveillé(s)**\n{txt}")

@bot.tree.command(name="prixmax", description="Définir le prix maximum (€)")
async def prixmax(i: discord.Interaction, montant: app_commands.Range[int, 1, 100000]):
    config["_max_price"] = montant; save_config()
    await i.response.send_message(f"💰 Le prix maximum a été réglé à {montant}€.")

@bot.tree.command(name="intervalle", description="Pause entre deux cycles (secondes)")
async def intervalle(i: discord.Interaction, secondes: app_commands.Range[int, 10, 3600]):
    config["_scan_interval_seconds"] = secondes; save_config()
    await i.response.send_message(f"⏱️ Intervalle réglé à {secondes}s.")

@bot.tree.command(name="stats", description="Bilan d'activité")
async def stats(i: discord.Interaction):
    up = int(time.time() - START)
    h, m = up // 3600, (up % 3600) // 60
    await i.response.send_message(
        f"📊 **Stats**\n• Articles vus : {len(seen_set)}\n• Uptime : {h}h{m:02d}\n"
        f"• Serveur keep-alive : {'✅ port ' + str(PORT) if state['http_ok'] else '❌'}\n"
        f"• Scanner : {'⏸️ pause' if state['paused'] else '▶️ actif'} ({state['last_status']})\n"
        f"• Dernier cycle : {state['last_scan'] or '—'}\n"
        f"• Prix max : {config['_max_price']}€ • Intervalle : {config['_scan_interval_seconds']}s")

class Confirm(discord.ui.View):
    def __init__(self, author):
        super().__init__(timeout=30); self.author = author
    async def interaction_check(self, i):
        return i.user.id == self.author
    @discord.ui.button(label="Confirmer", style=discord.ButtonStyle.danger)
    async def ok(self, i: discord.Interaction, b: discord.ui.Button):
        seen.clear(); seen_set.clear(); save_json(SEEN_FILE, seen)
        await i.response.edit_message(content="🧹 seen_items.json réinitialisé.", view=None)
    @discord.ui.button(label="Annuler", style=discord.ButtonStyle.secondary)
    async def no(self, i: discord.Interaction, b: discord.ui.Button):
        await i.response.edit_message(content="Annulé.", view=None)

@bot.tree.command(name="cleardoublons", description="Réinitialiser les articles déjà vus")
async def cleardoublons(i: discord.Interaction):
    await i.response.send_message("⚠️ Vider l'historique ? Les annonces actuelles pourront être re-notifiées.",
                                  view=Confirm(i.user.id))

@bot.tree.command(name="pause", description="Mettre le scanner en pause")
async def pause(i: discord.Interaction):
    state["paused"] = True
    await i.response.send_message("⏸️ Scanner en pause.")

@bot.tree.command(name="resume", description="Relancer le scanner")
async def resume(i: discord.Interaction):
    state["paused"] = False
    await i.response.send_message("▶️ Scanner relancé.")

if __name__ == "__main__":
    if not TOKEN or not WEBHOOK:
        raise SystemExit("DISCORD_BOT_TOKEN et DISCORD_WEBHOOK_URL sont requis.")
    threading.Thread(target=run_keepalive, daemon=True).start()
    bot.run(TOKEN)
