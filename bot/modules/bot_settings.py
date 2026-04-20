from aiofiles import open as aiopen
from aiofiles.os import remove, rename, path as aiopath
from aioshutil import rmtree
from ast import literal_eval, parse
from asyncio import (
    create_subprocess_exec,
    create_subprocess_shell,
    sleep,
    gather,
)
from functools import partial
from io import BytesIO
from os import getcwd
from pathlib import Path
from pyrogram.filters import create
from pyrogram.handlers import MessageHandler
from time import time

from .. import (
    LOGGER,
    drives_ids,
    drives_names,
    index_urls,
    intervals,
    aria2_options,
    task_dict,
    qbit_options,
    sabnzbd_client,
    nzb_options,
    jd_listener_lock,
    excluded_extensions,
    included_extensions,
    auth_chats,
    sudo_users,
)
from ..helper.ext_utils.bot_utils import (
    SetInterval,
    new_task,
)
from ..core.config_manager import Config
from ..core.telegram_manager import TgClient
from ..core.torrent_manager import TorrentManager
from ..core.startup import update_qb_options, update_nzb_options, update_variables
from ..helper.ext_utils.db_handler import database
from ..core.jdownloader_booter import jdownloader
from ..helper.ext_utils.task_manager import start_from_queued
from ..helper.mirror_leech_utils.rclone_utils.serve import rclone_serve_booter
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.message_utils import (
    send_message,
    send_file,
    edit_message,
    update_status_message,
    delete_message,
)
from .rss import add_job
from .search import initiate_search_tools

start = 0
state = "view"
handler_dict = {}
DEFAULT_VALUES = {
    "LEECH_SPLIT_SIZE": TgClient.MAX_SPLIT_SIZE,
    "RSS_DELAY": 600,
    "STATUS_UPDATE_INTERVAL": 15,
    "SEARCH_LIMIT": 0,
    "UPSTREAM_BRANCH": "master",
    "DEFAULT_UPLOAD": "rc",
}

SETTING_LABELS = {
    "AS_DOCUMENT": "Send as Document",
    "DEFAULT_UPLOAD": "Default Upload Tool",
    "EQUAL_SPLITS": "Split Evenly",
    "MEDIA_GROUP": "Media Group",
    "STATUS_UPDATE_INTERVAL": "Status Refresh Interval",
    "QUEUE_ALL": "Total Parallel Tasks",
    "QUEUE_DOWNLOAD": "Parallel Downloads",
    "QUEUE_UPLOAD": "Parallel Uploads",
    "LEECH_SPLIT_SIZE": "Split Size",
    "RCLONE_PATH": "Default Rclone Path",
    "RCLONE_FLAGS": "Default Rclone Flags",
    "GDRIVE_ID": "Default Google Drive ID",
    "INDEX_URL": "Index URL",
    "BASE_URL": "Web Base URL",
    "BASE_URL_PORT": "Web Port",
    "RSS_CHAT": "RSS Target Chat",
    "RSS_DELAY": "RSS Poll Interval",
    "RSS_SIZE_LIMIT": "RSS Size Limit",
    "SEARCH_LIMIT": "Search Result Limit",
    "AUTHORIZED_CHATS": "Authorized Chats",
    "SUDO_USERS": "Sudo Users",
    "USER_SESSION_STRING": "User Session String",
    "USER_TRANSMISSION": "Leech by User Session",
    "HYBRID_LEECH": "Hybrid Leech",
    "FILES_LINKS": "File Link Buttons",
    "STOP_DUPLICATE": "Stop Duplicates",
    "USE_SERVICE_ACCOUNTS": "Use Service Accounts",
    "WEB_PINCODE": "Web PIN Code",
    "UPLOAD_PATHS": "Saved Upload Paths",
    "NAME_SUBSTITUTE": "Filename Rename Rules",
    "YT_DLP_OPTIONS": "yt-dlp Defaults",
    "GALLERY_DL_OPTIONS": "gallery-dl Defaults",
    "FFMPEG_CMDS": "FFmpeg Presets",
    "TG_PROXY": "Telegram Proxy",
    "UPSTREAM_REPO": "Update Repository",
    "UPSTREAM_BRANCH": "Update Branch",
    "USENET_SERVERS": "Usenet Servers",
}


def get_setting_label(key):
    return SETTING_LABELS.get(key, key.replace("_", " ").title())


def summarize_value(value):
    if value in ("", None, [], {}, ()):
        return "Not set"
    if isinstance(value, bool):
        return "Enabled" if value else "Disabled"
    if isinstance(value, dict):
        keys = list(value.keys())
        preview = ", ".join(map(str, keys[:3]))
        extra = "" if len(keys) <= 3 else f" +{len(keys) - 3} more"
        return f"{len(value)} saved ({preview}{extra})" if preview else "Empty dict"
    if isinstance(value, list):
        preview = ", ".join(map(str, value[:3]))
        extra = "" if len(value) <= 3 else f" +{len(value) - 3} more"
        return f"{len(value)} items ({preview}{extra})" if preview else "Empty list"
    text = str(value).strip()
    return text if len(text) <= 60 else f"{text[:57]}..."


def load_config_defaults():
    path = Path(__file__).resolve().parent.parent / "core" / "config_manager.py"
    defaults = {}
    module = parse(path.read_text(encoding="utf-8"))
    for node in module.body:
        if getattr(node, "name", None) != "Config":
            continue
        for item in node.body:
            if getattr(item, "__class__", None).__name__ == "Assign" and len(item.targets) == 1:
                target = item.targets[0]
                if getattr(target, "id", None):
                    try:
                        defaults[target.id] = literal_eval(item.value)
                    except Exception:
                        continue
        break
    return defaults


CONFIG_DEFAULTS = load_config_defaults()


def get_config_source(key, value):
    default_value = CONFIG_DEFAULTS.get(key)
    if value in ("", None, [], {}, ()):
        return "Unset"
    if key in CONFIG_DEFAULTS and value == default_value:
        return "Bot Default"
    return "Saved Here"


def is_bool_setting(key):
    return isinstance(CONFIG_DEFAULTS.get(key), bool)


def get_edit_instructions(key):
    if is_bool_setting(key):
        return "Choose an option below. You can still send <code>true</code> or <code>false</code>. Timeout: 60 sec"
    if key == "DEFAULT_UPLOAD":
        return "Send <code>rc</code> for Rclone or <code>gd</code> for Google Drive. Timeout: 60 sec"
    return "Send a new value. Timeout: 60 sec"


def validate_bot_variable(key, value):
    label = get_setting_label(key)
    raw_value = str(value).strip()
    if key in {"STATUS_UPDATE_INTERVAL", "QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD", "RSS_DELAY", "RSS_SIZE_LIMIT", "SEARCH_LIMIT", "BASE_URL_PORT", "TORRENT_TIMEOUT"}:
        if not raw_value.isdigit():
            raise ValueError(f"{label} must be a whole number.")
        number = int(raw_value)
        if number < 0:
            raise ValueError(f"{label} cannot be negative.")
        if key == "BASE_URL_PORT" and not 1 <= number <= 65535:
            raise ValueError(f"{label} must be between 1 and 65535.")
        if key == "STATUS_UPDATE_INTERVAL" and number == 0:
            raise ValueError(f"{label} must be greater than 0.")
        return number
    if key == "LEECH_SPLIT_SIZE":
        if not raw_value.isdigit():
            raise ValueError(f"{label} must be a number of bytes.")
        number = int(raw_value)
        if number <= 0:
            raise ValueError(f"{label} must be greater than 0.")
        return min(number, TgClient.MAX_SPLIT_SIZE)
    if key == "DEFAULT_UPLOAD":
        lowered = raw_value.lower()
        if lowered not in {"gd", "rc"}:
            raise ValueError(f"{label} must be either 'gd' or 'rc'.")
        return lowered
    if key in {"AUTHORIZED_CHATS", "SUDO_USERS", "BASE_URL", "RSS_CHAT", "GDRIVE_ID", "INDEX_URL", "RCLONE_PATH", "RCLONE_FLAGS", "UPSTREAM_REPO", "UPSTREAM_BRANCH"} and not raw_value:
        raise ValueError(f"{label} cannot be empty.")
    return value


async def get_buttons(key=None, edit_type=None):
    buttons = ButtonMaker()
    if key is None:
        buttons.data_button("Bot Defaults", "botset var")
        buttons.data_button("Private Files", "botset private")
        buttons.data_button("qBittorrent", "botset qbit")
        buttons.data_button("Aria2c", "botset aria")
        buttons.data_button("Sabnzbd", "botset nzb")
        buttons.data_button("JDownloader Sync", "botset syncjd")
        buttons.data_button("Close", "botset close", position="footer")
        msg = "Bot Settings\nChoose an area to view or edit."
    elif edit_type is not None:
        if edit_type == "botvar":
            label = get_setting_label(key)
            value = Config.get(key)
            source = get_config_source(key, value)
            msg = ""
            if is_bool_setting(key):
                buttons.data_button("Enable", f"botset boolvar {key} true")
                buttons.data_button("Disable", f"botset boolvar {key} false")
            if key not in ["TELEGRAM_HASH", "TELEGRAM_API", "OWNER_ID", "BOT_TOKEN"]:
                buttons.data_button("Default", f"botset resetvar {key}")
            buttons.data_button("Back", "botset var", position="footer")
            buttons.data_button("Close", "botset close", position="footer")
            if key in [
                "CMD_SUFFIX",
                "OWNER_ID",
                "USER_SESSION_STRING",
                "TELEGRAM_HASH",
                "TELEGRAM_API",
                "BOT_TOKEN",
                "TG_PROXY",
            ]:
                msg += "Restart required for this edit to take effect! You will not see the changes in bot vars, the edit will be in database only!\n\n"
            msg += (
                f"<b>{label}</b>\n"
                f"Now: <code>{summarize_value(value)}</code>\n"
                f"Using: <b>{source}</b>\n"
                f"{get_edit_instructions(key)}"
            )
        elif edit_type == "ariavar":
            label = "Add New Aria2c Option" if key == "newkey" else get_setting_label(key)
            buttons.data_button("Back", "botset aria", position="footer")
            if key != "newkey":
                buttons.data_button("Empty String", f"botset emptyaria {key}")
            buttons.data_button("Close", "botset close", position="footer")
            msg = (
                f"<b>{label}</b>\nSend a key with value.\nExample: https-proxy-user:value\nTimeout: 60 sec"
                if key == "newkey"
                else (
                    f"<b>{label}</b>\n"
                    f"Current value: <code>{summarize_value(aria2_options[key])}</code>\n"
                    "Send a new value. Timeout: 60 sec"
                )
            )
        elif edit_type == "qbitvar":
            buttons.data_button("Back", "botset qbit", position="footer")
            buttons.data_button("Empty String", f"botset emptyqbit {key}")
            buttons.data_button("Close", "botset close", position="footer")
            msg = (
                f"<b>{get_setting_label(key)}</b>\n"
                f"Current value: <code>{summarize_value(qbit_options[key])}</code>\n"
                "Send a new value. Timeout: 60 sec"
            )
        elif edit_type == "nzbvar":
            buttons.data_button("Back", "botset nzb", position="footer")
            buttons.data_button("Default", f"botset resetnzb {key}")
            buttons.data_button("Empty String", f"botset emptynzb {key}")
            buttons.data_button("Close", "botset close", position="footer")
            msg = (
                f"<b>{get_setting_label(key)}</b>\n"
                f"Current value: <code>{summarize_value(nzb_options[key])}</code>\n"
                "If the value is a list, separate entries with spaces or commas.\n"
                "Example: .exe,info or .exe .info\n"
                "Timeout: 60 sec"
            )
        elif edit_type.startswith("nzbsevar"):
            index = 0 if key == "newser" else int(edit_type.replace("nzbsevar", ""))
            if key == "newser":
                buttons.data_button("Back", "botset nzbserver", position="footer")
                msg = "Send one server as dictionary {}, like in config.py without []. Timeout: 60 sec"
            else:
                buttons.data_button("Empty", f"botset emptyserkey {index} {key}")
                buttons.data_button("Back", f"botset nzbser{index}", position="footer")
                msg = f"Send a valid value for {key} in server {Config.USENET_SERVERS[index]['name']}. Current value is {Config.USENET_SERVERS[index][key]}. Timeout: 60 sec"
            buttons.data_button("Close", "botset close", position="footer")
    elif key == "var":
        conf_dict = Config.get_all()
        for k in list(conf_dict.keys())[start : 10 + start]:
            if k in ["DATABASE_URL", "DATABASE_NAME"] and state != "view":
                continue
            buttons.data_button(get_setting_label(k), f"botset botvar {k}")
        if state == "view":
            buttons.data_button("Edit", "botset edit var", position="header")
        else:
            buttons.data_button("View", "botset view var", position="header")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        for x in range(0, len(conf_dict), 10):
            buttons.data_button(
                f"{int(x / 10)}", f"botset start var {x}", position="footer"
            )
        msg = f"Bot Defaults | Page: {int(start / 10)} | State: {state}"
    elif key == "private":
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        msg = """Send private file: config.py, token.pickle, rclone.conf, accounts.zip, list_drives.txt, cookies.txt, .netrc or any other private file!
To delete private file send only the file name as text message.
Note: Changing .netrc will not take effect for aria2c until restart.
Timeout: 60 sec"""
    elif key == "aria":
        for k in list(aria2_options.keys())[start : 10 + start]:
            if k not in ["checksum", "index-out", "out", "pause", "select-file"]:
                buttons.data_button(get_setting_label(k), f"botset ariavar {k}")
        if state == "view":
            buttons.data_button("Edit", "botset edit aria", position="header")
        else:
            buttons.data_button("View", "botset view aria", position="header")
        buttons.data_button("Add new key", "botset ariavar newkey", position="header")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        for x in range(0, len(aria2_options), 10):
            buttons.data_button(
                f"{int(x / 10)}", f"botset start aria {x}", position="footer"
            )
        msg = f"Aria2c Options | Page: {int(start / 10)} | State: {state}"
    elif key == "qbit":
        for k in list(qbit_options.keys())[start : 10 + start]:
            buttons.data_button(get_setting_label(k), f"botset qbitvar {k}")
        if state == "view":
            buttons.data_button("Edit", "botset edit qbit", position="header")
        else:
            buttons.data_button("View", "botset view qbit", position="header")
        buttons.data_button("Sync Qbittorrent", "botset syncqbit", position="header")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        for x in range(0, len(qbit_options), 10):
            buttons.data_button(
                f"{int(x / 10)}", f"botset start qbit {x}", position="footer"
            )
        msg = f"Qbittorrent Options | Page: {int(start / 10)} | State: {state}"
    elif key == "nzb":
        for k in list(nzb_options.keys())[start : 10 + start]:
            buttons.data_button(get_setting_label(k), f"botset nzbvar {k}")
        if state == "view":
            buttons.data_button("Edit", "botset edit nzb", position="header")
        else:
            buttons.data_button("View", "botset view nzb", position="header")
        buttons.data_button("Servers", "botset nzbserver", position="header")
        buttons.data_button("Sync Sabnzbd", "botset syncnzb", position="header")
        buttons.data_button("Back", "botset back", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        for x in range(0, len(nzb_options), 10):
            buttons.data_button(
                f"{int(x / 10)}", f"botset start nzb {x}", position="footer"
            )
        msg = f"Sabnzbd Options | Page: {int(start / 10)} | State: {state}"
    elif key == "nzbserver":
        if len(Config.USENET_SERVERS) > 0:
            for index, k in enumerate(Config.USENET_SERVERS[start : 10 + start]):
                buttons.data_button(k["name"], f"botset nzbser{index}")
        buttons.data_button("Add New", "botset nzbsevar newser", position="header")
        buttons.data_button("Back", "botset nzb", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        if len(Config.USENET_SERVERS) > 10:
            for x in range(0, len(Config.USENET_SERVERS), 10):
                buttons.data_button(
                    f"{int(x / 10)}", f"botset start nzbser {x}", position="footer"
                )
        msg = f"Usenet Servers | Page: {int(start / 10)} | State: {state}"
    elif key.startswith("nzbser"):
        index = int(key.replace("nzbser", ""))
        for k in list(Config.USENET_SERVERS[index].keys())[start : 10 + start]:
            buttons.data_button(get_setting_label(k), f"botset nzbsevar{index} {k}")
        if state == "view":
            buttons.data_button("Edit", f"botset edit {key}", position="header")
        else:
            buttons.data_button("View", f"botset view {key}", position="header")
        buttons.data_button("Remove Server", f"botset remser {index}", position="header")
        buttons.data_button("Back", "botset nzbserver", position="footer")
        buttons.data_button("Close", "botset close", position="footer")
        if len(Config.USENET_SERVERS[index].keys()) > 10:
            for x in range(0, len(Config.USENET_SERVERS[index]), 10):
                buttons.data_button(
                    f"{int(x / 10)}", f"botset start {key} {x}", position="footer"
                )
        msg = f"Server Keys | Page: {int(start / 10)} | State: {state}"

    button = (
        buttons.build_menu(1, h_cols=2, f_cols=2)
        if key is None
        else buttons.build_menu(2, h_cols=2, f_cols=2)
    )
    return msg, button


async def update_buttons(message, key=None, edit_type=None):
    msg, button = await get_buttons(key, edit_type)
    await edit_message(message, msg, button)


async def save_bot_variable(pre_message, key, value):
    text_value = value if isinstance(value, str) else None
    if text_value and text_value.lower() == "true":
        value = True
    elif text_value and text_value.lower() == "false":
        value = False
        if key == "INCOMPLETE_TASK_NOTIFIER" and Config.DATABASE_URL:
            await database.trunc_table("tasks")
    elif key == "STATUS_UPDATE_INTERVAL":
        value = int(value)
        if len(task_dict) != 0 and (st := intervals["status"]):
            for cid, intvl in list(st.items()):
                intvl.cancel()
                intervals["status"][cid] = SetInterval(
                    value, update_status_message, cid
                )
    elif key == "TORRENT_TIMEOUT":
        await TorrentManager.change_aria2_option("bt-stop-timeout", value)
        value = int(value)
    elif key == "LEECH_SPLIT_SIZE":
        value = min(int(value), TgClient.MAX_SPLIT_SIZE)
    elif key == "BASE_URL_PORT":
        value = int(value)
        if Config.BASE_URL:
            await (await create_subprocess_exec("pkill", "-9", "-f", "gunicorn")).wait()
            await create_subprocess_shell(
                f"gunicorn -k uvicorn.workers.UvicornWorker -w 1 web.wserver:app --bind 0.0.0.0:{value}"
            )
    elif key == "EXCLUDED_EXTENSIONS":
        fx = value.split()
        excluded_extensions.clear()
        excluded_extensions.extend(["aria2", "!qB"])
        for x in fx:
            x = x.lstrip(".")
            excluded_extensions.append(x.strip().lower())
    elif key == "INCLUDED_EXTENSIONS":
        fx = value.split()
        included_extensions.clear()
        for x in fx:
            x = x.lstrip(".")
            included_extensions.append(x.strip().lower())
    elif key == "GDRIVE_ID":
        if drives_names and drives_names[0] == "Main":
            drives_ids[0] = value
        else:
            drives_ids.insert(0, value)
    elif key == "INDEX_URL":
        if drives_names and drives_names[0] == "Main":
            index_urls[0] = value
        else:
            index_urls.insert(0, value)
    elif key == "AUTHORIZED_CHATS":
        aid = value.split()
        auth_chats.clear()
        for id_ in aid:
            chat_id, *thread_ids = id_.split("|")
            chat_id = int(chat_id.strip())
            if thread_ids:
                thread_ids = list(map(lambda x: int(x.strip()), thread_ids))
                auth_chats[chat_id] = thread_ids
            else:
                auth_chats[chat_id] = []
    elif key == "SUDO_USERS":
        sudo_users.clear()
        aid = value.split()
        for id_ in aid:
            sudo_users.append(int(id_.strip()))
    elif isinstance(value, str) and value.isdigit():
        value = int(value)
    elif isinstance(value, str) and value.startswith("[") and value.endswith("]"):
        value = literal_eval(value)
    elif isinstance(value, str) and value.startswith("{") and value.endswith("}"):
        value = literal_eval(value)
    Config.set(key, value)
    await update_buttons(pre_message, "var")
    await database.update_config({key: value})
    if key in ["SEARCH_PLUGINS", "SEARCH_API_LINK"]:
        await initiate_search_tools()
    elif key in ["QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD"]:
        await start_from_queued()
    elif key in [
        "RCLONE_SERVE_URL",
        "RCLONE_SERVE_PORT",
        "RCLONE_SERVE_USER",
        "RCLONE_SERVE_PASS",
    ]:
        await rclone_serve_booter()
    elif key in ["JD_EMAIL", "JD_PASS"]:
        await jdownloader.boot()
    elif key == "RSS_DELAY":
        add_job()
    elif key == "USET_SERVERS":
        for s in value:
            await sabnzbd_client.set_special_config("servers", s)


@new_task
async def edit_variable(_, message, pre_message, key):
    handler_dict[message.chat.id] = False
    try:
        value = validate_bot_variable(key, message.text)
    except ValueError as e:
        await send_message(message, str(e))
        return
    await delete_message(message)
    await save_bot_variable(pre_message, key, value)


@new_task
async def edit_aria(_, message, pre_message, key):
    handler_dict[message.chat.id] = False
    value = str(message.text)
    if key == "newkey":
        if ":" not in value:
            await send_message(message, "Use key:value format.")
            return
        key, value = [x.strip() for x in value.split(":", 1)]
    elif value.lower() == "true":
        value = "true"
    elif value.lower() == "false":
        value = "false"
    await TorrentManager.change_aria2_option(key, value)
    await update_buttons(pre_message, "aria")
    await delete_message(message)
    await database.update_aria2(key, value)


@new_task
async def edit_qbit(_, message, pre_message, key):
    handler_dict[message.chat.id] = False
    value = str(message.text)
    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    elif key == "max_ratio":
        value = float(value)
    elif value.isdigit():
        value = int(value)
    await TorrentManager.qbittorrent.app.set_preferences({key: value})
    qbit_options[key] = value
    await update_buttons(pre_message, "qbit")
    await delete_message(message)
    await database.update_qbittorrent(key, value)


@new_task
async def edit_nzb(_, message, pre_message, key):
    handler_dict[message.chat.id] = False
    value = str(message.text)
    if value.isdigit():
        value = int(value)
    elif value.startswith("[") and value.endswith("]"):
        try:
            value = ",".join(literal_eval(value))
        except Exception as e:
            LOGGER.error(e)
            await send_message(message, f"{get_setting_label(key)} must be a valid list.")
            await update_buttons(pre_message, "nzb")
            return
    res = await sabnzbd_client.set_config("misc", key, value)
    nzb_options[key] = res["config"]["misc"][key]
    await update_buttons(pre_message, "nzb")
    await delete_message(message)
    await database.update_nzb_config()


@new_task
async def edit_nzb_server(_, message, pre_message, key, index=0):
    handler_dict[message.chat.id] = False
    value = str(message.text)
    if key == "newser":
        if value.startswith("{") and value.endswith("}"):
            try:
                value = literal_eval(value)
            except:
                await send_message(message, "Invalid dict format!")
                await update_buttons(pre_message, "nzbserver")
                return
            res = await sabnzbd_client.add_server(value)
            if not res["config"]["servers"][0]["host"]:
                await send_message(message, "Invalid server!")
                await update_buttons(pre_message, "nzbserver")
                return
            Config.USENET_SERVERS.append(value)
            await update_buttons(pre_message, "nzbserver")
        else:
            await send_message(message, "Invalid dict format!")
            await update_buttons(pre_message, "nzbserver")
            return
    else:
        if value.isdigit():
            value = int(value)
        res = await sabnzbd_client.add_server(
            {"name": Config.USENET_SERVERS[index]["name"], key: value}
        )
        if res["config"]["servers"][0][key] == "":
            await send_message(message, "Invalid value")
            return
        Config.USENET_SERVERS[index][key] = value
        await update_buttons(pre_message, f"nzbser{index}")
    await delete_message(message)
    await database.update_config({"USENET_SERVERS": Config.USENET_SERVERS})


async def sync_jdownloader():
    async with jd_listener_lock:
        if not Config.DATABASE_URL or not jdownloader.is_connected:
            return
        await jdownloader.device.system.exit_jd()
    if await aiopath.exists("cfg.zip"):
        await remove("cfg.zip")
    await (
        await create_subprocess_exec("7z", "a", "cfg.zip", "/JDownloader/cfg")
    ).wait()
    await database.update_private_file("cfg.zip")


@new_task
async def update_private_file(_, message, pre_message):
    handler_dict[message.chat.id] = False
    if not message.media and (file_name := str(message.text)):
        if await aiopath.isfile(file_name) and file_name != "config.py":
            await remove(file_name)
        if file_name == "accounts.zip":
            if await aiopath.exists("accounts"):
                await rmtree("accounts", ignore_errors=True)
            if await aiopath.exists("rclone_sa"):
                await rmtree("rclone_sa", ignore_errors=True)
            Config.USE_SERVICE_ACCOUNTS = False
            await database.update_config({"USE_SERVICE_ACCOUNTS": False})
        elif file_name in {".netrc", "netrc"}:
            await (await create_subprocess_exec("touch", ".netrc")).wait()
            await (await create_subprocess_exec("chmod", "600", ".netrc")).wait()
            await (await create_subprocess_exec("cp", ".netrc", "/root/.netrc")).wait()
        await delete_message(message)
    elif doc := message.document:
        file_name = doc.file_name
        fpath = f"{getcwd()}/{file_name}"
        if await aiopath.exists(fpath):
            await remove(fpath)
        await message.download(file_name=fpath)
        if file_name == "accounts.zip":
            if await aiopath.exists("accounts"):
                await rmtree("accounts", ignore_errors=True)
            if await aiopath.exists("rclone_sa"):
                await rmtree("rclone_sa", ignore_errors=True)
            await (
                await create_subprocess_exec(
                    "7z", "x", "-o.", "-aoa", "accounts.zip", "accounts/*.json"
                )
            ).wait()
            await (
                await create_subprocess_exec("chmod", "-R", "777", "accounts")
            ).wait()
        elif file_name == "list_drives.txt":
            drives_ids.clear()
            drives_names.clear()
            index_urls.clear()
            if Config.GDRIVE_ID:
                drives_names.append("Main")
                drives_ids.append(Config.GDRIVE_ID)
                index_urls.append(Config.INDEX_URL)
            async with aiopen("list_drives.txt", "r+") as f:
                lines = await f.readlines()
                for line in lines:
                    temp = line.strip().split()
                    drives_ids.append(temp[1])
                    drives_names.append(temp[0].replace("_", " "))
                    if len(temp) > 2:
                        index_urls.append(temp[2])
                    else:
                        index_urls.append("")
        elif file_name in [".netrc", "netrc"]:
            if file_name == "netrc":
                await rename("netrc", ".netrc")
                file_name = ".netrc"
            await (await create_subprocess_exec("chmod", "600", ".netrc")).wait()
            await (await create_subprocess_exec("cp", ".netrc", "/root/.netrc")).wait()
        elif file_name == "config.py":
            await load_config()
        if "@github.com" in Config.UPSTREAM_REPO:
            buttons = ButtonMaker()
            msg = "Push to UPSTREAM_REPO ?"
            buttons.data_button("Yes!", f"botset push {file_name}")
            buttons.data_button("No", "botset close")
            await send_message(message, msg, buttons.build_menu(2))
        else:
            await delete_message(message)
    if file_name == "rclone.conf":
        await rclone_serve_booter()
    await update_buttons(pre_message)
    await database.update_private_file(file_name)


async def event_handler(client, query, pfunc, rfunc, document=False):
    chat_id = query.message.chat.id
    handler_dict[chat_id] = True
    start_time = time()

    async def event_filter(_, __, event):
        user = event.from_user or event.sender_chat
        return bool(
            user.id == query.from_user.id
            and event.chat.id == chat_id
            and (event.text or event.document and document)
        )

    handler = client.add_handler(
        MessageHandler(pfunc, filters=create(event_filter)), group=-1
    )
    while handler_dict[chat_id]:
        await sleep(0.5)
        if time() - start_time > 60:
            handler_dict[chat_id] = False
            await rfunc()
    client.remove_handler(*handler)


@new_task
async def edit_bot_settings(client, query):
    data = query.data.split()
    message = query.message
    handler_dict[message.chat.id] = False
    if data[1] == "close":
        await query.answer()
        await delete_message(message.reply_to_message)
        await delete_message(message)
    elif data[1] == "back":
        await query.answer()
        globals()["start"] = 0
        await update_buttons(message, None)
    elif data[1] == "syncjd":
        if not Config.JD_EMAIL or not Config.JD_PASS:
            await query.answer(
                "No Email or Password provided!",
                show_alert=True,
            )
            return
        await query.answer(
            "Synchronization Started. JDownloader will get restarted. It takes up to 10 sec!",
            show_alert=True,
        )
        await sync_jdownloader()
    elif data[1] in ["var", "aria", "qbit", "nzb", "nzbserver"] or data[1].startswith(
        "nzbser"
    ):
        if data[1] == "nzbserver":
            globals()["start"] = 0
        await query.answer()
        await update_buttons(message, data[1])
    elif data[1] == "resetvar":
        await query.answer()
        expected_type = type(getattr(Config, data[2]))
        if expected_type == bool:
            value = False
        elif expected_type == int:
            value = 0
        elif expected_type == str:
            value = ""
        elif expected_type == list:
            value = []
        elif expected_type == dict:
            value = {}
        if data[2] in DEFAULT_VALUES:
            value = DEFAULT_VALUES[data[2]]
            if (
                data[2] == "STATUS_UPDATE_INTERVAL"
                and len(task_dict) != 0
                and (st := intervals["status"])
            ):
                for key, intvl in list(st.items()):
                    intvl.cancel()
                    intervals["status"][key] = SetInterval(
                        value, update_status_message, key
                    )
        elif data[2] == "EXCLUDED_EXTENSIONS":
            excluded_extensions.clear()
            excluded_extensions.extend(["aria2", "!qB"])
        elif data[2] == "INCLUDED_EXTENSIONS":
            included_extensions.clear()
        elif data[2] == "TORRENT_TIMEOUT":
            await TorrentManager.change_aria2_option("bt-stop-timeout", "0")
            await database.update_aria2("bt-stop-timeout", "0")
        elif data[2] == "BASE_URL":
            await (await create_subprocess_exec("pkill", "-9", "-f", "gunicorn")).wait()
        elif data[2] == "BASE_URL_PORT":
            value = 80
            if Config.BASE_URL:
                await (
                    await create_subprocess_exec("pkill", "-9", "-f", "gunicorn")
                ).wait()
                await create_subprocess_shell(
                    f"gunicorn -k uvicorn.workers.UvicornWorker -w 1 web.wserver:app --bind 0.0.0.0:{value}"
                )
        elif data[2] == "GDRIVE_ID":
            if drives_names and drives_names[0] == "Main":
                drives_names.pop(0)
                drives_ids.pop(0)
                index_urls.pop(0)
        elif data[2] == "INDEX_URL":
            if drives_names and drives_names[0] == "Main":
                index_urls[0] = ""
        elif data[2] == "INCOMPLETE_TASK_NOTIFIER":
            await database.trunc_table("tasks")
        elif data[2] in ["JD_EMAIL", "JD_PASS"]:
            await create_subprocess_exec("pkill", "-9", "-f", "java")
        elif data[2] == "USENET_SERVERS":
            for s in Config.USENET_SERVERS:
                await sabnzbd_client.delete_config("servers", s["name"])
        elif data[2] == "AUTHORIZED_CHATS":
            auth_chats.clear()
        elif data[2] == "SUDO_USERS":
            sudo_users.clear()
        Config.set(data[2], value)
        await update_buttons(message, "var")
        if data[2] == "DATABASE_URL":
            await database.disconnect()
        await database.update_config({data[2]: value})
        if data[2] in ["SEARCH_PLUGINS", "SEARCH_API_LINK"]:
            await initiate_search_tools()
        elif data[2] in ["QUEUE_ALL", "QUEUE_DOWNLOAD", "QUEUE_UPLOAD"]:
            await start_from_queued()
        elif data[2] in [
            "RCLONE_SERVE_URL",
            "RCLONE_SERVE_PORT",
            "RCLONE_SERVE_USER",
            "RCLONE_SERVE_PASS",
        ]:
            await rclone_serve_booter()
    elif data[1] == "resetnzb":
        await query.answer()
        res = await sabnzbd_client.set_config_default(data[2])
        nzb_options[data[2]] = res["config"]["misc"][data[2]]
        await update_buttons(message, "nzb")
        await database.update_nzb_config()
    elif data[1] == "syncnzb":
        await query.answer(
            "Synchronization Started. It takes up to 2 sec!", show_alert=True
        )
        nzb_options.clear()
        await update_nzb_options()
        await database.update_nzb_config()
    elif data[1] == "syncqbit":
        await query.answer(
            "Synchronization Started. It takes up to 2 sec!", show_alert=True
        )
        qbit_options.clear()
        await update_qb_options()
        await database.save_qbit_settings()
    elif data[1] == "emptyaria":
        await query.answer()
        aria2_options[data[2]] = ""
        await update_buttons(message, "aria")
        await TorrentManager.change_aria2_option(data[2], "")
        await database.update_aria2(data[2], "")
    elif data[1] == "emptyqbit":
        await query.answer()
        await TorrentManager.qbittorrent.app.set_preferences({data[2]: value})
        qbit_options[data[2]] = ""
        await update_buttons(message, "qbit")
        await database.update_qbittorrent(data[2], "")
    elif data[1] == "emptynzb":
        await query.answer()
        res = await sabnzbd_client.set_config("misc", data[2], "")
        nzb_options[data[2]] = res["config"]["misc"][data[2]]
        await update_buttons(message, "nzb")
        await database.update_nzb_config()
    elif data[1] == "remser":
        index = int(data[2])
        await sabnzbd_client.delete_config(
            "servers", Config.USENET_SERVERS[index]["name"]
        )
        del Config.USENET_SERVERS[index]
        await update_buttons(message, "nzbserver")
        await database.update_config({"USENET_SERVERS": Config.USENET_SERVERS})
    elif data[1] == "private":
        await query.answer()
        await update_buttons(message, data[1])
        pfunc = partial(update_private_file, pre_message=message)
        rfunc = partial(update_buttons, message)
        await event_handler(client, query, pfunc, rfunc, True)
    elif data[1] == "boolvar":
        await query.answer(
            f"{get_setting_label(data[2])} {'enabled' if data[3] == 'true' else 'disabled'}.",
            show_alert=True,
        )
        await save_bot_variable(message, data[2], data[3])
    elif data[1] == "botvar" and state == "edit":
        await query.answer()
        await update_buttons(message, data[2], data[1])
        if is_bool_setting(data[2]):
            return
        pfunc = partial(edit_variable, pre_message=message, key=data[2])
        rfunc = partial(update_buttons, message, "var")
        await event_handler(client, query, pfunc, rfunc)
    elif data[1] == "botvar" and state == "view":
        current_value = Config.get(data[2])
        value = f"{current_value}"
        if len(value) > 200:
            await query.answer()
            with BytesIO(str.encode(value)) as out_file:
                out_file.name = f"{data[2]}.txt"
                await send_file(message, out_file)
            return
        await query.answer(
            f"{get_setting_label(data[2])}: {summarize_value(current_value)} ({get_config_source(data[2], current_value)})",
            show_alert=True,
        )
    elif data[1] == "ariavar" and (state == "edit" or data[2] == "newkey"):
        await query.answer()
        await update_buttons(message, data[2], data[1])
        pfunc = partial(edit_aria, pre_message=message, key=data[2])
        rfunc = partial(update_buttons, message, "aria")
        await event_handler(client, query, pfunc, rfunc)
    elif data[1] == "ariavar" and state == "view":
        current_value = aria2_options[data[2]]
        value = f"{current_value}"
        if len(value) > 200:
            await query.answer()
            with BytesIO(str.encode(value)) as out_file:
                out_file.name = f"{data[2]}.txt"
                await send_file(message, out_file)
            return
        await query.answer(
            f"{get_setting_label(data[2])}: {summarize_value(current_value)}",
            show_alert=True,
        )
    elif data[1] == "qbitvar" and state == "edit":
        await query.answer()
        await update_buttons(message, data[2], data[1])
        pfunc = partial(edit_qbit, pre_message=message, key=data[2])
        rfunc = partial(update_buttons, message, "qbit")
        await event_handler(client, query, pfunc, rfunc)
    elif data[1] == "qbitvar" and state == "view":
        current_value = qbit_options[data[2]]
        value = f"{current_value}"
        if len(value) > 200:
            await query.answer()
            with BytesIO(str.encode(value)) as out_file:
                out_file.name = f"{data[2]}.txt"
                await send_file(message, out_file)
            return
        await query.answer(
            f"{get_setting_label(data[2])}: {summarize_value(current_value)}",
            show_alert=True,
        )
    elif data[1] == "nzbvar" and state == "edit":
        await query.answer()
        await update_buttons(message, data[2], data[1])
        pfunc = partial(edit_nzb, pre_message=message, key=data[2])
        rfunc = partial(update_buttons, message, "nzb")
        await event_handler(client, query, pfunc, rfunc)
    elif data[1] == "nzbvar" and state == "view":
        current_value = nzb_options[data[2]]
        value = f"{current_value}"
        if len(value) > 200:
            await query.answer()
            with BytesIO(str.encode(value)) as out_file:
                out_file.name = f"{data[2]}.txt"
                await send_file(message, out_file)
            return
        await query.answer(
            f"{get_setting_label(data[2])}: {summarize_value(current_value)}",
            show_alert=True,
        )
    elif data[1] == "emptyserkey":
        await query.answer()
        await update_buttons(message, f"nzbser{data[2]}")
        index = int(data[2])
        res = await sabnzbd_client.add_server(
            {"name": Config.USENET_SERVERS[index]["name"], data[3]: ""}
        )
        Config.USENET_SERVERS[index][data[3]] = res["config"]["servers"][0][data[3]]
        await database.update_config({"USENET_SERVERS": Config.USENET_SERVERS})
    elif data[1].startswith("nzbsevar") and (state == "edit" or data[2] == "newser"):
        index = 0 if data[2] == "newser" else int(data[1].replace("nzbsevar", ""))
        await query.answer()
        await update_buttons(message, data[2], data[1])
        pfunc = partial(edit_nzb_server, pre_message=message, key=data[2], index=index)
        rfunc = partial(
            update_buttons,
            message,
            f"nzbser{index}" if data[2] != "newser" else "nzbserver",
        )
        await event_handler(client, query, pfunc, rfunc)
    elif data[1].startswith("nzbsevar") and state == "view":
        index = int(data[1].replace("nzbsevar", ""))
        current_value = Config.USENET_SERVERS[index][data[2]]
        value = f"{current_value}"
        if len(value) > 200:
            await query.answer()
            with BytesIO(str.encode(value)) as out_file:
                out_file.name = f"{data[2]}.txt"
                await send_file(message, out_file)
            return
        await query.answer(
            f"{get_setting_label(data[2])}: {summarize_value(current_value)}",
            show_alert=True,
        )
    elif data[1] == "edit":
        await query.answer()
        globals()["state"] = "edit"
        await update_buttons(message, data[2])
    elif data[1] == "view":
        await query.answer()
        globals()["state"] = "view"
        await update_buttons(message, data[2])
    elif data[1] == "start":
        await query.answer()
        if start != int(data[3]):
            globals()["start"] = int(data[3])
            await update_buttons(message, data[2])
    elif data[1] == "push":
        await query.answer()
        filename = data[2].rsplit(".zip", 1)[0]
        if await aiopath.exists(filename):
            await (
                await create_subprocess_shell(
                    f"git add -f {filename} \
                    && git commit -sm botsettings -q \
                    && git push origin {Config.UPSTREAM_BRANCH} -qf"
                )
            ).wait()
        else:
            await (
                await create_subprocess_shell(
                    f"git rm -r --cached {filename} \
                    && git commit -sm botsettings -q \
                    && git push origin {Config.UPSTREAM_BRANCH} -qf"
                )
            ).wait()
        await delete_message(message.reply_to_message)
        await delete_message(message)


@new_task
async def send_bot_settings(_, message):
    handler_dict[message.chat.id] = False
    msg, button = await get_buttons()
    globals()["start"] = 0
    await send_message(message, msg, button)


async def load_config():
    Config.load()
    drives_ids.clear()
    drives_names.clear()
    index_urls.clear()
    await update_variables()

    if not await aiopath.exists("accounts"):
        Config.USE_SERVICE_ACCOUNTS = False

    if len(task_dict) != 0 and (st := intervals["status"]):
        for key, intvl in list(st.items()):
            intvl.cancel()
            intervals["status"][key] = SetInterval(
                Config.STATUS_UPDATE_INTERVAL, update_status_message, key
            )

    if Config.TORRENT_TIMEOUT:
        await TorrentManager.change_aria2_option(
            "bt-stop-timeout", f"{Config.TORRENT_TIMEOUT}"
        )
        await database.update_aria2("bt-stop-timeout", f"{Config.TORRENT_TIMEOUT}")

    if not Config.INCOMPLETE_TASK_NOTIFIER:
        await database.trunc_table("tasks")

    await (await create_subprocess_exec("pkill", "-9", "-f", "gunicorn")).wait()
    if Config.BASE_URL:
        await create_subprocess_shell(
            f"gunicorn -k uvicorn.workers.UvicornWorker -w 1 web.wserver:app --bind 0.0.0.0:{Config.BASE_URL_PORT}"
        )

    if Config.DATABASE_URL:
        await database.connect()
        config_dict = Config.get_all()
        await database.update_config(config_dict)
    else:
        await database.disconnect()
    await gather(initiate_search_tools(), start_from_queued(), rclone_serve_booter())
    add_job()
