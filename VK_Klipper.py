import asyncio
import aiofiles
import time
import os
from datetime import datetime


from vk_api_group import TeraVkBot
from moonraker_api_tera import TeraMoonrakerClient
from db_klipper import PeerConservationDB





MOONRAKER_CFG = "moonraker_group_vk.cfg"






def clear_c():
    if os.name == "nt":
        os.system("cls")

    else:
        os.system("clear")

clear_c()
clear_c()




DataMetrics = ""
JPG_Snapshot_Stream = bytes(b"")

async def VK_BOT_USERS(vk_bot, peer_db):
    global DataMetrics
    global JPG_Snapshot_Stream
    
    while True:
        db_check = await peer_db.is_empty()
        if db_check != True:
            async for key_peer_id in peer_db.get_all_peer_id():
                id_message = await peer_db.get_conserv_id(key_peer_id)
                
                
                if len(JPG_Snapshot_Stream) != 0 and str(JPG_Snapshot_Stream).find("Server Error") == -1:
                    JPGAttachFrame = await vk_bot.sendPhoto(key_peer_id, JPG_Snapshot_Stream)
        
                    await vk_bot.editMessage(key_peer_id, id_message, DataMetrics, AttachFrame)
        
                    print(f"[VK_BOT_USERS] VK ДАННЫЕ ОБНОВЛЕНЫ: {key_peer_id}\r\n" + f"[P] Длина изображения: {len(JPG_Snapshot_Stream)}")
        
                else:
                    await vk_bot.editMessage(key_peer_id, id_message, DataMetrics, "")
        
                    print(f"[VK_BOT_USERS] VK ДАННЫЕ ОБНОВЛЕНЫ: {key_peer_id}\r\n")
                    print("[VK_BOT_USERS] Отсутствует фото: ", JPG_Snapshot_Stream.decode(), "\r\n")
 
    
async def vk_message_handler(vk_bot, klipper_db, peer_id, text_message):
    global DataMetrics
    global JPG_Snapshot_Stream
    text_message_low = text_message.casefold()
    
    
    if text_message == "/start":
        if len(JPG_Snapshot_Stream) != 0 and str(JPG_Snapshot_Stream).find("Server Error") == -1:
            JPGAttachFrame = await vk_bot.sendPhoto(peer_id, JPG_Snapshot_Stream)
        
            id_new_message = await vk_bot.sendMessage(peer_id, DataMetrics, AttachFrame)
            await klipper_db.save_conserv_id(peer_id, id_new_message)
            
            print(f"[+] VK ПОЛЬЗОВАТЕЛЬ ДОБАВЛЕН: {peer_id}\r\n" + f"[P] Длина изображения: {len(JPG_Snapshot_Stream)}")
        
        else:
            id_new_message = await vk_bot.sendMessage(peer_id, DataMetrics, "")
            await klipper_db.save_conserv_id(peer_id, id_new_message)
         
            print(f"[+] VK ПОЛЬЗОВАТЕЛЬ ДОБАВЛЕН: {peer_id}\r\n")
            print("[-] Отсутствует фото: ", JPG_Snapshot_Stream.decode(), "\r\n")

    
    
async def VK_BOT_TERA():
    global MOONRAKER_CFG
    global DataMetrics
    global JPG_Snapshot_Stream
    
    VkBot = TeraVkBot(MOONRAKER_CFG)
    await VkBot.initClient()
    
    db_vk = PeerConservationDB("peer_id_DB.db")
    await db_vk.init()
    
    asyncio.create_task(VK_BOT_USERS(VkBot, db_vk))
    
    
    while True:
        
        
        
        async for user_message in VkBot.getMessages():
            PEER_ID = next(iter(user_message), None)
            
            asyncio.create_task(vk_message_handler(VkBot, db_vk, PEER_ID, user_message[PEER_ID]))
            await asyncio.sleep(0)
        
        
        await asyncio.sleep(0)

async def MOONRAKERBOT_TERA():
    global MOONRAKER_CFG
    global DataMetrics
    global JPG_Snapshot_Stream

    MoonrakerWork = TeraMoonrakerClient()
    MoonrakerWork.set_name_cfg(MOONRAKER_CFG)
    PROGRESS_PRC = int(MoonrakerWork._GetPARAM_CFG("PROGRESS_PRC"))

    
    if await MoonrakerWork.connect_moonraker() is not None:
        
        while True:
            await MoonrakerWork.update()
            
            Progress_Moonraker = MoonrakerWork.GetProgress()
            Progress_Bar = "[" + ("█" * int((Progress_Moonraker / 100) * PROGRESS_PRC)) + ("░" * int(PROGRESS_PRC - (Progress_Moonraker / 100) * PROGRESS_PRC)) + "]"
            
            struct_time_update = time.localtime()
            
            Hour = struct_time_update.tm_hour
            Minute = struct_time_update.tm_min
            Sec = struct_time_update.tm_sec
            
            DataMetrics = f"---VK-KLIPPER-BOT---\r\nLAST_UPDATE: {Hour}:{Minute}:{Sec}\r\n\r\nFile name: {MoonrakerWork.GetFileName_Printing()}\r\nStatus printing: {MoonrakerWork.GetStatusPrinter()}\r\nHeater bed: {MoonrakerWork.GetTemp_HeaterBed()}\r\nExtruder: {MoonrakerWork.GetTemp_Extruder()}\r\nПрогресс: {Progress_Moonraker}%\r\n{Progress_Bar}"
            JPG_Snapshot_Stream = MoonrakerWork.GetFrame_Camera()
            print("\033[2J\033[H" + DataMetrics)

async def Work_ClientBot():
    
    MoonrakerTera = asyncio.create_task(MOONRAKERBOT_TERA())
    VkTera = asyncio.create_task(VK_BOT_TERA())

    await asyncio.gather(MoonrakerTera, VkTera)
    
asyncio.run(Work_ClientBot())


