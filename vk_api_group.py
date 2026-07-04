import asyncio
import aiohttp
import aiofiles
import random
import datetime
import os
import sys


class TeraVkBot:
    def __init__(self, name_cfg):
        self.__cfg_name = name_cfg
        
        self.__ACCESS_TOKEN = self._GetPARAM_CFG("ACCESS_TOKEN")
        self.__ID_GROUP = self._GetPARAM_CFG("ID_GROUP_VK")
        
        self.__DEBUG = int(self._GetPARAM_CFG("DEBUG_VK"))
        self.__DEBUG_PATH =  self._GetPARAM_CFG("DEBUG_PATH_VK")
        
        self.__SERVER_LONGPOLL_BOT = ""
        self.__SERVER_KEY = ""
        self.__SERVER_TS = 0
        
        self.__ID_MESSAGE_LAST = -1
        self.__vk_request = aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl=False))

        self.__ATTACH_LAST_CACHEPHOTO = ""
        
        self.__REQUESTS_COUNT = 0
        self.__REQUESTS_LIMIT = 1
        self.__REQUESTS_TIME_DELAY = 7
        
        
        CreateLog_Deb = open(self.__DEBUG_PATH, "w")
        CreateLog_Deb.close()
    
    
    
    
    async def __requests_block_api(self):
        self.__REQUESTS_COUNT += 1
        
        if self.__REQUESTS_COUNT >= self.__REQUESTS_LIMIT:
            await asyncio.sleep(self.__REQUESTS_TIME_DELAY)
            self.__REQUESTS_COUNT = 0
            
    
    async def _WriteDEBUG(self, Data):
        async with aiofiles.open(self.__DEBUG_PATH, "a") as WriteLog:
            await WriteLog.write(f"{datetime.datetime.now()} {str(Data)}\r\n\r\n")
            await WriteLog.flush()
            
    def _GetPARAM_CFG(self, Param_name):
        Cfg_Handle = open(self.__cfg_name, "r")
    
        LineCfg = Cfg_Handle.readline()
    
        while LineCfg != "":
            if Param_name in LineCfg:
                Offset = LineCfg.find("=") + 1

                while LineCfg[Offset] == " ":
                    Offset += 1
            
                Cfg_Handle.close()
                return LineCfg[Offset:].strip()
        
            LineCfg = Cfg_Handle.readline()
        
        return None

    



    def _EditPARAM_CFG(self, Param_name, ValueParam):
        GetTemp_Value = self._GetPARAM_CFG(Param_name)
        DataCfg = ""
    
        with open(self.__cfg_name, "r") as CfgGet:
            DataCfg = CfgGet.read()
        
        if Param_name not in DataCfg:
            return None
            
        with open(self.__cfg_name, "w") as CfgEdit:
            CfgEdit.write(DataCfg.replace(Param_name + " = " + GetTemp_Value, Param_name + " = " + ValueParam))
    
   
        
    async def initClient(self):
        try:
            request_init = await self.__vk_request.post("https://api.vk.com/method/groups.getLongPollServer", data = {"group_id": self.__ID_GROUP, "access_token": self.__ACCESS_TOKEN, "v": "5.199"})
        
        
            if request_init.status == 200:
                jsonvk_init = await request_init.json()
        
                self.__SERVER_LONGPOLL_BOT = jsonvk_init["response"]["server"]
                self.__SERVER_KEY = jsonvk_init["response"]["key"]
                self.__SERVER_TS = int(jsonvk_init["response"]["ts"])
        
        except aiohttp.ClientConnectorError as error_connected:
            await self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            await self.initClient()
            
        except aiohttp.ClientResponseError as response_error:
            await self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            await self.initClient()
            

            
            
        except asyncio.TimeoutError:
            await self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)
            await self.initClient()
            
            
        except aiohttp.InvalidURL as url_error:
            await self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            await self.initClient()
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            await self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            await self.initClient()
            
            
        except OSError as os_error:
            await self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
            await self.initClient()


    
    async def getMessages(self):
        try:
            request_longpoll = await self.__vk_request.post(self.__SERVER_LONGPOLL_BOT, data = {"key": self.__SERVER_KEY, "ts": str(self.__SERVER_TS)})
        
        
            if request_longpoll.status == 200:
                json_longpoll = await request_longpoll.json()

                
                if "failed" in json_longpoll:
                    self.__SERVER_TS = int(json_longpoll["ts"]) + 1
                
                    request_longpoll = await self.__vk_request.post(self.__SERVER_LONGPOLL_BOT, data = {"key": self.__SERVER_KEY, "ts": str(self.__SERVER_TS)})

                if request_longpoll.status == 200:
                    json_longpoll = await request_longpoll.json()
                
                    if json_longpoll["updates"] != 0:
                        self.__SERVER_TS += len(json_longpoll["updates"])
                
                        for IterEvent in json_longpoll["updates"]:
                            all_messages = {}
                            if "message" in IterEvent["object"]:
                                all_messages[(str(IterEvent["object"]["message"]["peer_id"]))] = IterEvent["object"]["message"]["text"]
                                yield  all_messages
        
        except aiohttp.client_exceptions.ClientConnectionError as error_connection:
            await self._WriteDEBUG(error_connection)
            
            await asyncio.sleep(2)

            
        except aiohttp.client_exceptions.ServerDisconnectedError as disconnect:
            await self._WriteDEBUG(disconnect)
            
            await asyncio.sleep(2)

            
        except aiohttp.ClientConnectorError as error_connected:
            await self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            
        except aiohttp.ClientResponseError as response_error:
            await self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            

            
        except asyncio.TimeoutError:
            await self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)


        except aiohttp.InvalidURL as url_error:
            await self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            await self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            
            
        except OSError as os_error:
            await self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
    
    async def editMessage(self, peer_id, id_message, message_text, attachments = ""):
        try:
            if attachments == "":
                API_VK_RESP = await self.__vk_request.post("https://api.vk.com/method/messages.edit?v=5.199", data = {"peer_id": peer_id, "message": message_text, "message_id": id_message, "access_token": self.__ACCESS_TOKEN})
            
                if API_VK_RESP.status == 200:
                    json_out = await API_VK_RESP.json()
                    await self.__requests_block_api()
                    
                    if "response" in json_out:
                        if json_out["response"]:
                            return True
                
                        return False
                
                    return json_out["error"]["error_msg"]
                
            
            
            else:
                API_VK_RESP = await self.__vk_request.post("https://api.vk.com/method/messages.edit?v=5.199", data = {"peer_id": peer_id, "message": message_text, "attachment": attachments, "message_id": id_message, "access_token": self.__ACCESS_TOKEN})
            
                json_out = await API_VK_RESP.json()
                await self.__requests_block_api()
            
                if "response" in json_out:
                    if json_out["response"]:
                        return True
                
                    return False
                
                return json_out["error"]["error_msg"]
                
        except aiohttp.ClientConnectorError as error_connected:
            await self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            await self.editMessage()
            
        except aiohttp.ClientResponseError as response_error:
            await self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            await self.editMessage()
            

            
            
        except asyncio.TimeoutError:
            await self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)
            await self.editMessage()
            
            
        except aiohttp.InvalidURL as url_error:
            await self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            await self.editMessage()
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            await self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            await self.editMessage()
            
            
        except OSError as os_error:
            await self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
            await self.editMessage()
            
    async def sendMessage(self, peer_id, text_message, attachments = ""):
        try:
            if attachments == "":
                message_send = await self.__vk_request.post("https://api.vk.com/method/messages.send", data = {"user_id": peer_id, "random_id": str(random.randint(23567, 1000000)), "message": text_message, "access_token": self.__ACCESS_TOKEN, "v": "5.199"})
            
            
                json_id = await message_send.json()
                await self.__requests_block_api()
            
                return str(json_id["response"])
        
            else:
                message_send = await self.__vk_request.post("https://api.vk.com/method/messages.send", data = {"user_id": peer_id, "random_id": str(random.randint(23567, 1000000)), "message": text_message, "attachment": attachments, "access_token": self.__ACCESS_TOKEN, "v": "5.199"})
            
                json_id = await message_send.json()
                await self.__requests_block_api()
            
                return str(json_id["response"])
                
                
        except aiohttp.client_exceptions.ClientConnectionError as error_connect:
            await self._WriteDEBUG(error_connect)
            
            await asyncio.sleep(2)
            await self.sendMessage()
        except aiohttp.ClientConnectorError as error_connected:
            await self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            await self.sendMessage()
            
        except aiohttp.ClientResponseError as response_error:
            await self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            await self.sendMessage()
            

            
        except asyncio.TimeoutError:
            await self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)
            await self.sendMessage()
            
            
        except aiohttp.InvalidURL as url_error:
            await self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            await self.sendMessage()
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            await self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            await self.sendMessage()
            
            
        except OSError as os_error:
            await self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
            await self.sendMessage()
            
            
    async def sendPhoto(self, peer_id, PhotoBytes: bytes):
        if len(PhotoBytes) == 0:
            return self.__ATTACH_LAST_CACHEPHOTO
            
        
        while True:               
            ALBUM_ID = ""
            UPLOAD_URL = ""

        
            UploadServer_URL = await self.__vk_request.post("https://api.vk.com/method/photos.getMessagesUploadServer?v=5.275", data = {"peer_id": peer_id, "group_id": "0", "upload_v2": "1", "access_token": self.__ACCESS_TOKEN})
            
            JsonUploadServer_URL = await UploadServer_URL.json()
            await self.__requests_block_api()
            
            await self._WriteDEBUG(JsonUploadServer_URL)
        
            if "upload_url" in JsonUploadServer_URL["response"]:
                UPLOAD_URL = JsonUploadServer_URL["response"]["upload_url"]
                ALBUM_ID = JsonUploadServer_URL["response"]["album_id"]

            

                FormPhoto_Data = aiohttp.FormData()
                FormPhoto_Data.add_field('file1', PhotoBytes, filename='temp.jpg', content_type='image/jpeg')
            
            
                UploadPhoto = await self.__vk_request.post(UPLOAD_URL, data = FormPhoto_Data)
                
                JsonUpload = await UploadPhoto.text()
                await self._WriteDEBUG(JsonUpload)
                await self.__requests_block_api()
                
            
                if "error" in JsonUpload:
                    CountTry = 0
                    while "error" in JsonUpload and CountTry < 3:
                        CountTry += 1
                        await self._WriteDEBUG(f"{datetime.datetime.now()} JsonUpload")
                    
                        FormPhoto_Try = aiohttp.FormData()
                        FormPhoto_Try.add_field('file1', PhotoBytes, filename='temp.jpg', content_type='image/jpeg') 
                        UploadPhoto = await self.__vk_request.post(UPLOAD_URL, data = FormPhoto_Try)
                        
                        JsonUpload = await UploadPhoto.text()
                        await self.__requests_block_api()
                        
                    if CountTry == 2:
                        return self.__ATTACH_LAST_CACHEPHOTO 
                    
 
            
                SavePHOTO = await self.__vk_request.post("https://api.vk.com/method/photos.saveMessagesPhoto?v=5.275", data = {"upload_v2": "1", "photo": JsonUpload, "group_id": "0", "access_token": self.__ACCESS_TOKEN})
                
                JsonSave = await SavePHOTO.json()
                await self._WriteDEBUG(JsonSave)
                
                await self.__requests_block_api()
                
                if "error" in JsonSave:
                    while JsonSave["error"]["error_msg"] == "Internal server error":
                        SavePHOTO = await self.__vk_request.post("https://api.vk.com/method/photos.saveMessagesPhoto?v=5.275", data = {"upload_v2": "1", "photo": JsonUpload, "group_id": "0", "access_token": self.__ACCESS_TOKEN})
                    
                        JsonSave = await SavePHOTO.json()
                        await self._WriteDEBUG(JsonSave)
                        
                        await self.__requests_block_api()

                    self.__ATTACH_LAST_CACHEPHOTO = f'photo{str(JsonSave["response"][0]["owner_id"])}_{str(JsonSave["response"][0]["id"])}_{str(JsonSave["response"][0]["access_key"])}'
                    return self.__ATTACH_LAST_CACHEPHOTO  
                    
                    
                else:
                    self.__ATTACH_LAST_CACHEPHOTO = f'photo{str(JsonSave["response"][0]["owner_id"])}_{str(JsonSave["response"][0]["id"])}_{str(JsonSave["response"][0]["access_key"])}'
                    return self.__ATTACH_LAST_CACHEPHOTO
