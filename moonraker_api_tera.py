import aiohttp
import asyncio
import aiofiles
import os
import sys
import datetime



class TeraMoonrakerClient:
    def __init__(self):
        self.__Extruder_Temperature = 0
        self.__Heater_Bed_Temperature = 0
        self.__Progress = 0.0
        self.__STATUS_PRINTER = ""
        self.__PRINTER_DOP_STATUS = ""
        
        self.__FILENAME = ""
        
        self.__DEBUG_MODE = 0
        self.__DEBUG_PATH = ""
        self.__cfg_name = ""
        
        self.__is_token_required = True
        self.__WB_API_MOONRAKER = None
        self.__MoonrakerSess = aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl=False))
        self.__MoonrakerWebSocket = aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl=False))
        self.__MoonrakerStream = aiohttp.ClientSession(connector = aiohttp.TCPConnector(ssl=False, force_close = True, enable_cleanup_closed = True))
        
        self.__FRAME_JPEG = bytes(b"")
        
        self.__IP_SERVER_MOONRAKER = ""
        self.__LOGIN_MOONRAKER = ""
        self.__PASSWORD_MOONRAKER = ""
        
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
            print("[] Параметр", Param_name, "отсутствует")
            sys.exit(1)
            
        with open(self.__cfg_name, "w") as CfgEdit:
            CfgEdit.write(DataCfg.replace(Param_name + " = " + GetTemp_Value, Param_name + " = " + ValueParam))
       
        
        
        
        
            
    async def _GetAuthBearer_Token(self):
        try:
            Auth_Data = await self.__MoonrakerSess.post(f"http://{self.__IP_SERVER_MOONRAKER}/access/login", data = {"username": self.__LOGIN_MOONRAKER, "password": self.__PASSWORD_MOONRAKER,"source":"moonraker"})
        
            if Auth_Data.status == 200:
                AuthJson = await Auth_Data.json()
    
                return AuthJson["result"]["token"]
            
            else:
                self.__WriteDEBUG(f"Ошибка запроса в Moonraker API: {Auth_Data.status}")
        
        except aiohttp.ClientConnectorError as error_connected:
            self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
        except aiohttp.ClientResponseError as response_error:
            self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
        except aiohttp.ClientTimeout as timeout_error:
            self._WriteDEBUG(timeout_error)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
            
        except asyncio.TimeoutError:
            self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
            
        except aiohttp.InvalidURL as url_error:
            self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            
            
        except OSError as os_error:
            self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
            await self._GetAuthBearer_Token()
            

    async def _Get_WebSocket_Token(self):
        try:
            TokenWeb_SOCKET = await self.__MoonrakerSess.get(f"http://{self.__IP_SERVER_MOONRAKER}/access/oneshot_token", headers = {"Authorization": f"Bearer {await self._GetAuthBearer_Token()}"})
        
            if TokenWeb_SOCKET.status == 200:
                JsonConv = await TokenWeb_SOCKET.json()
    
                return JsonConv["result"]
            
            else:
                self.__WriteDEBUG(f"[] Moonraker API: {TokenWeb_SOCKET.status}")
                
                
        except aiohttp.ClientConnectorError as error_connected:
            self._WriteDEBUG(error_connected)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
        except aiohttp.ClientResponseError as response_error:
            self._WriteDEBUG(response_error)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
        except aiohttp.ClientTimeout as timeout_error:
            self._WriteDEBUG(timeout_error)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
            
        except asyncio.TimeoutError:
            self._WriteDEBUG("Coroutine timeout error")
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
            
        except aiohttp.InvalidURL as url_error:
            self._WriteDEBUG(url_error)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
            
        except aiohttp.ContentTypeError as contenttype_error:
            self._WriteDEBUG(contenttype_error)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
            
        except OSError as os_error:
            self._WriteDEBUG(os_error)
            
            await asyncio.sleep(2)
            await self._Get_WebSocket_Token()
            
        
        
    async def _parse_data(self, JsonPrinter_WebSocket):

        if self.__DEBUG_MODE == 1:
            await self._WriteDEBUG(JsonPrinter_WebSocket)
        
        
        if "result" in JsonPrinter_WebSocket:
            if "eventtime" in JsonPrinter_WebSocket["result"]:
                self.__Extruder_Temperature = JsonPrinter_WebSocket["result"]["status"]["extruder"]["temperature"]
                self.__Heater_Bed_Temperature = JsonPrinter_WebSocket["result"]["status"]["heater_bed"]["temperature"]
                self.__Progress = JsonPrinter_WebSocket["result"]["status"]["virtual_sdcard"]["progress"] * 100
                self.__STATUS_PRINTER = JsonPrinter_WebSocket["result"]["status"]["print_stats"]["state"]
                self.__PRINTER_DOP_STATUS = JsonPrinter_WebSocket["result"]["status"]["webhooks"]["state_message"]
                self.__FILENAME = JsonPrinter_WebSocket["result"]["status"]["print_stats"]["filename"]

    
    async def connect_moonraker(self):        
        try:
            if self.__is_token_required:
                TokenWeb_Socket = await self._Get_WebSocket_Token()
                self.__WB_API_MOONRAKER = await self.__MoonrakerWebSocket.ws_connect(f"ws://{self.__IP_SERVER_MOONRAKER}/websocket?token={TokenWeb_Socket}")

                return self.__WB_API_MOONRAKER
            
            else:
                self.__WB_API_MOONRAKER = await self.__MoonrakerWebSocket.ws_connect(f"ws://{self.__IP_SERVER_MOONRAKER}/websocket")
                return self.__WB_API_MOONRAKER
                
        except aiohttp.ClientConnectionError as error_connection:
            self._WriteDEBUG(error_connection)
            
            await asyncio.sleep(2)
            await self.connect_moonraker()
            
        except asyncio.TimeoutError as error_timeout:
            self._WriteDEBUG(error_timeout)
            
            await asyncio.sleep(2)
            await self.connect_moonraker()
            
        except aiohttp.ClientConnectorError as error_connector:
            self._WriteDEBUG(error_connector)
            
            await asyncio.sleep(2)
            await self.connect_moonraker()
        
        except OSError as error_os:
            self._WriteDEBUG(error_os)
            
            await asyncio.sleep(2)
            await self.connect_moonraker()
            
        except aiohttp.WebSocketError as web_socket_allerror:
            self._WriteDEBUG(f"Error websocket: {web_socket_allerror}")
            
            await asyncio.sleep(2)
            await self.connect_moonraker()
        
    async def update(self):

        if self.__WB_API_MOONRAKER is not None:

            try:
                await self.__WB_API_MOONRAKER.send_str(r'{"id":10020,"method":"printer.objects.query","jsonrpc":"2.0","params":{"objects":{"webhooks":null,"pause_resume":null,"heater_bed":null,"print_stats":null,"virtual_sdcard":null,"extruder":null}}}')
                
                WebSocket_DataJson = await self.__WB_API_MOONRAKER.receive_json()
                
                
                Data_Printer = await self._parse_data(WebSocket_DataJson)
                

            except aiohttp.client_exceptions.ClientConnectionResetError as client_connection_reset_error:
                await self._WriteDEBUG(f"Error websocket connectionr reset Error: {client_connection_reset_error}")
                
                await self.connect_moonraker()
                await self.update()
            except aiohttp.WSServerHandshakeError as handshake_error:
                self._WriteDEBUG(f"Error websocket handshake: {handshake_error}")
                
                await self.connect_moonraker()
                await self.update()
        
        FrameCAMERA = await self.__MoonrakerStream.get(f"http://{self.__IP_SERVER_MOONRAKER}/webcam/?action=snapshot")
        
        self.__FRAME_JPEG = await FrameCAMERA.content.read()

    


    
    
    def GetTemp_Extruder(self):
        return self.__Extruder_Temperature
    
    
    def GetTemp_HeaterBed(self):
        return self.__Heater_Bed_Temperature
    
    
    def GetProgress(self):
        return self.__Progress
    
    
    def GetStatusPrinter(self):
        return self.__STATUS_PRINTER
    
    
    def GetFileName_Printing(self):
        return self.__FILENAME
    
    
    def GetFrame_Camera(self):
        return self.__FRAME_JPEG
    
    
    
    def set_name_cfg(self, name_cfg):
        self.__cfg_name = name_cfg
        
        self.__DEBUG_MODE = int(self._GetPARAM_CFG("DEBUG_MOONRAKER"))
        self.__DEBUG_PATH = self._GetPARAM_CFG("DEBUG_PATH_MOONRAKER")
        self.__IP_SERVER_MOONRAKER = self._GetPARAM_CFG("IP_ADDR_SERVER")
        
        if self.__IP_SERVER_MOONRAKER.find("192.168") != -1:
            self.__is_token_required = False
        
        else:
            self.__LOGIN_MOONRAKER = self._GetPARAM_CFG("LOGIN_MOONRAKER")
            self.__PASSWORD_MOONRAKER = self._GetPARAM_CFG("PASSWORD_MOONRAKER")
        
        if self.__DEBUG_MODE == 1:
            CreateLog = open(self.__DEBUG_PATH, "w")
            CreateLog.close()
        
        