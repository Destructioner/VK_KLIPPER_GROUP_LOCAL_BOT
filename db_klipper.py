import asyncio
import aiofiles
import aiofiles.os
import json

class PeerConservationDB:
    def __init__(self, file_name_db):
        self.__file_name_db = file_name_db
        self.__cache_db = {}

    async def init(self):
        if await self.is_empty() != True:
            try:
                with open(self.__file_name_db, "r") as init_cache:
                    TempJson = init_cache.read()
                    self.__cache_db = json.loads(TempJson)
                
            except FileNotFoundError:
                create_file = open(self.__file_name_db, "w")
                create_file.close()
    
    async def get_conserv_id(self, peer_id: str):
        if peer_id in self.__cache_db:
            return str(self.__cache_db[peer_id])
            
        try:
            async with aiofiles.open(self.__file_name_db, "r") as file_db:
                peer_id_line = await file_db.read()
                json_db = json.loads(peer_id_line)
                
                if peer_id in json_db:
                    self.__cache_db[peer_id] = json_db[peer_id]
                    
                    return str(self.__cache_db[peer_id])
                
                return None
                    
        except FileNotFoundError:
            create_file = open(self.__file_name_db, "w")
            create_file.close()
            
            return None
            


    async def save_conserv_id(self, peer_id, conserv_id):
        self.__cache_db[peer_id] = conserv_id
        
        async with aiofiles.open(self.__file_name_db, "w") as file_db_save:
            await file_db_save.write(json.dumps(self.__cache_db, indent=2))
            
    
    async def get_all_peer_id(self):
        for key_id in self.__cache_db.keys():
            await asyncio.sleep(0)
            yield key_id
            
    async def is_empty(self):
        length_db_file = await aiofiles.os.stat(self.__file_name_db)
        
        if length_db_file.st_size == 0:
            return True
            
        return False