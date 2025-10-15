# app/modules/kits_repo.py
from typing import List, Dict, Any, Optional

def list_kits() -> List[Dict[str, Any]]:
    # верните [{'id':1,'name':'starter','description':'...','items_count':27}, ...]
    ...

def grant_kit_to_uuid(uuid: str, kit_id: int, *, by: Optional[str]=None, server: Optional[str]=None) -> Optional[Dict[str,Any]]:
    # выполните выдачу (через ваш мост / очередь / БД), можно вернуть
    # произвольный dict (он уйдёт в data), либо None — тогда фронт увидит {"granted": true}
    ...
