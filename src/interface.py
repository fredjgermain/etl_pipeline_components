from dataclasses import dataclass, field 
from typing import Protocol

from utils.metaframe import MetaFrame 


@dataclass
class IComponentContext: 
  name:str 
  options: dict = field(default_factory=dict) 
  
  description:str = '' 
  
  success: bool | None = None 
  result: MetaFrame | None = None 
  error: Exception  | None = None 
  #artifact: Optional[MetaFrame] = None 
