from dataclasses import dataclass 
from typing import Callable 

from src.interface import IComponentContext 
from src.components import ETLComponent 


@dataclass
class ETLFactory[C:IComponentContext]: # C:ETLComponent, X:ETLComponentContext
  context_class: type[C]

  def make_batch(self, specifics: list[tuple[str, Callable, dict, list]]) -> list[ETLComponent[C]]: 
    return [self.make_component(*args) for args in specifics] 
  
  def make_component(self, name:str, func:Callable, ops:dict, deps:list[IComponentContext]=[]) -> ETLComponent[C]: 
    ctx = self.context_class(name, **ops) 
    return ETLComponent(func, ctx, deps) 
