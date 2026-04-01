
from typing import Protocol



class DependentComponent(Protocol):
  name: str

class DependencyError(Exception):
  """Raised when a required dependency is missing or incompatible."""

  def __init__(self, compo_name:str, deps: list[DependentComponent]): 
    deps_msg = [ d.name for d in deps] 
    
    self.message = f'Dependency failures: {deps_msg}' 
    super().__init__(self.message) 
    
  def __str__(self):
    return self.message


# class ETLExecutionError(Exception):
#     """Raised when an ETL component fails during execution."""

#     def __init__(self, compo_name: str, cause: Exception):
#         self.cause = cause
#         self.message = f"Component '{compo_name}' failed: {type(cause).__name__}: {cause}"
#         super().__init__(self.message)

#     def __str__(self):
#         return self.message

