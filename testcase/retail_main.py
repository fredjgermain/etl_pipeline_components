
from src.pipeline import EtlPipeline, ETLComponent,IComponentContext 
from testcase.retail_config import cus_pipe_ctx 


def main() -> None:
  """
  Load config 
  init pipeline 
  exec ETL 
  """
  
  # ! Pipeline configs 
  etl = EtlPipeline(ctx = cus_pipe_ctx) 
  
  etl.run() 
  for c in etl.ctx.components: 
    print(c.ctx.name, c.ctx.success) # ! loader failed ERRNO 2
    if not c.ctx.success:
      print(c.ctx.error)

main()