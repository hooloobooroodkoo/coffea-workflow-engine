from typing import Any
from .config import Config
from .model import Workflow
from .renderers.render_local import render_local
# from .renderers.luigi import render_luigi

def render(workflow: Workflow, config: Config) -> Any:
    if config.renderer == "local":
        return render_local(workflow, config)
    # if config.renderer == "luigi":
    #     return render_luigi(workflow, config)
    raise NotImplementedError(config.renderer)
