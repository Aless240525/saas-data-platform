from omegaconf import OmegaConf
import os

def load_config(env_path="config/base.yaml"):
    """
    Carga la configuración de rutas mediante OmegaConf.
    """
    if not os.path.exists(env_path):
        raise FileNotFoundError(f"No se encontro el archivo de configuracion en {env_path}")
    
    return OmegaConf.load(env_path)