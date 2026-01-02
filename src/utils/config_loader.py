# E:\SKILLS\customer-analytics-ecommerce-azure\src\utils\config_loader.py
import os
import re
import yaml
from pathlib import Path
from dotenv import load_dotenv

def load_config():
    """Load config with environment variable substitution."""
    
    # Load .env file
    project_root = Path(__file__).parent.parent.parent
    env_path = project_root / ".env"
    load_dotenv(env_path)
    
    # Load YAML config
    config_path = project_root / "config" / "settings.yaml"
    with open(config_path) as f:
        config = yaml.safe_load(f)
    
    # Replace ${VAR_NAME} patterns with actual env variables
    def replace_env_vars(obj):
        if isinstance(obj, dict):
            return {k: replace_env_vars(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [replace_env_vars(item) for item in obj]
        elif isinstance(obj, str):
            # Match ${VAR_NAME} pattern
            pattern = r'\$\{([^}]+)\}'
            matches = re.findall(pattern, obj)
            for match in matches:
                env_value = os.getenv(match)
                if env_value is None:
                    raise ValueError(f"Environment variable {match} not found!")
                obj = obj.replace(f"${{{match}}}", env_value)
            return obj
        return obj
    
    return replace_env_vars(config)

# Usage
if __name__ == "__main__":
    config = load_config()
    print(config["azure"]["connection_string"][:50])  # Print first 50 chars
