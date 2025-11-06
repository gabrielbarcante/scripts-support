from pathlib import Path
import os
from typing import List, Tuple
from dotenv import load_dotenv


def load_environment_variables(filename: str = ".env", file_path: str = ".") -> None:
    """
    Load environment variables from a .env file.

    Args:
        filename (str): Name of the environment file. Defaults to ".env".
        file_path (str): Path where the environment file is located. Defaults to ".".

    Raises:
        FileNotFoundError: If the specified file doesn't exist.
    """
    path_file = Path(file_path)
    path_file = path_file.resolve() / filename

    assert path_file.exists(), FileNotFoundError(
        f"File {filename} not found in {path_file.parent.as_posix()}."
    )

    load_dotenv(dotenv_path=path_file.as_posix(), override=True)
    print("Environment variables loading completed.")


def get_environment_variables(env_vars: List[str] | str) -> Tuple:
    """
    Retrieve environment variables and return them as a tuple.

    Args:
        env_vars (List[str] | str): Environment variable name(s) to retrieve.
                                   Can be a single string or list of strings.

    Returns:
        Tuple: A tuple containing the values of the requested environment variables.
    """
    if isinstance(env_vars, str):
        env_vars = [env_vars]

    return tuple(os.environ[env_var] for env_var in env_vars)
