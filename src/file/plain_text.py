from pathlib import Path
from typing import List, Any

from .operations import separate_file_extension
from .temporary import generate_random_filename


def escrever_lista_txt(full_path_arquivo: str | Path, lista_texto: List[Any], nova_linha: bool = True) -> Path:

    if not isinstance(full_path_arquivo, Path):
        full_path_arquivo = Path(full_path_arquivo)

    if full_path_arquivo.exists() and full_path_arquivo.is_file():
        raise FileExistsError(f"O arquivo '{full_path_arquivo}' já existe.")
    
    if full_path_arquivo.is_dir():
        full_path_arquivo = full_path_arquivo / generate_random_filename(extension=".txt", method="uuid")

    nome, extensao = separate_file_extension(full_path_arquivo)
    if extensao.lower() != ".txt":
        raise ValueError("A extensão do arquivo deve ser '.txt'.")

    lista_texto = [str(item) if not isinstance(item, str) else item for item in lista_texto]

    if nova_linha:
        lista_texto = list(map(lambda x: f"{x}\n", lista_texto))

    with open(full_path_arquivo, mode="w") as f:
        f.writelines(lista_texto)

    return full_path_arquivo


def ler_arquivo_txt(full_path_arquivo: str | Path, encoding: str = "utf-8", criar_arquivo_nao_existe: bool = False):

    modo = "a+" if criar_arquivo_nao_existe else "r"
    with open(full_path_arquivo, mode=modo, encoding=encoding) as f:
        f.seek(0)
        data = f.read()
    
    return data
